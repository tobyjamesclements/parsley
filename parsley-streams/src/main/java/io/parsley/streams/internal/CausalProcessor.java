package io.parsley.streams.internal;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationReason;
import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.buffer.CausalViolationHandler;
import io.parsley.internal.ImmutableVectorClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public final class CausalProcessor<K, V> implements Processor<K, V, K, V> {

    public static final String FRONTIER_STORE = "parsley-frontier";
    public static final String FRONTIER_KEY = "f";
    static final String CLOCK_HEADER = "parsley-vector-clock";
    static final String SOURCE_TOPIC_HEADER = "parsley-src-topic";
    static final String SOURCE_PARTITION_HEADER = "parsley-src-partition";
    static final String SOURCE_OFFSET_HEADER = "parsley-src-offset";

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final VectorClockSerialiser serialiser;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;

    private ProcessorContext<K, V> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private ImmutableVectorClock frontier = ImmutableVectorClock.empty();
    private int sizeLimit = Integer.MAX_VALUE;

    // in-memory buffer: sequence → (record, clock, bufferedAt, srcPartition, srcOffset)
    record Buffered<K, V>(ConsumerRecord<K, V> record, VectorClock clock, Instant bufferedAt,
                          Partition srcPartition, long srcOffset) {}
    private final java.util.LinkedHashMap<Long, Buffered<K, V>> inMemory = new java.util.LinkedHashMap<>();
    private long seq = 0;

    public CausalProcessor(BufferingPolicy policy, CausalViolationHandler violationHandler,
                            VectorClockSerialiser serialiser, Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.serialiser = serialiser;
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(FRONTIER_STORE);

        byte[] stored = frontierStore.get(FRONTIER_KEY);
        if (stored != null) {
            frontier = (ImmutableVectorClock) serialiser.deserialise(stored);
        }

        configureLimits(limitOf(policy));
    }

    @Override
    public void process(Record<K, V> record) {
        Optional<org.apache.kafka.streams.processor.api.RecordMetadata> meta = context.recordMetadata();
        String srcTopic = meta.map(org.apache.kafka.streams.processor.api.RecordMetadata::topic).orElse("");
        int srcPartition = meta.map(org.apache.kafka.streams.processor.api.RecordMetadata::partition).orElse(0);
        long srcOffset = meta.map(org.apache.kafka.streams.processor.api.RecordMetadata::offset).orElse(0L);
        Partition srcPart = new Partition(srcTopic, srcPartition);

        Header clockHeader = record.headers().lastHeader(CLOCK_HEADER);
        if (clockHeader == null) {
            violationHandler.onViolation(toConsumerRecord(record, srcTopic, srcPartition, srcOffset),
                    CausalViolationReason.MISSING_HEADER);
            frontier = frontier.advance(srcPart, srcOffset);
            persistFrontier();
            forward(record, srcTopic, srcPartition, srcOffset);
            drainBuffer();
            return;
        }

        VectorClock recordClock;
        try {
            recordClock = serialiser.deserialise(clockHeader.value());
        } catch (Exception e) {
            violationHandler.onViolation(toConsumerRecord(record, srcTopic, srcPartition, srcOffset),
                    CausalViolationReason.UNRESOLVABLE_CLOCK);
            frontier = frontier.advance(srcPart, srcOffset);
            persistFrontier();
            forward(record, srcTopic, srcPartition, srcOffset);
            drainBuffer();
            return;
        }

        if (frontier.dominates(recordClock)) {
            frontier = frontier.advance(srcPart, srcOffset);
            persistFrontier();
            forward(record, srcTopic, srcPartition, srcOffset);
            drainBuffer();
        } else {
            inMemory.put(seq++, new Buffered<>(
                    toConsumerRecord(record, srcTopic, srcPartition, srcOffset),
                    recordClock, Instant.now(), srcPart, srcOffset));
            if (inMemory.size() >= sizeLimit) {
                evictBuffer();
            }
        }
    }

    private void drainBuffer() {
        boolean progress = true;
        while (progress) {
            progress = false;
            for (var entry : new ArrayList<>(inMemory.entrySet())) {
                Buffered<K, V> b = entry.getValue();
                if (frontier.dominates(b.clock())) {
                    frontier = frontier.advance(b.srcPartition(), b.srcOffset());
                    persistFrontier();
                    forwardConsumerRecord(b.record(), b.srcPartition(), b.srcOffset());
                    inMemory.remove(entry.getKey());
                    progress = true;
                }
            }
        }
    }

    private void evictBuffer() {
        if (inMemory.isEmpty()) return;
        List<ConsumerRecord<K, V>> toForward = new ArrayList<>();
        inMemory.values().forEach(b -> {
            ConsumerRecord<K, V> cr = b.record();
            switch (policy) {
                case BufferingPolicy.Ignore ignore -> {
                    violationHandler.onViolation(cr, CausalViolationReason.LIMIT_REACHED);
                    toForward.add(cr);
                }
                case BufferingPolicy.Drop drop ->
                        violationHandler.onViolation(cr, CausalViolationReason.LIMIT_REACHED);
                case BufferingPolicy.DeadLetter dl -> {
                    deadLetterSink.accept(cr);
                    violationHandler.onViolation(cr, CausalViolationReason.LIMIT_REACHED);
                }
            }
        });
        inMemory.clear();
        for (ConsumerRecord<K, V> cr : toForward) {
            Partition p = new Partition(cr.topic(), cr.partition());
            frontier = frontier.advance(p, cr.offset());
            persistFrontier();
            forwardConsumerRecord(cr, p, cr.offset());
        }
    }

    private void forward(Record<K, V> record, String topic, int partition, long offset) {
        Headers enriched = new RecordHeaders(record.headers());
        enriched.add(new RecordHeader(SOURCE_TOPIC_HEADER, topic.getBytes(StandardCharsets.UTF_8)));
        enriched.add(new RecordHeader(SOURCE_PARTITION_HEADER, intToBytes(partition)));
        enriched.add(new RecordHeader(SOURCE_OFFSET_HEADER, longToBytes(offset)));
        context.forward(new Record<>(record.key(), record.value(), record.timestamp(), enriched));
    }

    private void forwardConsumerRecord(ConsumerRecord<K, V> cr, Partition srcPart, long srcOffset) {
        Headers enriched = new RecordHeaders(cr.headers());
        enriched.add(new RecordHeader(SOURCE_TOPIC_HEADER, srcPart.topic().getBytes(StandardCharsets.UTF_8)));
        enriched.add(new RecordHeader(SOURCE_PARTITION_HEADER, intToBytes(srcPart.partition())));
        enriched.add(new RecordHeader(SOURCE_OFFSET_HEADER, longToBytes(srcOffset)));
        context.forward(new Record<>(cr.key(), cr.value(), cr.timestamp(), enriched));
    }

    private void persistFrontier() {
        frontierStore.put(FRONTIER_KEY, serialiser.serialise(frontier));
    }

    private static BufferLimit limitOf(BufferingPolicy policy) {
        return switch (policy) {
            case BufferingPolicy.Ignore ig -> ig.limit();
            case BufferingPolicy.Drop dp -> dp.limit();
            case BufferingPolicy.DeadLetter de -> de.limit();
        };
    }

    private void configureLimits(BufferLimit limit) {
        switch (limit) {
            case BufferLimit.DurationLimit dl ->
                context.schedule(dl.duration(), PunctuationType.WALL_CLOCK_TIME, ts -> evictBuffer());
            case BufferLimit.SizeLimit sl ->
                sizeLimit = sl.messages();
            case BufferLimit.FirstLimit fl -> {
                for (BufferLimit inner : fl.limits()) configureLimits(inner);
            }
            case BufferLimit.BytesLimit ignored ->
                throw new UnsupportedOperationException(
                        "BytesLimit is not supported by CausalProcessor; use DurationLimit or SizeLimit");
            case BufferLimit.FrontierAdvancementLimit ignored ->
                throw new UnsupportedOperationException(
                        "FrontierAdvancementLimit is not supported by CausalProcessor; use DurationLimit or SizeLimit");
        }
    }

    private ConsumerRecord<K, V> toConsumerRecord(Record<K, V> record, String topic, int partition, long offset) {
        return new ConsumerRecord<>(
                topic, partition, offset,
                record.timestamp(), TimestampType.CREATE_TIME,
                -1, -1,
                record.key(), record.value(),
                record.headers(), Optional.empty());
    }

    static byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    static int bytesToInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    static long bytesToLong(byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }
}
