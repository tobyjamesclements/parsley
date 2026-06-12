package io.parsley.kafka.internal;

import io.parsley.BufferingPolicy;
import io.parsley.CausalHeader;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.ParsleyAttributes;
import io.parsley.VectorClockSerialiser;
import io.parsley.core.CausalEngine;
import io.parsley.kafka.KafkaVectorClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public final class CausalProcessor<K, V> implements Processor<K, V, K, V> {

    public static final String FRONTIER_STORE = "parsley-frontier";
    public static final String FRONTIER_KEY = "f";
    static final String SOURCE_TOPIC_HEADER = "parsley-src-topic";
    static final String SOURCE_PARTITION_HEADER = "parsley-src-partition";
    static final String SOURCE_OFFSET_HEADER = "parsley-src-offset";

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final VectorClockSerialiser<KafkaVectorClock> serialiser;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;

    private ProcessorContext<K, V> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private CausalEngine<K, V, KafkaVectorClock> engine;

    public CausalProcessor(BufferingPolicy policy, CausalViolationHandler violationHandler,
                           VectorClockSerialiser<KafkaVectorClock> serialiser,
                           Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.serialiser = serialiser;
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(FRONTIER_STORE);

        KafkaVectorClock initialFrontier = KafkaVectorClock.empty();
        byte[] stored = frontierStore.get(FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = serialiser.deserialise(stored);
        }

        Consumer<CausalRecord<K, V, KafkaVectorClock>> sink = deadLetterSink == null
                ? null
                : record -> deadLetterSink.accept(toConsumerRecord(record));

        engine = new CausalEngine<>(policy, violationHandler, serialiser, initialFrontier, sink,
                frontier -> frontierStore.put(FRONTIER_KEY, serialiser.serialise(frontier)));

        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> engine.evictNow().forEach(this::forward)));
    }

    @Override
    public void process(Record<K, V> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String srcTopic = meta.map(RecordMetadata::topic).orElse("");
        int srcPartition = meta.map(RecordMetadata::partition).orElse(0);
        long srcOffset = meta.map(RecordMetadata::offset).orElse(0L);

        engine.onRecord(toCausalRecord(record, srcTopic, srcPartition, srcOffset))
                .forEach(this::forward);
    }

    private CausalRecord<K, V, KafkaVectorClock> toCausalRecord(
            Record<K, V> record, String topic, int partition, long offset) {
        List<CausalHeader> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new CausalHeader(header.key(), header.value()));
        }
        Header clockHeader = record.headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK);
        TopicPartition tp = new TopicPartition(topic, partition);
        return new CausalRecord<>(
                record.key(), record.value(), record.timestamp(), headers,
                clockHeader == null ? null : clockHeader.value(),
                KafkaVectorClock.empty().advance(tp, offset),
                topic + "-" + partition + "@" + offset);
    }

    private void forward(CausalRecord<K, V, KafkaVectorClock> record) {
        Map.Entry<TopicPartition, Long> source = sourceOf(record);
        Headers enriched = toHeaders(record);
        enriched.add(new RecordHeader(SOURCE_TOPIC_HEADER,
                source.getKey().topic().getBytes(StandardCharsets.UTF_8)));
        enriched.add(new RecordHeader(SOURCE_PARTITION_HEADER, intToBytes(source.getKey().partition())));
        enriched.add(new RecordHeader(SOURCE_OFFSET_HEADER, longToBytes(source.getValue())));
        context.forward(new Record<>(record.key(), record.value(), record.timestamp(), enriched));
    }

    private ConsumerRecord<K, V> toConsumerRecord(CausalRecord<K, V, KafkaVectorClock> record) {
        Map.Entry<TopicPartition, Long> source = sourceOf(record);
        return new ConsumerRecord<>(
                source.getKey().topic(), source.getKey().partition(), source.getValue(),
                record.timestamp(), TimestampType.CREATE_TIME,
                -1, -1,
                record.key(), record.value(),
                toHeaders(record), Optional.empty());
    }

    /** A record's position is a singleton clock at its own source coordinate. */
    private static Map.Entry<TopicPartition, Long> sourceOf(CausalRecord<?, ?, KafkaVectorClock> record) {
        return record.position().positions().entrySet().iterator().next();
    }

    private Headers toHeaders(CausalRecord<K, V, KafkaVectorClock> record) {
        Headers headers = new RecordHeaders();
        for (CausalHeader header : record.headers()) {
            headers.add(new RecordHeader(header.key(), header.value()));
        }
        return headers;
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
