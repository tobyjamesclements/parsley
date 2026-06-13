package io.parsley.stream;

import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.VectorClock;
import io.parsley.internal.Attributes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
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
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The Kafka Streams {@link Processor} that drives the {@link CausalEngine}.
 *
 * <p>Restores the frontier from the {@code parsley-frontier} state store on init, persists it
 * before forwarding each record, and stamps source-coordinate headers so a
 * {@code CausalConsumer} can reconstruct the original {@link ConsumerRecord}.
 */
final class CausalProcessor<K, V> implements Processor<K, V, K, V> {

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;
    private final String frontierStoreName;

    private ProcessorContext<K, V> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private CausalEngine<K, V> engine;

    CausalProcessor(BufferingPolicy policy, CausalViolationHandler violationHandler,
                    Consumer<ConsumerRecord<K, V>> deadLetterSink, String frontierStoreName) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.deadLetterSink = deadLetterSink;
        this.frontierStoreName = frontierStoreName;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(frontierStoreName);

        VectorClock initialFrontier = VectorClock.empty();
        byte[] stored = frontierStore.get(Attributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = VectorClock.fromBytes(stored);
        }

        Consumer<CausalRecord<K, V>> sink = deadLetterSink == null
                ? null
                : record -> deadLetterSink.accept(record.toConsumerRecord());

        engine = new CausalEngine<>(policy, violationHandler, initialFrontier, sink,
                frontier -> frontierStore.put(Attributes.FRONTIER_KEY, frontier.toBytes()));

        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME,
                        _ -> engine.evictNow().forEach(this::forward)));
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

    private CausalRecord<K, V> toCausalRecord(
            Record<K, V> record, String topic, int partition, long offset) {
        List<CausalHeader> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new CausalHeader(header.key(), header.value()));
        }
        Header clockHeader = record.headers().lastHeader(Attributes.VECTOR_CLOCK);
        return new CausalRecord<>(
                record.key(), record.value(), record.timestamp(), headers,
                clockHeader == null ? null : clockHeader.value(),
                new TopicPartition(topic, partition), offset);
    }

    private void forward(CausalRecord<K, V> record) {
        Headers enriched = record.toHeaders();
        enriched.add(new RecordHeader(Attributes.SOURCE_TOPIC,
                record.sourcePartition().topic().getBytes(StandardCharsets.UTF_8)));
        enriched.add(new RecordHeader(Attributes.SOURCE_PARTITION, intToBytes(record.sourcePartition().partition())));
        enriched.add(new RecordHeader(Attributes.SOURCE_OFFSET, longToBytes(record.sourceOffset())));
        context.forward(new Record<>(record.key(), record.value(), record.timestamp(), enriched));
    }

    static byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }
}
