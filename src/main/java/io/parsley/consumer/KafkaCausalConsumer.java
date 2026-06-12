package io.parsley.consumer;

import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.VectorClock;
import io.parsley.internal.Attributes;
import io.parsley.stream.CausalProcessorSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Decorator over a Kafka Streams pipeline (built from the public
 * {@link CausalProcessorSupplier}) exposing a {@code poll()}-based consumer API. Causally
 * ordered records are captured off the topology onto a ready queue, and the frontier is read
 * back from the processor's state store.
 */
final class KafkaCausalConsumer<K, V> implements CausalConsumer<K, V> {

    private final KafkaStreams streams;
    private final LinkedBlockingQueue<ConsumerRecord<K, V>> readyQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<VectorClock> frontierRef = new AtomicReference<>(VectorClock.empty());

    KafkaCausalConsumer(
            Collection<String> topics,
            BufferingPolicy policy,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {

        CausalProcessorSupplier<K, V> causalSupplier =
                CausalProcessorSupplier.create(policy, CausalViolationHandler.noop());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<K, V> stream = builder.stream(topics);
        stream.process(causalSupplier)
              .process(captureSupplier(), Attributes.FRONTIER_STORE);

        Map<String, Object> merged = new HashMap<>();
        merged.put("processing.exception.handler.global.enabled", "true");
        merged.putAll(streamsConfig);
        merged.putAll(consumerConfig);
        this.streams = new KafkaStreams(builder.build(), new StreamsConfig(merged));
        this.streams.start();
    }

    ProcessorSupplier<K, V, Void, Void> captureSupplier() {
        return () -> new Processor<>() {
            private ProcessorContext<Void, Void> ctx;

            @Override
            public void init(ProcessorContext<Void, Void> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<K, V> record) {
                String topic = extractStringHeader(record, Attributes.SOURCE_TOPIC);
                int partition = extractIntHeader(record, Attributes.SOURCE_PARTITION);
                long offset = extractLongHeader(record, Attributes.SOURCE_OFFSET);

                ConsumerRecord<K, V> cr = new ConsumerRecord<>(
                        topic.isEmpty() ? "unknown" : topic,
                        partition, offset,
                        record.timestamp(), TimestampType.CREATE_TIME,
                        -1, -1,
                        record.key(), record.value(),
                        record.headers(), Optional.empty());

                readyQueue.add(cr);

                byte[] frontierBytes = ctx.<KeyValueStore<String, byte[]>>
                        getStateStore(Attributes.FRONTIER_STORE)
                        .get(Attributes.FRONTIER_KEY);
                if (frontierBytes != null) {
                    frontierRef.set(VectorClock.fromBytes(frontierBytes));
                }
            }
        };
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> byPartition = new LinkedHashMap<>();

        try {
            ConsumerRecord<K, V> first = readyQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (first != null) {
                accumulate(byPartition, first);
                List<ConsumerRecord<K, V>> drained = new ArrayList<>();
                readyQueue.drainTo(drained);
                drained.forEach(r -> accumulate(byPartition, r));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new ConsumerRecords<>(byPartition);
    }

    private void accumulate(Map<TopicPartition, List<ConsumerRecord<K, V>>> map, ConsumerRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        map.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    }

    @Override
    public VectorClock frontier() {
        return frontierRef.get();
    }

    @Override
    public void close() {
        streams.close();
    }

    static String extractStringHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    static int extractIntHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? 0 : ByteBuffer.wrap(h.value()).getInt();
    }

    static long extractLongHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? 0L : ByteBuffer.wrap(h.value()).getLong();
    }
}
