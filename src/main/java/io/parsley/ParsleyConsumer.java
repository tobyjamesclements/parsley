package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

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
 * Decorator over a Kafka Streams pipeline (built from {@link CausalProcessorSupplier#create}) exposing a
 * {@code poll()}-based consumer API. The causal ordering is performed by a {@link CausalProcessorSupplier#create}
 * node whose delegate is a non-forwarding capture processor: it reconstructs each causally ordered
 * record from {@code recordMetadata()} (correct even for records that were buffered and drained
 * later) and enqueues it. The frontier is observed through the processor's public
 * {@link CausalFrontierListener} hook rather than by reading its internal state store.
 *
 * <p>Because the capture never forwards, no clock stamping occurs and each delivered record retains
 * the upstream producer's clock header — so {@code CausalDependencies.fromRecord(consumed)} still yields the
 * producer's clock.
 */
final class ParsleyConsumer<K, V> implements CausalConsumer<K, V> {

    private final KafkaStreams streams;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final LinkedBlockingQueue<ConsumerRecord<K, V>> readyQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<CausalDependencies> frontierRef = new AtomicReference<>(CausalDependencies.empty());

    ParsleyConsumer(
            Collection<String> topics,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig,
            String storeName) {

        Map<String, Object> merged = new HashMap<>();
        merged.put("processing.exception.handler.global.enabled", "true");
        merged.putAll(streamsConfig);
        merged.putAll(consumerConfig);
        StreamsConfig config = new StreamsConfig(merged);

        @SuppressWarnings("unchecked")
        Serde<K> keySerde = (Serde<K>) config.defaultKeySerde();
        @SuppressWarnings("unchecked")
        Serde<V> valueSerde = (Serde<V>) config.defaultValueSerde();
        // We own these serdes (StreamsConfig instantiates a fresh, configured instance per call),
        // so we hold and close them in close(). Serde extends Closeable.
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        StreamsBuilder builder = new StreamsBuilder();
        builder.<K, V>stream(topics)
                .process(CausalProcessorSupplier.create(captureSupplier(), policy, onViolation,
                        topic -> keySerde, topic -> valueSerde, storeName, this::onFrontierAdvanced));

        this.streams = new KafkaStreams(builder.build(), config);
        this.streams.start();
    }

    // Merge (rather than overwrite) so the frontier is correct across multiple partitions/tasks —
    // each task publishes a frontier over only its own partitions — and across the restored frontier
    // each task seeds at startup. Invoked from Streams threads; updateAndGet keeps it atomic.
    private void onFrontierAdvanced(CausalDependencies frontier) {
        frontierRef.updateAndGet(current -> current.merge(frontier));
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
                RecordMetadata meta = ctx.recordMetadata().orElseThrow();
                // Rebuild the consumer-facing record from the processor's record metadata, which
                // names the record's true source coordinate even when it was buffered and drained
                // later (not the Streams record currently being processed). The -1, -1 are the
                // serialized key/value sizes — unknown here, as the record is already deserialised.
                ConsumerRecord<K, V> cr = new ConsumerRecord<>(
                        meta.topic(), meta.partition(), meta.offset(),
                        record.timestamp(), TimestampType.CREATE_TIME,
                        -1, -1,
                        record.key(), record.value(),
                        record.headers(), Optional.empty());

                readyQueue.add(cr);
            }
        };
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> byPartition = new LinkedHashMap<>();

        try {
            // Block up to the timeout for the first record, then drain whatever else is already
            // queued without blocking again — so a poll returns a batch (like a real Kafka consumer)
            // yet still wakes promptly on the first available record.
            ConsumerRecord<K, V> first = readyQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (first != null) {
                accumulate(byPartition, first);
                List<ConsumerRecord<K, V>> drained = new ArrayList<>();
                readyQueue.drainTo(drained);
                drained.forEach(r -> accumulate(byPartition, r));
            }
        } catch (InterruptedException e) {
            // Restore the interrupt flag and return whatever we have; poll is not declared to throw.
            Thread.currentThread().interrupt();
        }

        return new ConsumerRecords<>(byPartition, Map.of());
    }

    private void accumulate(Map<TopicPartition, List<ConsumerRecord<K, V>>> map, ConsumerRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        map.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    }

    @Override
    public CausalDependencies frontier() {
        return frontierRef.get();
    }

    @Override
    public void close() {
        streams.close();
        // Close the serdes we manufactured from config; close() is a no-op for stateless serdes
        // but releases resources for stateful ones (e.g. Schema Registry-backed).
        keySerde.close();
        valueSerde.close();
    }
}
