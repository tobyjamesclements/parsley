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
import org.apache.kafka.streams.state.KeyValueStore;

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
 * Decorator over a Kafka Streams pipeline (built from {@link CausalProcessor#create}) exposing a
 * {@code poll()}-based consumer API. The causal ordering is performed by a {@link CausalProcessor#create}
 * node whose delegate is a non-forwarding capture processor: it reconstructs each causally ordered
 * record from {@code recordMetadata()} (correct even for records that were buffered and drained
 * later), enqueues it, and reads the frontier back from the processor's state store.
 *
 * <p>Because the capture never forwards, no clock stamping occurs and each delivered record retains
 * the upstream producer's clock header — so {@code VectorClock.fromRecord(consumed)} still yields the
 * producer's clock.
 */
final class ParsleyConsumer<K, V> implements CausalConsumer<K, V> {

    private final KafkaStreams streams;
    private final String frontierStoreName;
    private final LinkedBlockingQueue<ConsumerRecord<K, V>> readyQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<VectorClock> frontierRef = new AtomicReference<>(VectorClock.empty());

    ParsleyConsumer(
            Collection<String> topics,
            BufferingPolicy policy,
            ViolationHandler onViolation,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig,
            String storeName) {

        this.frontierStoreName = storeName + "-frontier";

        Map<String, Object> merged = new HashMap<>();
        merged.put("processing.exception.handler.global.enabled", "true");
        merged.putAll(streamsConfig);
        merged.putAll(consumerConfig);
        StreamsConfig config = new StreamsConfig(merged);

        @SuppressWarnings("unchecked")
        Serde<K> keySerde = (Serde<K>) config.defaultKeySerde();
        @SuppressWarnings("unchecked")
        Serde<V> valueSerde = (Serde<V>) config.defaultValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.<K, V>stream(topics)
                .process(CausalProcessor.create(captureSupplier(), policy, onViolation,
                        topic -> keySerde, topic -> valueSerde, storeName));

        this.streams = new KafkaStreams(builder.build(), config);
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
                RecordMetadata meta = ctx.recordMetadata().orElseThrow();
                ConsumerRecord<K, V> cr = new ConsumerRecord<>(
                        meta.topic(), meta.partition(), meta.offset(),
                        record.timestamp(), TimestampType.CREATE_TIME,
                        -1, -1,
                        record.key(), record.value(),
                        record.headers(), Optional.empty());

                readyQueue.add(cr);

                byte[] frontierBytes = ctx.<KeyValueStore<String, byte[]>>
                        getStateStore(frontierStoreName)
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
}
