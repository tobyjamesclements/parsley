package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Decorator over a Kafka Streams pipeline (built from {@link CausalProcessors}) exposing a
 * {@code poll()}-based consumer API. The causal ordering is performed by a
 * {@link CausalProcessorSupplier} node whose delegate embeds original source coordinates as
 * internal headers and forwards records into an internal Kafka outbox topic. The
 * {@link CausalConsumer#poll poll()} method delegates to an internal {@link KafkaConsumer}
 * subscribed to that outbox topic, strips the internal headers, and reconstructs each record at
 * its original source coordinate.
 *
 * <p>The topology passes raw {@code byte[]} end-to-end: key and value bytes are never
 * deserialized inside the Streams pipeline (the causal engine gates on header clocks only).
 * Deserialization happens once, in {@code poll()}, using the configured default key/value serdes
 * with the <em>original source topic</em>. This avoids Schema Registry subject collisions that
 * would otherwise arise when serializing multiple Avro record types into a single outbox topic
 * under the default {@code TopicNameStrategy}.
 *
 * <p>The outbox topic ({@code {applicationId}-{storeName}-outbox}) is created automatically on
 * startup; its partition count matches the maximum of the input topics'. Records written to it
 * are durable — they survive a consumer restart and will be re-delivered, giving
 * <strong>at-least-once</strong> semantics. Set {@code processing.guarantee=exactly_once_v2} in
 * {@code streamsConfig} for exactly-once.
 *
 * <p>Because {@link ParsleyProcessorContext} stamps the delivery-time frontier onto the
 * {@link ParsleyAttributes#VECTOR_CLOCK} header when the outbox delegate calls
 * {@code ctx.forward()}, the original producer's clock is saved under
 * {@link ParsleyAttributes#ORIG_CLOCK} before forwarding and restored in {@code poll()} so that
 * {@link CausalDependencies#fromRecord} still returns the upstream producer's causal intent.
 */
final class ParsleyConsumer<K, V> implements CausalConsumer<K, V> {

    private final KafkaStreams streams;
    private final KafkaConsumer<byte[], byte[]> outboxConsumer;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final AtomicReference<CausalDependencies> frontierRef = new AtomicReference<>(CausalDependencies.empty());

    ParsleyConsumer(
            Collection<String> topics,
            CausalBufferPolicy policy,
            CausalViolationHandler onViolation,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig,
            String storeName) {

        Map<String, Object> merged = new HashMap<>();
        merged.put("processing.exception.handler.global.enabled", "true");
        merged.putAll(streamsConfig);
        merged.putAll(consumerConfig);
        StreamsConfig config = new StreamsConfig(merged);

        String applicationId = (String) merged.get(StreamsConfig.APPLICATION_ID_CONFIG);
        String bootstrap     = (String) merged.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        String outboxTopic   = applicationId + "-" + storeName + "-outbox";

        // Obtained from config for poll()-time deserialization only; not used inside the topology.
        @SuppressWarnings("unchecked")
        Serde<K> keySerde = (Serde<K>) config.defaultKeySerde();
        @SuppressWarnings("unchecked")
        Serde<V> valueSerde = (Serde<V>) config.defaultValueSerde();
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        createOutboxTopic(bootstrap, topics, outboxTopic);

        Serde<byte[]> bytes = Serdes.ByteArray();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(topics, Consumed.with(bytes, bytes))
                .process(CausalProcessors.builder(outboxDelegate(), policy)
                        .serdesByTopic(t -> bytes, t -> bytes)
                        .onViolation(onViolation)
                        .storeName(storeName)
                        .frontierListener(this::onFrontierAdvanced)
                        .build())
                .to(outboxTopic, Produced.with(bytes, bytes));

        this.streams = new KafkaStreams(builder.build(), config);
        this.streams.start();

        Map<String, Object> outboxCfg = new HashMap<>();
        outboxCfg.put("bootstrap.servers", bootstrap);
        outboxCfg.put("group.id", applicationId + "-" + storeName + "-outbox-consumer");
        outboxCfg.put("auto.offset.reset", "earliest");
        outboxCfg.put("isolation.level", "read_committed");
        outboxCfg.put("key.deserializer", ByteArrayDeserializer.class.getName());
        outboxCfg.put("value.deserializer", ByteArrayDeserializer.class.getName());
        this.outboxConsumer = new KafkaConsumer<>(outboxCfg);
        this.outboxConsumer.subscribe(List.of(outboxTopic));
    }

    // Merge (rather than overwrite) so the frontier is correct across multiple partitions/tasks —
    // each task publishes a frontier over only its own partitions — and across the restored frontier
    // each task seeds at startup. Invoked from Streams threads; updateAndGet keeps it atomic.
    private void onFrontierAdvanced(CausalDependencies frontier) {
        frontierRef.updateAndGet(current -> current.merge(frontier));
    }

    private ProcessorSupplier<byte[], byte[], byte[], byte[]> outboxDelegate() {
        return () -> new Processor<>() {
            private ProcessorContext<byte[], byte[]> ctx;

            @Override
            public void init(ProcessorContext<byte[], byte[]> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<byte[], byte[]> record) {
                // ParsleyProcessorContext.recordMetadata() returns the original source coordinate
                // of the buffered record, even when it was drained later by a different trigger.
                RecordMetadata meta = ctx.recordMetadata().orElseThrow();
                // Save the producer's clock before ParsleyProcessorContext.stamp() replaces it.
                Header origClock = record.headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK);
                Headers h = new RecordHeaders(record.headers());
                if (origClock != null) {
                    h.add(ParsleyAttributes.ORIG_CLOCK, origClock.value());
                }
                h.add(ParsleyAttributes.SRC_TOPIC,     meta.topic().getBytes(UTF_8));
                h.add(ParsleyAttributes.SRC_PARTITION,  intToBytes(meta.partition()));
                h.add(ParsleyAttributes.SRC_OFFSET,     longToBytes(meta.offset()));
                // stamp() will replace parsley-vector-clock with the delivery-time frontier.
                ctx.forward(new Record<>(record.key(), record.value(), record.timestamp(), h));
            }
        };
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> raw = outboxConsumer.poll(timeout);
        Map<TopicPartition, List<ConsumerRecord<K, V>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> r : raw) {
            String srcTopic     = new String(r.headers().lastHeader(ParsleyAttributes.SRC_TOPIC).value(), UTF_8);
            int    srcPartition = intFromBytes(r.headers().lastHeader(ParsleyAttributes.SRC_PARTITION).value());
            long   srcOffset    = longFromBytes(r.headers().lastHeader(ParsleyAttributes.SRC_OFFSET).value());
            // Deserialize with the original source topic so Schema Registry subjects (TopicNameStrategy:
            // "{topic}-value") resolve to the same schema the producer registered.
            K key   = keySerde.deserializer().deserialize(srcTopic, r.key());
            V value = valueSerde.deserializer().deserialize(srcTopic, r.value());
            Headers headers = restoreOriginalClock(r.headers());
            ConsumerRecord<K, V> cr = new ConsumerRecord<>(
                    srcTopic, srcPartition, srcOffset,
                    r.timestamp(), TimestampType.CREATE_TIME,
                    -1, -1, key, value, headers, Optional.empty());
            byPartition.computeIfAbsent(new TopicPartition(srcTopic, srcPartition),
                    k -> new ArrayList<>()).add(cr);
        }
        return new ConsumerRecords<>(byPartition, Map.of());
    }

    @Override
    public CausalDependencies frontier() {
        return frontierRef.get();
    }

    @Override
    public void close() {
        outboxConsumer.close();
        streams.close();
        keySerde.close();
        valueSerde.close();
    }

    private static void createOutboxTopic(String bootstrap, Collection<String> inputTopics,
                                          String outboxTopic) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(new ArrayList<>(inputTopics)).allTopicNames().get();
            int maxPartitions = descriptions.values().stream()
                    .mapToInt(td -> td.partitions().size())
                    .max()
                    .orElse(1);
            try {
                admin.createTopics(Set.of(new NewTopic(outboxTopic, maxPartitions, (short) 1))).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException("Failed to create outbox topic " + outboxTopic, e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to describe input topics for outbox sizing", e);
        }
    }

    /**
     * Strips all internal {@code _parsley_*} headers and swaps the delivery-time frontier clock
     * back for the producer's original {@link ParsleyAttributes#VECTOR_CLOCK}, so that
     * {@link CausalDependencies#fromRecord} returns the upstream producer's causal intent rather
     * than the frontier at delivery time.
     */
    private static Headers restoreOriginalClock(Headers source) {
        Header origClock = source.lastHeader(ParsleyAttributes.ORIG_CLOCK);
        RecordHeaders out = new RecordHeaders();
        for (Header h : source) {
            String key = h.key();
            if (key.startsWith("_parsley_")) continue;         // SRC_* and ORIG_CLOCK
            if (key.equals(ParsleyAttributes.VECTOR_CLOCK)) continue;  // frontier clock from stamp()
            out.add(h);
        }
        if (origClock != null) {
            out.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, origClock.value()));
        }
        return out;
    }

    private static byte[] intToBytes(int v)   { return ByteBuffer.allocate(4).putInt(v).array(); }
    private static int intFromBytes(byte[] b)  { return ByteBuffer.wrap(b).getInt(); }
    private static byte[] longToBytes(long v)  { return ByteBuffer.allocate(8).putLong(v).array(); }
    private static long longFromBytes(byte[] b) { return ByteBuffer.wrap(b).getLong(); }
}
