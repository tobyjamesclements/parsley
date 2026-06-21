package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
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
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Decorator over a Kafka Streams pipeline exposing a {@code poll()}-based consumer API. The
 * causal ordering is performed by a {@link ParsleyOutboxProcessor} node, which gates each record
 * on {@link ParsleyEngine}, embeds its original source coordinate as internal headers, and
 * forwards it into an internal Kafka outbox topic — headers otherwise untouched, so the producer's
 * original {@link ParsleyAttributes#CAUSAL_DEPENDENCIES} header survives to {@code poll()} unchanged.
 * The {@link CausalConsumer#poll poll()} method delegates to an internal {@link KafkaConsumer}
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
 */
final class ParsleyConsumer<K, V> implements CausalConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyConsumer.class);

    private final KafkaStreams streams;
    private final KafkaConsumer<byte[], byte[]> outboxConsumer;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    ParsleyConsumer(
            Collection<String> topics,
            CausalBufferLimit limit,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig,
            String storeName,
            ParsleyTopicAdmin topicAdmin,
            Map<String, Uuid> topicUuids) {

        Map<String, Object> merged = new HashMap<>();
        merged.put("processing.exception.handler.global.enabled", "true");
        merged.putAll(streamsConfig);
        merged.putAll(consumerConfig);
        StreamsConfig config = new StreamsConfig(merged);

        String applicationId = (String) merged.get(StreamsConfig.APPLICATION_ID_CONFIG);
        String bootstrap     = (String) merged.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        String outboxTopic   = applicationId + "-" + storeName + "-outbox";

        @SuppressWarnings("unchecked")
        Serde<K> keySerde = (Serde<K>) config.defaultKeySerde();
        @SuppressWarnings("unchecked")
        Serde<V> valueSerde = (Serde<V>) config.defaultValueSerde();
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        ParsleyTopicAdmin admin = (topicAdmin != null) ? topicAdmin : ParsleyTopicAdmin.ofBootstrap(bootstrap);
        try {
            setupOutbox(admin, topics, outboxTopic);
        } finally {
            try { admin.close(); } catch (Exception ignored) {}
        }

        Serde<byte[]> bytes = Serdes.ByteArray();
        ProcessorSupplier<byte[], byte[], byte[], byte[]> outboxProcessor = ParsleyOutboxProcessor.supplier(
                limit, storeName, topicUuids);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(topics, Consumed.with(bytes, bytes))
                .process(outboxProcessor)
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
        log.info("ParsleyConsumer started [applicationId: {}, topics: {}, outbox: {}]",
                applicationId, topics, outboxTopic);
        log.debug("Topic UUIDs: {}", topicUuids);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> raw = outboxConsumer.poll(timeout);
        Map<TopicPartition, List<ConsumerRecord<K, V>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> r : raw) {
            String srcTopic     = new String(r.headers().lastHeader(ParsleyAttributes.SRC_TOPIC).value(), UTF_8);
            int    srcPartition = intFromBytes(r.headers().lastHeader(ParsleyAttributes.SRC_PARTITION).value());
            long   srcOffset    = longFromBytes(r.headers().lastHeader(ParsleyAttributes.SRC_OFFSET).value());
            Headers headers = stripInternalHeaders(r.headers());
            ConsumerRecord<K, V> cr = toTypedRecord(
                    srcTopic, srcPartition, srcOffset, r.timestamp(), r.key(), r.value(), headers);
            byPartition.computeIfAbsent(new TopicPartition(srcTopic, srcPartition),
                    k -> new ArrayList<>()).add(cr);
        }
        return new ConsumerRecords<>(byPartition, Map.of());
    }

    /**
     * Deserializes a raw record's key/value at {@code topic} and assembles a typed
     * {@link ConsumerRecord} at the given source coordinate. {@code headers} must already be the
     * caller's intended final headers — this method does no header massaging itself.
     */
    private ConsumerRecord<K, V> toTypedRecord(
            String topic, int partition, long offset, long timestamp,
            byte[] keyBytes, byte[] valueBytes, Headers headers) {
        K key   = keySerde.deserializer().deserialize(topic, keyBytes);
        V value = valueSerde.deserializer().deserialize(topic, valueBytes);
        return new ConsumerRecord<>(
                topic, partition, offset, timestamp, TimestampType.CREATE_TIME,
                -1, -1, key, value, headers, Optional.empty());
    }


    @Override
    public void close() {
        outboxConsumer.close();
        streams.close();
        keySerde.close();
        valueSerde.close();
    }

    /**
     * Creates the outbox topic, sized to the maximum partition count among the input topics.
     */
    private static void setupOutbox(ParsleyTopicAdmin admin, Collection<String> inputTopics,
                                     String outboxTopic) {
        try {
            Map<String, Integer> partitionCounts = admin.partitionCounts(new ArrayList<>(inputTopics));

            int maxPartitions = partitionCounts.values().stream()
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(1);
            try {
                admin.createTopic(outboxTopic, maxPartitions);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException("Failed to create outbox topic " + outboxTopic, e);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to create outbox topic " + outboxTopic, e);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read input topics' partition counts for outbox sizing", e);
        }
    }

    /**
     * Strips internal {@code _parsley_*} routing headers, leaving the producer's original
     * {@link ParsleyAttributes#CAUSAL_DEPENDENCIES} (and every other header) untouched —
     * {@link ParsleyOutboxProcessor} never rewrites it, so there is nothing to restore.
     */
    private static Headers stripInternalHeaders(Headers source) {
        RecordHeaders out = new RecordHeaders();
        for (Header h : source) {
            if (h.key().startsWith("_parsley_")) continue;
            out.add(h);
        }
        return out;
    }

    private static int intFromBytes(byte[] b)   { return ByteBuffer.wrap(b).getInt(); }
    private static long longFromBytes(byte[] b) { return ByteBuffer.wrap(b).getLong(); }
}
