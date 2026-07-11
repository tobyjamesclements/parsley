package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KafkaEpochTransport}'s client-config derivation and single-partition contract —
 * the parts that can be verified without a broker (Kafka's own {@code MockProducer}/{@code
 * MockConsumer} stand in for the clients). The full broker round-trip (append + full-log replay) is
 * exercised by the WS4d Docker integration test.
 */
class KafkaEpochTransportTest {

    private static final String TOPIC = "t1-epoch-events";

    /**
     * The epoch-events topic must never lose history: every instance's fold replays the whole log
     * from the beginning, so a {@code cleanup.policy} including {@code compact} or a finite
     * {@code retention.ms} silently amputates the join/commit history — a restarted instance then
     * folds the truncated log, sees epoch 0, and treats an established domain as a cold start.
     * {@code requireEternalLogConfig} is the pure validator the broker-backed constructor runs at
     * startup, mirroring the single-partition fail-fast.
     *
     * Asserts a compacting policy and a finite retention each fail fast, while delete-with-infinite
     * -retention passes, and an unknown (null/unparseable) value is tolerated as not provably wrong.
     */
    @Test
    void requireEternalLogConfigFailsFastOnCompactionOrFiniteRetention() {
        assertThrows(IllegalStateException.class,
                () -> KafkaEpochTransport.requireEternalLogConfig(TOPIC, "compact", "-1"),
                "a compacting epoch-events topic removes events the fold needs and must fail startup");
        assertThrows(IllegalStateException.class,
                () -> KafkaEpochTransport.requireEternalLogConfig(TOPIC, "compact,delete", "-1"),
                "compact,delete still compacts and must fail startup");
        assertThrows(IllegalStateException.class,
                () -> KafkaEpochTransport.requireEternalLogConfig(TOPIC, "delete", "604800000"),
                "finite retention silently deletes join/commit history and must fail startup");
        assertThrows(IllegalStateException.class,
                () -> KafkaEpochTransport.requireEternalLogConfig(TOPIC, "delete", "0"),
                "zero retention is finite retention and must fail startup");

        KafkaEpochTransport.requireEternalLogConfig(TOPIC, "delete", "-1");   // the documented config
        KafkaEpochTransport.requireEternalLogConfig(TOPIC, null, null);       // unknown: tolerated
        KafkaEpochTransport.requireEternalLogConfig(TOPIC, "delete", "not-a-number");
    }

    /**
     * Every append targets partition 0 explicitly — the one partition every fold reads. Left to the
     * producer's own partitioner, a null-key send would scatter events across the partitions of a
     * mis-created topic, landing a join or a publication where no fold ever looks — silent,
     * permanent protocol-event loss.
     */
    @Test
    void appendPinsEveryEventToPartitionZero() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        try (KafkaEpochTransport transport = new KafkaEpochTransport(TOPIC, producer, singlePartitionConsumer())) {
            transport.append(new ParsleyEpochEvent.SnapshotRequested("member-a"));
            transport.append(new ParsleyEpochEvent.Leave("member-a"));
        }

        assertEquals(2, producer.history().size(), "both events must have been sent");
        assertTrue(producer.history().stream().allMatch(record -> Integer.valueOf(0).equals(record.partition())),
                "every epoch event must be pinned to partition 0, the only partition the fold reads");
    }

    /**
     * Construction fails fast when the epoch-events topic has more than one partition: the protocol's
     * total order is a single partition's offset order, so a multi-partition topic (e.g. created with
     * a broker default partition count) is a startup misconfiguration, surfaced once and loudly — not
     * a protocol that silently degrades.
     */
    @Test
    void constructionFailsFastOnAMultiPartitionEpochEventsTopic() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, null, null, null),
                new PartitionInfo(TOPIC, 1, null, null, null)));

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new KafkaEpochTransport(TOPIC, producer, consumer),
                "a 2-partition epoch-events topic must fail transport construction");
        assertTrue(failure.getMessage().contains("exactly 1"),
                "the failure must state the single-partition requirement, got: " + failure.getMessage());
    }

    private static MockConsumer<byte[], byte[]> singlePartitionConsumer() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
        consumer.updateBeginningOffsets(Map.of(new org.apache.kafka.common.TopicPartition(TOPIC, 0), 0L));
        return consumer;
    }

    /**
     * The producer config carries the app's broker settings through, forces byte-array serializers and
     * idempotence, and sets acks=all — the handshake rides its own idempotent producer, not a transaction.
     */
    @Test
    void producerConfigForcesIdempotentByteArrayProducerAndKeepsBrokerSettings() {
        Map<String, Object> appConfigs = Map.of(
                "bootstrap.servers", "broker:9092",
                "security.protocol", "SSL",
                "application.id", "my-streams-app");   // a Streams-only key, tolerated as unused

        Map<String, Object> config = KafkaEpochTransport.producerConfig(appConfigs);

        assertEquals("broker:9092", config.get("bootstrap.servers"), "bootstrap is inherited from appConfigs");
        assertEquals("SSL", config.get("security.protocol"), "broker security settings are inherited");
        assertEquals(ByteArraySerializer.class.getName(),
                config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), "keys are serialised as raw bytes");
        assertEquals(ByteArraySerializer.class.getName(),
                config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), "values are serialised as raw bytes");
        assertEquals(true, config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "the producer is idempotent");
        assertEquals("all", config.get(ProducerConfig.ACKS_CONFIG), "appends wait for full acknowledgement");
    }

    /**
     * The consumer config forces byte-array deserializers, disables auto-commit, and removes any group id —
     * the reader uses manual assignment + seek-to-beginning so every instance replays the whole log with no
     * group and no committed offsets.
     */
    @Test
    void consumerConfigForcesGrouplessFullLogReader() {
        Map<String, Object> appConfigs = Map.of(
                "bootstrap.servers", "broker:9092",
                "group.id", "some-streams-group");   // must be dropped: we assign, not subscribe

        Map<String, Object> config = KafkaEpochTransport.consumerConfig(appConfigs);

        assertEquals("broker:9092", config.get("bootstrap.servers"), "bootstrap is inherited from appConfigs");
        assertEquals(ByteArrayDeserializer.class.getName(),
                config.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), "keys are deserialised as raw bytes");
        assertEquals(ByteArrayDeserializer.class.getName(),
                config.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), "values are deserialised as raw bytes");
        assertEquals(false, config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "auto-commit is disabled");
        assertFalse(config.containsKey(ConsumerConfig.GROUP_ID_CONFIG),
                "the group id is removed — the reader belongs to no consumer group");
    }
}
