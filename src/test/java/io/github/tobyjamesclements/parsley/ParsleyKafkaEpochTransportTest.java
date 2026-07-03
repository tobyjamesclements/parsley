package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link ParsleyKafkaEpochTransport}'s client-config derivation — the part that can be verified
 * without a broker. The full broker round-trip (append + full-log replay) is exercised by the WS4d Docker
 * integration test.
 */
class ParsleyKafkaEpochTransportTest {

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

        Map<String, Object> config = ParsleyKafkaEpochTransport.producerConfig(appConfigs);

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

        Map<String, Object> config = ParsleyKafkaEpochTransport.consumerConfig(appConfigs);

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
