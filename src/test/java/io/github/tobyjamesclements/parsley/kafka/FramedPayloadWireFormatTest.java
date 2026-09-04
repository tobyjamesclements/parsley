package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.CausesCodec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Establishes that a framed binary payload passes through byte for byte.
 *
 * <p>The framing is the Confluent Schema Registry layout: a zero magic byte, a four-byte
 * big-endian schema id, then an opaque body. A leading zero byte and embedded nulls are what
 * catch an implementation that wraps, prefixes or length-encodes what it was given.
 *
 * <p>The body is opaque on purpose. Parsley's topology carries bytes and applies application
 * serdes only after the delivery decision, so the payload's own encoding cannot affect the
 * claim, and a real codec library would add a dependency without adding evidence.
 */
class FramedPayloadWireFormatTest {
    private static final int SCHEMA_ID = 7;

    /** An order, encoded as a length-prefixed item name followed by a quantity. */
    record Order(String item, int qty) {
    }

    /** A serde in Schema Registry wire format: magic byte, schema id, then the body. */
    static final class FramedSerde implements Serde<Order> {
        @Override
        public Serializer<Order> serializer() {
            return (topic, order) -> {
                if (order == null) {
                    return null;
                }
                byte[] item = order.item().getBytes(StandardCharsets.UTF_8);
                return ByteBuffer.allocate(1 + Integer.BYTES + Integer.BYTES + item.length + Integer.BYTES)
                        .put((byte) 0)
                        .putInt(SCHEMA_ID)
                        .putInt(item.length)
                        .put(item)
                        .putInt(order.qty())
                        .array();
            };
        }

        @Override
        public Deserializer<Order> deserializer() {
            return (topic, bytes) -> {
                if (bytes == null) {
                    return null;
                }
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                if (buffer.get() != 0) {
                    throw new IllegalArgumentException("not schema-registry wire format");
                }
                assertEquals(SCHEMA_ID, buffer.getInt());
                byte[] item = new byte[buffer.getInt()];
                buffer.get(item);
                return new Order(new String(item, StandardCharsets.UTF_8), buffer.getInt());
            };
        }
    }

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    /** A framed binary payload passes through byte exact, and causes ride a header. */
    @Test
    void framedPayloadPassesThroughByteExact() {
        Serde<Order> framed = new FramedSerde();
        Channel<String, Order> in = Channel.of("framed-in", Serdes.String(), framed);
        Channel<String, Order> out = Channel.of("framed-out", Serdes.String(), framed);
        ProcessDefinition definition = ProcessDefinition.named("framed")
                .receives(in, (delivery, state) -> Effects.builder()
                        .send(out, delivery.key(), new Order(delivery.value().item(), delivery.value().qty() * 2))
                        .build())
                .sends(out)
                .build();

        Map<String, TopicInfo> topics = Map.of(
                "framed-in", new TopicInfo(new UUID(7, 1), 1),
                "framed-out", new TopicInfo(new UUID(7, 2), 1));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "framed-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(ProcessTopology.build(
                definition, topics, TopicIdentitySource.ALL_ALIVE,
                Duration.ofSeconds(1)), props);

        byte[] valueIn = framed.serializer().serialize("framed-in", new Order("widget", 3));
        assertEquals(0, valueIn[0], "the framing must begin with a zero magic byte");
        driver.createInputTopic("framed-in", new ByteArraySerializer(), new ByteArraySerializer())
                .pipeInput(new TestRecord<>("k".getBytes(StandardCharsets.UTF_8), valueIn));

        TestRecord<byte[], byte[]> record = driver.createOutputTopic(
                "framed-out", new ByteArrayDeserializer(), new ByteArrayDeserializer()).readRecord();

        assertArrayEquals(framed.serializer().serialize("framed-out", new Order("widget", 6)), record.value());
        assertEquals(6, framed.deserializer().deserialize("framed-out", record.value()).qty());
        assertNotNull(record.headers().lastHeader(CausesCodec.HEADER_KEY));
    }
}
