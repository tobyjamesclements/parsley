package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParsleySerializerTest {

    // Non-zero partition is deliberate: the round-trip test verifies the partition is preserved.
    private static final TopicPartition T1 = new TopicPartition("t1", 2);

    private final ParsleySerializer<String, String> serializer =
            new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> Serdes.String()));

    /**
     * Serialising and then deserialising a {@code ParsleyRecord} round-trips every field:
     * key, value, timestamp, source partition (including non-zero partition number), source
     * offset, dependency bytes, and user headers including headers with null values.
     *
     * Asserts that all fields match the original record after the round-trip.
     */
    @Test
    void roundTripsEveryField() {
        CausalDependencies deps = CausalDependencies.builder()
                .require(new CausalPosition(CausalPosition.deriveUuid("t1"), 0, 4))
                .build();
        List<ParsleyHeader> userHeaders = List.of(
                new ParsleyHeader("h1", "a".getBytes()),
                new ParsleyHeader("h2", null));
        ParsleyRecord<String, String> record = buildRecord("key", "value", 123L, T1, 7L, deps, userHeaders);

        ParsleyRecord<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertEquals("key", out.key(), "key must round-trip");
        assertEquals("value", out.value(), "value must round-trip");
        assertEquals(123L, out.timestamp(), "timestamp must round-trip");
        assertEquals(T1, out.sourcePartition(), "source partition (including non-zero partition number) must round-trip");
        assertEquals(7L, out.sourceOffset(), "source offset must round-trip");
        assertArrayEquals(deps.toBytes(), out.encodedDependencies(), "dependency bytes must round-trip");
        // 2 user headers + CAUSAL_DEPENDENCIES + SRC_TOPIC + SRC_PARTITION + SRC_OFFSET = 6
        assertEquals(6, out.headers().size(), "all six headers must be preserved");
        assertEquals("h1", out.headers().get(0).key(), "first user header key must round-trip");
        assertArrayEquals("a".getBytes(), out.headers().get(0).value(), "first user header value must round-trip");
        assertEquals("h2", out.headers().get(1).key(), "second user header key must round-trip");
        assertNull(out.headers().get(1).value(), "null user header value must round-trip as null");
        assertEquals(deps, CausalDependencies.fromBytes(out.encodedDependencies()),
                "decoded dependencies must equal the original");
    }

    /**
     * A record with a null key and null value round-trips without error, and the null
     * dependency field is preserved as null.
     *
     * Asserts that key, value, and encodedDependencies are all null after the round-trip.
     */
    @Test
    void roundTripsNullKeyAndValue() {
        ParsleyRecord<String, String> record = buildRecord(null, null, 0L, T1, 0L, null, List.of());

        ParsleyRecord<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertNull(out.key(), "null key must round-trip as null");
        assertNull(out.value(), "null value must round-trip as null");
        assertNull(out.encodedDependencies(), "null dependency field must round-trip as null");
    }

    /**
     * The serializer passes the record's source topic — not the buffer store's changelog
     * topic name — to the key and value serdes on both serialise and deserialise.
     *
     * <p>This matters because topic-specific serdes (e.g. schema-registry Avro serdes) must
     * use the correct subject to locate the right schema.
     *
     * Asserts that each spy serde sees the source topic name twice (once on serialise,
     * once on deserialise).
     */
    @Test
    void serialisationUsesTheRecordSourceTopicNotTheStoreName() {
        SpySerde keySpy = new SpySerde();
        SpySerde valueSpy = new SpySerde();
        ParsleySerializer<String, String> spying =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> keySpy, topic -> valueSpy));
        ParsleyRecord<String, String> record =
                buildRecord("k", "v", 0L, T1, 1L, CausalDependencies.empty(), List.of());

        spying.deserialize(spying.serialize(record));

        assertEquals(List.of("t1", "t1"), keySpy.topics,
                "key serde must be invoked with the source topic on both serialise and deserialise");
        assertEquals(List.of("t1", "t1"), valueSpy.topics,
                "value serde must be invoked with the source topic on both serialise and deserialise");
    }

    /**
     * {@code ParsleySerializer.deserialize()} rejects a byte array whose leading version
     * byte is not a recognised format version.
     *
     * Asserts that {@code IllegalStateException} is thrown for an unknown version byte.
     */
    @Test
    void deserializeRejectsAnUnknownFormatVersion() {
        // Leading version byte 99 (not 2) — rejected before any field is read.
        assertThrows(IllegalStateException.class, () -> serializer.deserialize(new byte[]{99}),
                "deserialize must throw for an unrecognised format version");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyRecord<String, String> buildRecord(String key, String value, long timestamp,
                                                              TopicPartition tp, long offset,
                                                              CausalDependencies deps,
                                                              List<ParsleyHeader> userHeaders) {
        List<ParsleyHeader> headers = new ArrayList<>(userHeaders);
        if (deps != null) {
            headers.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        return new ParsleyRecord<>(key, value, timestamp, headers);
    }

    /** A String serde that records the topic argument it was invoked with. */
    private static final class SpySerde implements Serde<String> {
        final List<String> topics = new ArrayList<>();
        private final Serde<String> delegate = Serdes.String();

        @Override
        public Serializer<String> serializer() {
            return (topic, data) -> {
                topics.add(topic);
                return delegate.serializer().serialize(topic, data);
            };
        }

        @Override
        public Deserializer<String> deserializer() {
            return (topic, data) -> {
                topics.add(topic);
                return delegate.deserializer().deserialize(topic, data);
            };
        }
    }
}
