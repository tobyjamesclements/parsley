package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.jspecify.annotations.Nullable;

class ParsleySerializerTest {

    // Non-zero partition is deliberate: the round-trip test verifies the partition is preserved.
    private static final TopicPartition C1 = new TopicPartition("c1", 2);
    private static final Uuid C1_ID = Uuid.randomUuid();

    private final ParsleySerializer<String, String> serializer =
            new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> Serdes.String()));

    /**
     * Serialising and then deserialising a {@code ParsleyMessage} round-trips every field:
     * key, value, timestamp, source partition (including non-zero partition number), source
     * offset, dependency bytes, and user headers including headers with null values.
     *
     * Asserts that all fields match the original record after the round-trip.
     */
    @Test
    void roundTripsEveryField() {
        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(Uuid.randomUuid(), 0, 4);
        List<ParsleyHeader> userHeaders = List.of(
                new ParsleyHeader("h1", "a".getBytes()),
                new ParsleyHeader("h2", null));
        ParsleyMessage<String, String> record = buildRecord("key", "value", 123L, C1, 7L, deps, userHeaders);

        ParsleyMessage<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertEquals("key", out.key(), "key must round-trip");
        assertEquals("value", out.value(), "value must round-trip");
        assertEquals(123L, out.timestamp(), "timestamp must round-trip");
        assertEquals(C1, new TopicPartition(out.topic(), out.partition()),
                "source topic and partition (including non-zero partition number) must round-trip");
        assertEquals(7L, out.offset(), "source offset must round-trip");
        assertEquals(deps, out.dependencies(), "the dependency clock must round-trip");
        assertEquals(2, out.headers().size(), "the user headers must be preserved (no internal headers)");
        assertEquals("h1", out.headers().get(0).key(), "first user header key must round-trip");
        assertArrayEquals("a".getBytes(), out.headers().get(0).value(), "first user header value must round-trip");
        assertEquals("h2", out.headers().get(1).key(), "second user header key must round-trip");
        assertNull(out.headers().get(1).value(), "null user header value must round-trip as null");
    }

    /**
     * A record with a null key and null value round-trips without error, and the null
     * dependency field is preserved as null.
     *
     * Asserts that key, value, and encodedDependencies are all null after the round-trip.
     */
    @Test
    void roundTripsNullKeyAndValue() {
        ParsleyMessage<String, String> record = buildRecord(null, null, 0L, C1, 0L, null, List.of());

        ParsleyMessage<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertNull(out.key(), "null key must round-trip as null");
        assertNull(out.value(), "null value must round-trip as null");
        assertEquals(ParsleyVectorClock.empty(), out.dependencies(), "empty dependencies must round-trip as empty");
    }

    /**
     * A record with an empty (zero-length, but non-null) key and value round-trips as empty
     * strings, not as null. This distinguishes the wire format's null-sentinel length ({@code -1})
     * from a genuinely empty byte array (length {@code 0}) — a key/value with no content is not
     * the same as an absent one.
     *
     * Asserts that key and value round-trip as {@code ""}, not {@code null}.
     */
    @Test
    void roundTripsEmptyButNonNullKeyAndValue() {
        ParsleyMessage<String, String> record = buildRecord("", "", 0L, C1, 0L, null, List.of());

        ParsleyMessage<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertEquals("", out.key(), "an empty (zero-length) key must round-trip as \"\", not null");
        assertEquals("", out.value(), "an empty (zero-length) value must round-trip as \"\", not null");
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
        ParsleyMessage<String, String> record =
                buildRecord("k", "v", 0L, C1, 1L, ParsleyVectorClock.empty(), List.of());

        spying.deserialize(spying.serialize(record));

        assertEquals(List.of("c1", "c1"), keySpy.topics,
                "key serde must be invoked with the source topic on both serialise and deserialise");
        assertEquals(List.of("c1", "c1"), valueSpy.topics,
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

    /**
     * {@code ParsleySerializer.deserializeIndexMetadata()} rejects a byte array whose leading
     * version byte is not a recognised format version, the same as {@link #deserializeRejectsAnUnknownFormatVersion}
     * asserts for the full {@code deserialize()} path.
     *
     * Asserts that {@code IllegalStateException} is thrown for an unknown version byte.
     */
    @Test
    void deserializeIndexMetadataRejectsAnUnknownFormatVersion() {
        // Leading version byte 99 (not 2) — rejected before any field is read.
        assertThrows(IllegalStateException.class, () -> serializer.deserializeIndexMetadata(new byte[]{99}),
                "deserializeIndexMetadata must throw for an unrecognised format version");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyMessage<String, String> buildRecord(@Nullable String key, @Nullable String value,
                                                              long timestamp, TopicPartition tp, long offset,
                                                              @Nullable ParsleyVectorClock deps,
                                                              List<ParsleyHeader> userHeaders) {
        return new ParsleyMessage<>(tp.topic(), C1_ID, tp.partition(), offset, timestamp,
                key, value, userHeaders, deps == null ? ParsleyVectorClock.empty() : deps);
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
