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

    private static final TopicPartition ORDERS_2 = new TopicPartition("orders", 2);

    private final ParsleySerializer<String, String> serializer =
            new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> Serdes.String()));

    /** Builds a record with the given user headers plus the required internal coordinate/clock headers. */
    private static ParsleyRecord<String, String> rec(String key, String value, long timestamp,
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

    @Test
    void roundTripsEveryField() {
        CausalDependencies deps = CausalDependencies.empty().advance(CausalPosition.deriveUuid("prices"), 0, 4);
        List<ParsleyHeader> userHeaders = List.of(
                new ParsleyHeader("h1", "a".getBytes()),
                new ParsleyHeader("h2", null));
        ParsleyRecord<String, String> record = rec("key", "value", 123L, ORDERS_2, 7L, deps, userHeaders);

        ParsleyRecord<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertEquals("key", out.key());
        assertEquals("value", out.value());
        assertEquals(123L, out.timestamp());
        assertEquals(ORDERS_2, out.sourcePartition());
        assertEquals(7L, out.sourceOffset());
        assertArrayEquals(deps.toBytes(), out.encodedDependencies());
        // 2 user headers + CAUSAL_DEPENDENCIES + SRC_TOPIC + SRC_PARTITION + SRC_OFFSET = 6
        assertEquals(6, out.headers().size());
        assertEquals("h1", out.headers().get(0).key());
        assertArrayEquals("a".getBytes(), out.headers().get(0).value());
        assertEquals("h2", out.headers().get(1).key());
        assertNull(out.headers().get(1).value());
        assertEquals(deps, CausalDependencies.fromBytes(out.encodedDependencies()),
                "the dependency clock is recovered by decoding the restored encodedDependencies");
    }

    @Test
    void roundTripsNullKeyAndValue() {
        ParsleyRecord<String, String> record = rec(null, null, 0L, ORDERS_2, 0L, null, List.of());

        ParsleyRecord<String, String> out = serializer.deserialize(serializer.serialize(record));

        assertNull(out.key());
        assertNull(out.value());
        assertNull(out.encodedDependencies());
    }

    @Test
    void serialisationUsesTheRecordSourceTopicNotTheStoreName() {
        SpySerde keySpy = new SpySerde();
        SpySerde valueSpy = new SpySerde();
        ParsleySerializer<String, String> spying =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> keySpy, topic -> valueSpy));
        ParsleyRecord<String, String> record =
                rec("k", "v", 0L, ORDERS_2, 1L, CausalDependencies.empty(), List.of());

        spying.deserialize(spying.serialize(record));

        // Each serde is invoked twice — serialise on buffer, deserialise on restore — always with
        // the record's source topic ("orders"), never the buffer store's changelog name.
        assertEquals(List.of("orders", "orders"), keySpy.topics);
        assertEquals(List.of("orders", "orders"), valueSpy.topics);
    }

    @Test
    void deserializeRejectsAnUnknownFormatVersion() {
        // Leading version byte 99 (not 2) — rejected before any field is read.
        assertThrows(IllegalStateException.class, () -> serializer.deserialize(new byte[]{99}));
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
