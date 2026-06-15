package io.parsley;

import org.apache.kafka.common.TopicPartition;
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

class BufferedRecordCodecTest {

    private static final TopicPartition ORDERS_2 = new TopicPartition("orders", 2);

    private final BufferedRecordCodec<String, String> codec =
            new BufferedRecordCodec<>(topic -> Serdes.String(), topic -> Serdes.String());

    @Test
    void roundTripsEveryField() {
        VectorClock deps = VectorClock.empty().advance(new TopicPartition("prices", 0), 4);
        ParsleyRecord<String, String> record = new ParsleyRecord<>(
                "key", "value", 123L,
                List.of(new ParsleyHeader("h1", "a".getBytes()), new ParsleyHeader("h2", null)),
                deps.toBytes(), ORDERS_2, 7L);

        ParsleyRecord<String, String> out = codec.deserialize(codec.serialize(record));

        assertEquals("key", out.key());
        assertEquals("value", out.value());
        assertEquals(123L, out.timestamp());
        assertEquals(ORDERS_2, out.sourcePartition());
        assertEquals(7L, out.sourceOffset());
        assertArrayEquals(deps.toBytes(), out.encodedDependencies());
        assertEquals(2, out.headers().size());
        assertEquals("h1", out.headers().get(0).key());
        assertArrayEquals("a".getBytes(), out.headers().get(0).value());
        assertEquals("h2", out.headers().get(1).key());
        assertNull(out.headers().get(1).value());
        assertEquals(deps, VectorClock.fromBytes(out.encodedDependencies()),
                "the dependency clock is recovered by decoding the restored encodedDependencies");
    }

    @Test
    void roundTripsNullKeyAndValue() {
        ParsleyRecord<String, String> record =
                new ParsleyRecord<>(null, null, 0L, List.of(), null, ORDERS_2, 0L);

        ParsleyRecord<String, String> out = codec.deserialize(codec.serialize(record));

        assertNull(out.key());
        assertNull(out.value());
        assertNull(out.encodedDependencies());
    }

    @Test
    void serialisationUsesTheRecordSourceTopicNotTheStoreName() {
        SpySerde keySpy = new SpySerde();
        SpySerde valueSpy = new SpySerde();
        BufferedRecordCodec<String, String> spying =
                new BufferedRecordCodec<>(topic -> keySpy, topic -> valueSpy);
        ParsleyRecord<String, String> record =
                new ParsleyRecord<>("k", "v", 0L, List.of(), VectorClock.empty().toBytes(), ORDERS_2, 1L);

        spying.deserialize(spying.serialize(record));

        // Each serde is invoked twice — serialise on buffer, deserialise on restore — always with
        // the record's source topic ("orders"), never the buffer store's changelog name.
        assertEquals(List.of("orders", "orders"), keySpy.topics);
        assertEquals(List.of("orders", "orders"), valueSpy.topics);
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
