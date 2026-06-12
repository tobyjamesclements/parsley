package io.parsley.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaVectorClockSerialiserTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();

    @Test
    void emptyClockRoundTrips() {
        assertEquals(KafkaVectorClock.empty(),
                SERIALISER.deserialise(SERIALISER.serialise(KafkaVectorClock.empty())));
    }

    @Test
    void singleEntryClockRoundTrips() {
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(new TopicPartition("orders", 0), 42L));
        assertEquals(clock, SERIALISER.deserialise(SERIALISER.serialise(clock)));
    }

    @Test
    void multiEntryClockRoundTrips() {
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(
                new TopicPartition("orders", 0), 42L,
                new TopicPartition("payments", 3), 7L,
                new TopicPartition("shipments", 12), Long.MAX_VALUE));
        assertEquals(clock, SERIALISER.deserialise(SERIALISER.serialise(clock)));
    }

    @Test
    void nonAsciiTopicNameRoundTrips() {
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(
                new TopicPartition("commandes-évènements-注文", 5), 13L));
        assertEquals(clock, SERIALISER.deserialise(SERIALISER.serialise(clock)));
    }

    @Test
    void truncatedBytesThrowIllegalStateException() {
        byte[] valid = SERIALISER.serialise(
                new KafkaVectorClock(Map.of(new TopicPartition("orders", 0), 42L)));
        byte[] truncated = Arrays.copyOf(valid, valid.length - 3);

        assertThrows(IllegalStateException.class, () -> SERIALISER.deserialise(truncated));
    }

    @Test
    void garbageBytesThrowIllegalStateException() {
        // The UNRESOLVABLE_CLOCK classification in the engine relies on deserialise
        // throwing rather than returning a bogus clock.
        assertThrows(IllegalStateException.class, () -> SERIALISER.deserialise(new byte[]{1, 2}));
    }
}
