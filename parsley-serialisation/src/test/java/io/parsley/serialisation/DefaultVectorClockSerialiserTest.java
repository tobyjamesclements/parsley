package io.parsley.serialisation;

import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.internal.ImmutableVectorClock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultVectorClockSerialiserTest {

    private static final DefaultVectorClockSerialiser SERIALISER = new DefaultVectorClockSerialiser();
    private static final Partition P0 = new Partition("orders", 0);
    private static final Partition P1 = new Partition("events", 1);

    @Test
    void emptyClockRoundTrip() {
        VectorClock restored = roundTrip(ImmutableVectorClock.empty());
        assertTrue(restored.positions().isEmpty());
    }

    @Test
    void singlePartitionRoundTrip() {
        VectorClock restored = roundTrip(new ImmutableVectorClock(Map.of(P0, 42L)));
        assertEquals(1, restored.positions().size());
        assertEquals(42L, restored.positions().get(P0));
    }

    @Test
    void multiPartitionRoundTrip() {
        VectorClock original = new ImmutableVectorClock(Map.of(P0, 10L, P1, 20L));
        assertEquals(original.positions(), roundTrip(original).positions());
    }

    @Test
    void singlePartitionByteCount() {
        // wire format: [4 count][2 topicLen][topic bytes][4 partition][8 offset]
        Partition p = new Partition("t", 0);
        byte[] bytes = SERIALISER.serialise(new ImmutableVectorClock(Map.of(p, 0L)));
        int expected = 4 + 2 + "t".getBytes().length + 4 + 8;
        assertEquals(expected, bytes.length);
    }

    @Test
    void deserialisedClockSupportsDominatesOp() {
        VectorClock ahead = roundTrip(new ImmutableVectorClock(Map.of(P0, 5L)));
        VectorClock behind = roundTrip(new ImmutableVectorClock(Map.of(P0, 3L)));
        assertTrue(ahead.dominates(behind));
        assertFalse(behind.dominates(ahead));
    }

    @Test
    void unicodeTopicNameRoundTrip() {
        Partition unicode = new Partition("événements", 0);
        VectorClock restored = roundTrip(new ImmutableVectorClock(Map.of(unicode, 7L)));
        assertEquals(7L, restored.positions().get(unicode));
    }

    @Test
    void emptyBytesThrowOnDeserialise() {
        assertThrows(IllegalStateException.class, () -> SERIALISER.deserialise(new byte[0]));
    }

    @Test
    void countWithNoEntriesThrowsOnDeserialise() {
        // count=10 but zero entry bytes follow
        assertThrows(IllegalStateException.class, () -> SERIALISER.deserialise(new byte[]{0, 0, 0, 10}));
    }

    @Test
    void truncatedBytesThrowOnDeserialise() {
        // serialise a valid clock, then truncate mid-topic to trigger EOFException in readFully
        byte[] full = SERIALISER.serialise(new ImmutableVectorClock(Map.of(P0, 42L)));
        byte[] truncated = Arrays.copyOf(full, 10);
        assertThrows(IllegalStateException.class, () -> SERIALISER.deserialise(truncated));
    }

    private static VectorClock roundTrip(VectorClock clock) {
        return SERIALISER.deserialise(SERIALISER.serialise(clock));
    }
}
