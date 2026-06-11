package io.parsley.internal;

import io.parsley.Partition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ImmutableVectorClockTest {

    private static final Partition P0 = new Partition("t", 0);
    private static final Partition P1 = new Partition("t", 1);

    @Test
    void emptyHasNoPositions() {
        assertTrue(ImmutableVectorClock.empty().positions().isEmpty());
    }

    @Test
    void advanceOnEmptySetsOffset() {
        ImmutableVectorClock c = ImmutableVectorClock.empty().advance(P0, 5L);
        assertEquals(5L, c.positions().get(P0));
        assertEquals(1, c.positions().size());
    }

    @Test
    void advanceUsesMaxSemantics() {
        ImmutableVectorClock c = ImmutableVectorClock.empty()
                .advance(P0, 5L)
                .advance(P0, 3L);
        assertEquals(5L, c.positions().get(P0));
    }

    @Test
    void advanceDoesNotMutateOriginal() {
        ImmutableVectorClock original = ImmutableVectorClock.empty();
        original.advance(P0, 5L);
        assertTrue(original.positions().isEmpty());
    }

    @Test
    void mergeTakesMaxPerPartition() {
        ImmutableVectorClock a = new ImmutableVectorClock(Map.of(P0, 10L, P1, 2L));
        ImmutableVectorClock b = new ImmutableVectorClock(Map.of(P0, 3L, P1, 8L));
        ImmutableVectorClock merged = a.merge(b);
        assertEquals(10L, merged.positions().get(P0));
        assertEquals(8L, merged.positions().get(P1));
    }

    @Test
    void mergeUnifiesDistinctPartitions() {
        ImmutableVectorClock a = new ImmutableVectorClock(Map.of(P0, 5L));
        ImmutableVectorClock b = new ImmutableVectorClock(Map.of(P1, 3L));
        ImmutableVectorClock merged = a.merge(b);
        assertEquals(2, merged.positions().size());
        assertEquals(5L, merged.positions().get(P0));
        assertEquals(3L, merged.positions().get(P1));
    }

    @Test
    void mergeDoesNotMutateOperands() {
        ImmutableVectorClock a = new ImmutableVectorClock(Map.of(P0, 5L));
        ImmutableVectorClock b = new ImmutableVectorClock(Map.of(P1, 3L));
        a.merge(b);
        assertEquals(1, a.positions().size(), "a must not be mutated by merge");
        assertEquals(1, b.positions().size(), "b must not be mutated by merge");
    }

    @Test
    void positionsMapIsImmutable() {
        ImmutableVectorClock c = ImmutableVectorClock.empty().advance(P0, 1L);
        assertThrows(UnsupportedOperationException.class, () -> c.positions().put(P0, 99L));
    }

    @Test
    void constructorMakesDefensiveCopy() {
        Map<Partition, Long> mutable = new HashMap<>();
        mutable.put(P0, 5L);
        ImmutableVectorClock clock = new ImmutableVectorClock(mutable);
        mutable.put(P0, 99L);
        assertEquals(5L, clock.positions().get(P0));
    }
}
