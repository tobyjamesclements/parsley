package io.parsley;

import io.parsley.internal.ImmutableVectorClock;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorClockTest {

    private static final Partition P0 = new Partition("t", 0);
    private static final Partition P1 = new Partition("t", 1);

    @Test
    void emptyDominatesEmpty() {
        assertTrue(clock(Map.of()).dominates(clock(Map.of())));
    }

    @Test
    void emptyDoesNotDominateNonEmpty() {
        assertFalse(clock(Map.of()).dominates(clock(Map.of(P0, 5L))));
    }

    @Test
    void equalPositionsDominates() {
        assertTrue(clock(Map.of(P0, 5L)).dominates(clock(Map.of(P0, 5L))));
    }

    @Test
    void strictlyAheadDominates() {
        assertTrue(clock(Map.of(P0, 10L)).dominates(clock(Map.of(P0, 5L))));
    }

    @Test
    void behindOnOnePartitionDoesNotDominate() {
        VectorClock a = clock(Map.of(P0, 5L, P1, 3L));
        VectorClock b = clock(Map.of(P0, 5L, P1, 5L));
        assertFalse(a.dominates(b));
    }

    @Test
    void missingPartitionTreatedAsBehind() {
        VectorClock a = clock(Map.of(P0, 10L));
        VectorClock b = clock(Map.of(P0, 10L, P1, 5L));
        assertFalse(a.dominates(b));
    }

    @Test
    void dominatedByIsSymmetricInverse() {
        VectorClock ahead = clock(Map.of(P0, 10L));
        VectorClock behind = clock(Map.of(P0, 5L));
        assertTrue(behind.dominatedBy(ahead));
        assertFalse(ahead.dominatedBy(behind));
    }

    @Test
    void incomparableClocksNeitherDominatesTheOther() {
        VectorClock a = clock(Map.of(P0, 5L, P1, 3L));
        VectorClock b = clock(Map.of(P0, 3L, P1, 5L));
        assertFalse(a.dominates(b));
        assertFalse(b.dominates(a));
    }

    @Test
    void missingPartitionWithZeroRequiredOffsetIsNotDominated() {
        // -1L sentinel must be strictly less than 0L — offset=0 is not free
        assertFalse(clock(Map.of()).dominates(clock(Map.of(P0, 0L))));
    }

    @Test
    void clockDominatesItselfWhenPositionsAreEqual() {
        VectorClock a = clock(Map.of(P0, 5L, P1, 3L));
        VectorClock b = clock(Map.of(P0, 5L, P1, 3L));
        assertTrue(a.dominates(b));
        assertTrue(b.dominates(a));
    }

    private static VectorClock clock(Map<Partition, Long> positions) {
        return new ImmutableVectorClock(positions);
    }
}
