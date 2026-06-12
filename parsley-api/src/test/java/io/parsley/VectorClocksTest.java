package io.parsley;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorClocksTest {

    // --- satisfied ---

    @Test
    void emptyRequiredIsAlwaysSatisfied() {
        assertTrue(VectorClocks.<String, Long>satisfied(Map.of(), Map.of()));
        assertTrue(VectorClocks.satisfied(Map.<String, Long>of(), Map.of("p", 5L)));
    }

    @Test
    void partitionAbsentFromFrontierIsUnsatisfied() {
        assertFalse(VectorClocks.satisfied(Map.of("p", 0L), Map.of()));
        assertFalse(VectorClocks.satisfied(Map.of("p", 0L), Map.of("other", 99L)));
    }

    @Test
    void equalPositionIsSatisfied() {
        assertTrue(VectorClocks.satisfied(Map.of("p", 5L), Map.of("p", 5L)));
    }

    @Test
    void laterFrontierPositionIsSatisfied() {
        assertTrue(VectorClocks.satisfied(Map.of("p", 5L), Map.of("p", 6L)));
    }

    @Test
    void earlierFrontierPositionIsUnsatisfied() {
        assertFalse(VectorClocks.satisfied(Map.of("p", 5L), Map.of("p", 4L)));
    }

    @Test
    void oneLaggingPartitionMakesMultiPartitionRequirementUnsatisfied() {
        Map<String, Long> required = Map.of("a", 2L, "b", 7L);
        assertFalse(VectorClocks.satisfied(required, Map.of("a", 5L, "b", 6L)));
        assertTrue(VectorClocks.satisfied(required, Map.of("a", 2L, "b", 7L)));
    }

    // --- merge ---

    @Test
    void mergeTakesPerPartitionMaximum() {
        Map<String, Long> merged = VectorClocks.merge(
                Map.of("a", 5L, "b", 1L),
                Map.of("a", 3L, "b", 9L));
        assertEquals(Map.of("a", 5L, "b", 9L), merged);
    }

    @Test
    void mergeUnionsDisjointPartitions() {
        Map<String, Long> merged = VectorClocks.merge(Map.of("a", 1L), Map.of("b", 2L));
        assertEquals(Map.of("a", 1L, "b", 2L), merged);
    }

    @Test
    void mergeResultIsImmutable() {
        Map<String, Long> merged = VectorClocks.merge(Map.of("a", 1L), Map.of("b", 2L));
        assertThrows(UnsupportedOperationException.class, () -> merged.put("c", 3L));
    }

    // --- advance ---

    @Test
    void advanceAddsNewPartition() {
        assertEquals(Map.of("p", 4L), VectorClocks.advance(Map.of(), "p", 4L));
    }

    @Test
    void advanceMovesPartitionForward() {
        assertEquals(Map.of("p", 7L), VectorClocks.advance(Map.of("p", 4L), "p", 7L));
    }

    @Test
    void advanceNeverMovesBackwards() {
        assertEquals(Map.of("p", 7L), VectorClocks.advance(Map.of("p", 7L), "p", 3L));
    }

    @Test
    void advanceResultIsImmutable() {
        Map<String, Long> advanced = VectorClocks.advance(Map.of(), "p", 1L);
        assertThrows(UnsupportedOperationException.class, () -> advanced.put("q", 2L));
    }

    @Test
    void advanceDoesNotMutateInput() {
        Map<String, Long> input = new HashMap<>(Map.of("p", 1L));
        VectorClocks.advance(input, "p", 9L);
        assertEquals(Map.of("p", 1L), input);
    }

    // --- non-Long positions (Kinesis-style sequence numbers exceed long) ---

    @Test
    void algebraWorksWithBigIntegerPositions() {
        BigInteger low = new BigInteger("49590338271490256608559692538361571095921575989136588898");
        BigInteger high = low.add(BigInteger.ONE);

        assertTrue(VectorClocks.satisfied(Map.of("shard-0", low), Map.of("shard-0", high)));
        assertFalse(VectorClocks.satisfied(Map.of("shard-0", high), Map.of("shard-0", low)));
        assertEquals(Map.of("shard-0", high),
                VectorClocks.merge(Map.of("shard-0", low), Map.of("shard-0", high)));
        assertEquals(Map.of("shard-0", high),
                VectorClocks.advance(Map.of("shard-0", low), "shard-0", high));
    }
}
