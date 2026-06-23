package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link ParsleyClock} vector clock — the one type serving as both the node frontier and a
 * record's dependencies. {@link #dominates} is the satisfaction check; {@link #missing} the causal gap.
 */
class ParsleyClockTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * An empty dependency clock carries no requirements and is therefore dominated by any frontier,
     * including an empty one.
     *
     * Asserts that an empty clock is dominated by both an empty and a non-empty frontier.
     */
    @Test
    void emptyClockIsDominatedByAnything() {
        assertTrue(ParsleyClock.empty().dominates(ParsleyClock.empty()),
                "an empty frontier must dominate empty dependencies");
        assertTrue(ParsleyClock.empty().observe(T1_ID, 0, 7).dominates(ParsleyClock.empty()),
                "a non-empty frontier must dominate empty dependencies");
    }

    /**
     * A non-empty dependency clock is dominated only when the frontier has reached or passed the
     * required offset for every tracked (topicId, partition) pair.
     *
     * Asserts that {@code dominates} is false when the frontier is behind, true when at or ahead.
     */
    @Test
    void dominatesRequiresEveryPartitionToBeCaughtUp() {
        ParsleyClock required = ParsleyClock.empty().observe(T1_ID, 0, 3);
        assertFalse(ParsleyClock.empty().dominates(required),
                "empty frontier cannot dominate a non-empty dependency");
        assertFalse(ParsleyClock.empty().observe(T1_ID, 0, 2).dominates(required),
                "frontier at offset 2 does not dominate a requirement of offset 3");
        assertTrue(ParsleyClock.empty().observe(T1_ID, 0, 3).dominates(required),
                "frontier at exactly the required offset must dominate");
        assertTrue(ParsleyClock.empty().observe(T1_ID, 0, 4).dominates(required),
                "frontier ahead of the required offset must dominate");
    }

    /**
     * {@code observe} for the same (topicId, partition) keeps the higher offset, because a record at
     * the higher offset implies the lower offset has already been produced.
     *
     * Asserts that observing a lower offset after a higher one does not regress the clock.
     */
    @Test
    void observeTakesTheMaximum() {
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 5),
                ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T1_ID, 0, 2),
                "observe must retain only the maximum offset per (topicId, partition)");
    }

    /**
     * Merging two clocks produces a new clock that takes the per-partition maximum offset from each.
     *
     * Asserts that the merged clock holds the higher offset for each (topicId, partition).
     */
    @Test
    void mergeTakesPerPartitionMaximum() {
        ParsleyClock a = ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 1);
        ParsleyClock b = ParsleyClock.empty().observe(T1_ID, 0, 1).observe(T2_ID, 0, 9);
        assertEquals(
                ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 9),
                a.merge(b),
                "merged clock must take the per-partition maximum from both clocks");
    }

    /**
     * {@code without} removes a single (topicId, partition) coordinate, leaving the rest intact.
     *
     * Asserts that the removed coordinate is gone and an unrelated one remains.
     */
    @Test
    void withoutRemovesASingleCoordinate() {
        ParsleyClock clock = ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 9);
        assertEquals(ParsleyClock.empty().observe(T2_ID, 0, 9), clock.without(T1_ID, 0),
                "without must drop only the named coordinate");
        assertEquals(clock, clock.without(T1_ID, 1),
                "without must be a no-op for a coordinate not present");
    }

    /**
     * {@code missing} returns an empty clock when the given frontier satisfies all required
     * dependencies, including the case where the dependencies are themselves empty.
     *
     * Asserts that the gap is empty for a satisfied frontier and for empty dependencies.
     */
    @Test
    void missingIsEmptyWhenFrontierSatisfiesDependencies() {
        ParsleyClock required = ParsleyClock.empty().observe(T1_ID, 0, 3);
        assertTrue(required.missing(ParsleyClock.empty().observe(T1_ID, 0, 3)).isEmpty(),
                "gap must be empty when frontier is at exactly the required offset");
        assertTrue(required.missing(ParsleyClock.empty().observe(T1_ID, 0, 9)).isEmpty(),
                "gap must be empty when frontier is ahead of the required offset");
        assertTrue(ParsleyClock.empty().missing(ParsleyClock.empty()).isEmpty(),
                "gap must be empty when dependencies are empty");
    }

    /**
     * {@code missing} reports the per-partition shortfall when the frontier is behind one or more
     * required offsets; an absent frontier coordinate counts as {@code -1}, so requiring offset {@code n}
     * against it is a gap of {@code n + 1}.
     *
     * Asserts the gap clock records the correct per-partition shortfalls.
     */
    @Test
    void missingReportsPerPartitionShortfall() {
        ParsleyClock required = ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T2_ID, 0, 2);
        // T1: required 5, frontier at 1 → gap 4. T2: required 2, frontier absent (-1) → gap 3.
        ParsleyClock gap = required.missing(ParsleyClock.empty().observe(T1_ID, 0, 1));
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 4).observe(T2_ID, 0, 3), gap,
                "gap must reflect the per-partition shortfall for each unsatisfied dependency");
    }

    /**
     * A clock round-trips through its binary wire format.
     *
     * Asserts that deserialising the serialised form produces an equal clock.
     */
    @Test
    void serialisationRoundTrips() {
        ParsleyClock clock = ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T2_ID, 0, 2);
        assertEquals(clock, ParsleyClock.fromBytes(clock.toBytes()),
                "clock must round-trip through binary serialisation");
    }

    /**
     * A dependency on a coordinate under an old topic UUID is not dominated by a frontier that has
     * advanced under a new UUID for the same topic name and partition. Topic UUIDs uniquely identify
     * incarnations.
     *
     * Asserts that only a frontier using the old UUID dominates the dependency.
     */
    @Test
    void recreatedTopicUuidDistinguishesFromOldIncarnation() {
        Uuid oldUuid = new Uuid(0L, 1L);
        Uuid newUuid = new Uuid(0L, 2L);   // same name, different incarnation
        ParsleyClock required = ParsleyClock.empty().observe(oldUuid, 0, 5L);

        assertFalse(ParsleyClock.empty().observe(newUuid, 0, 5L).dominates(required),
                "new-UUID frontier must not dominate an old-UUID dependency");
        assertTrue(ParsleyClock.empty().observe(oldUuid, 0, 5L).dominates(required),
                "old-UUID frontier at matching offset must dominate the dependency");
    }
}
