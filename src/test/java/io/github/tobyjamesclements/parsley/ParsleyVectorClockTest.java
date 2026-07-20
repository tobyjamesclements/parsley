package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link ParsleyVectorClock} vector clock — the one type serving as both the node frontier and a
 * record's dependencies. {@link #dominates} is the satisfaction check; {@link #missing} the causal gap.
 */
class ParsleyVectorClockTest {

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
        assertTrue(ParsleyVectorClock.empty().dominates(ParsleyVectorClock.empty()),
                "an empty frontier must dominate empty dependencies");
        assertTrue(ParsleyVectorClock.empty().observe(T1_ID, 0, 7).dominates(ParsleyVectorClock.empty()),
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
        ParsleyVectorClock required = ParsleyVectorClock.empty().observe(T1_ID, 0, 3);
        assertFalse(ParsleyVectorClock.empty().dominates(required),
                "empty frontier cannot dominate a non-empty dependency");
        assertFalse(ParsleyVectorClock.empty().observe(T1_ID, 0, 2).dominates(required),
                "frontier at offset 2 does not dominate a requirement of offset 3");
        assertTrue(ParsleyVectorClock.empty().observe(T1_ID, 0, 3).dominates(required),
                "frontier at exactly the required offset must dominate");
        assertTrue(ParsleyVectorClock.empty().observe(T1_ID, 0, 4).dominates(required),
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
        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 5),
                ParsleyVectorClock.empty().observe(T1_ID, 0, 5).observe(T1_ID, 0, 2),
                "observe must retain only the maximum offset per (topicId, partition)");
    }

    /**
     * Merging two clocks produces a new clock that takes the per-partition maximum offset from each.
     *
     * Asserts that the merged clock holds the higher offset for each (topicId, partition).
     */
    @Test
    void mergeTakesPerPartitionMaximum() {
        ParsleyVectorClock a = ParsleyVectorClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 1);
        ParsleyVectorClock b = ParsleyVectorClock.empty().observe(T1_ID, 0, 1).observe(T2_ID, 0, 9);
        assertEquals(
                ParsleyVectorClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 9),
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
        ParsleyVectorClock clock = ParsleyVectorClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 9);
        assertEquals(ParsleyVectorClock.empty().observe(T2_ID, 0, 9), clock.without(T1_ID, 0),
                "without must drop only the named coordinate");
        assertEquals(clock, clock.without(T1_ID, 1),
                "without must be a no-op for a coordinate not present");
    }

    /**
     * A clock round-trips through its binary wire format.
     *
     * Asserts that deserialising the serialised form produces an equal clock.
     */
    @Test
    void serialisationRoundTrips() {
        ParsleyVectorClock clock = ParsleyVectorClock.empty().observe(T1_ID, 0, 5).observe(T2_ID, 0, 2);
        assertEquals(clock, ParsleyVectorClock.fromBytes(clock.toBytes()),
                "clock must round-trip through binary serialisation");
    }

    /**
     * {@code retaining} drops every coordinate the predicate rejects — both a whole topic outside
     * scope and an out-of-scope partition of an in-scope topic — and keeps the rest. The dropped
     * coordinates then no longer count against {@code dominates}, so they are vacuously satisfied.
     *
     * Asserts the filtered clock holds only the in-scope coordinate, and that an empty frontier
     * dominates the filtered dependencies even though it did not dominate the originals.
     */
    @Test
    void retainingDropsOutOfScopeCoordinates() {
        // In scope: topic T1 on partition 0 only. T1-1 (wrong partition) and T2-0 (wrong topic) are out.
        ParsleyVectorClock.CoordinatePredicate inScope = (topicId, partition) -> topicId.equals(T1_ID) && partition == 0;
        ParsleyVectorClock deps = ParsleyVectorClock.empty()
                .observe(T1_ID, 0, 3).observe(T1_ID, 1, 8).observe(T2_ID, 0, 5);

        ParsleyVectorClock scoped = deps.retaining(inScope);

        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 3), scoped,
                "retaining must keep only the in-scope coordinate and drop the others");
        assertFalse(ParsleyVectorClock.empty().dominates(deps),
                "the unfiltered dependencies are not satisfied by an empty frontier");
        assertFalse(ParsleyVectorClock.empty().dominates(scoped),
                "the in-scope coordinate still requires offset 3 from the frontier");
        assertTrue(ParsleyVectorClock.empty().observe(T1_ID, 0, 3).dominates(scoped),
                "once out-of-scope coordinates are dropped, only the in-scope requirement gates");
    }

    /**
     * {@code retaining} returns the same instance — not a copy — when every coordinate is in scope,
     * so the common all-in-scope case allocates nothing on the gating path.
     *
     * Asserts the returned clock is reference-identical to the original.
     */
    @Test
    void retainingReturnsSameInstanceWhenNothingDropped() {
        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 5);
        assertSame(deps, deps.retaining((topicId, partition) -> true),
                "retaining must not allocate when every coordinate is kept");
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
        ParsleyVectorClock required = ParsleyVectorClock.empty().observe(oldUuid, 0, 5L);

        assertFalse(ParsleyVectorClock.empty().observe(newUuid, 0, 5L).dominates(required),
                "new-UUID frontier must not dominate an old-UUID dependency");
        assertTrue(ParsleyVectorClock.empty().observe(oldUuid, 0, 5L).dominates(required),
                "old-UUID frontier at matching offset must dominate the dependency");
    }
}
