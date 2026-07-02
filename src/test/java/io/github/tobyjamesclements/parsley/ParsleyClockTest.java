package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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
        ParsleyClock.CoordinatePredicate inScope = (topicId, partition) -> topicId.equals(T1_ID) && partition == 0;
        ParsleyClock deps = ParsleyClock.empty()
                .observe(T1_ID, 0, 3).observe(T1_ID, 1, 8).observe(T2_ID, 0, 5);

        ParsleyClock scoped = deps.retaining(inScope);

        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 3), scoped,
                "retaining must keep only the in-scope coordinate and drop the others");
        assertFalse(ParsleyClock.empty().dominates(deps),
                "the unfiltered dependencies are not satisfied by an empty frontier");
        assertFalse(ParsleyClock.empty().dominates(scoped),
                "the in-scope coordinate still requires offset 3 from the frontier");
        assertTrue(ParsleyClock.empty().observe(T1_ID, 0, 3).dominates(scoped),
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
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 5);
        assertSame(deps, deps.retaining((topicId, partition) -> true),
                "retaining must not allocate when every coordinate is kept");
    }

    /**
     * {@code strippedBelow} drops a dependency below its coordinate's {@code startsAt} bound (an
     * out-of-domain reference to a prior epoch) while keeping a dependency at or above the bound, and
     * keeps a coordinate with no bound entirely ({@link Long#MAX_VALUE}).
     *
     * Asserts the stripped clock keeps only the at/above-bound and unbounded coordinates, and that an
     * empty frontier then dominates it despite not dominating the originals.
     */
    @Test
    void strippedBelowDropsDependenciesBelowTheStartsAtBound() {
        // T1: bound 10 (T1@3 below → stripped; would need offset 3 otherwise). T2: no bound (kept).
        ParsleyEpoch.View bound = (topicId, partition) ->
                topicId.equals(T1_ID) && partition == 0 ? 10L : ParsleyEpoch.NO_BOUND;
        ParsleyClock deps = ParsleyClock.empty()
                .observe(T1_ID, 0, 3).observe(T2_ID, 0, 5);

        ParsleyClock stripped = deps.strippedBelow(bound);

        assertEquals(ParsleyClock.empty().observe(T2_ID, 0, 5), stripped,
                "T1@3 (below startsAt 10) must be stripped; T2@5 (unbounded) must be kept");
        assertFalse(ParsleyClock.empty().dominates(deps),
                "the unstripped dependencies are not satisfied by an empty frontier");
        assertTrue(ParsleyClock.empty().observe(T2_ID, 0, 5).dominates(stripped),
                "once the below-bound dependency is stripped, only the surviving one gates");
    }

    /**
     * A dependency exactly at its coordinate's {@code startsAt} bound is kept — the bound is the
     * lowest offset that still participates in the epoch, not the first excluded one.
     *
     * Asserts a dependency at the bound survives stripping.
     */
    @Test
    void strippedBelowKeepsADependencyExactlyAtTheBound() {
        ParsleyEpoch.View bound = (topicId, partition) -> 10L;
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 10);
        assertEquals(deps, deps.strippedBelow(bound),
                "a dependency exactly at startsAt is inside the domain and must be kept");
    }

    /**
     * {@code strippedBelow} returns the same instance — not a copy — when nothing is below its bound,
     * so the common no-strip case (including the disabled {@link ParsleyEpoch.View#NONE}) allocates
     * nothing on the gating path.
     *
     * Asserts the returned clock is reference-identical to the original under NONE.
     */
    @Test
    void strippedBelowReturnsSameInstanceWhenNothingStripped() {
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 5);
        assertSame(deps, deps.strippedBelow(ParsleyEpoch.View.NONE),
                "strippedBelow must not allocate when every coordinate is at or above its bound");
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

    /**
     * When both clocks record the same (topicId, partition) coordinate, mergeMin takes the lower of
     * the two offsets, bounding the result at the slowest of the two clocks.
     *
     * Asserts that the result clock contains the minimum offset for each coordinate present in both.
     */
    @Test
    void mergeMinBothPresentTakesMin() {
        ParsleyClock a = ParsleyClock.empty().observe(T1_ID, 0, 7).observe(T2_ID, 0, 3);
        ParsleyClock b = ParsleyClock.empty().observe(T1_ID, 0, 2).observe(T2_ID, 0, 9);
        assertEquals(
                ParsleyClock.empty().observe(T1_ID, 0, 2).observe(T2_ID, 0, 3),
                a.mergeMin(b),
                "mergeMin must take the minimum offset for each coordinate present in both clocks");
    }

    /**
     * A coordinate present in only one clock is kept at its value: unknown is not zero. A channel that
     * has never mentioned a coordinate does not pin it down.
     *
     * Asserts that a coordinate unique to either side appears unchanged in the result.
     */
    @Test
    void mergeMinPresentOnOneSideKept() {
        ParsleyClock a = ParsleyClock.empty().observe(T1_ID, 0, 5);
        ParsleyClock b = ParsleyClock.empty().observe(T2_ID, 0, 3);
        assertEquals(
                ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T2_ID, 0, 3),
                a.mergeMin(b),
                "mergeMin must keep a coordinate present in only one clock at its original value");
    }

    /**
     * mergeMin with an empty clock is an identity in both directions: merging any clock with empty
     * returns a clock equal to the non-empty one.
     *
     * Asserts that mergeMin(a, empty) and mergeMin(empty, a) both equal a.
     */
    @Test
    void mergeMinEmptyIsIdentity() {
        ParsleyClock a = ParsleyClock.empty().observe(T1_ID, 0, 4).observe(T2_ID, 0, 1);
        assertEquals(a, a.mergeMin(ParsleyClock.empty()),
                "mergeMin with an empty clock on the right must return the original clock");
        assertEquals(a, ParsleyClock.empty().mergeMin(a),
                "mergeMin with an empty clock on the left must return the original clock");
    }

    /**
     * mergeMin is commutative: merging a with b produces the same result as merging b with a.
     *
     * Asserts that mergeMin(a, b) equals mergeMin(b, a) for clocks with both shared and unique
     * coordinates.
     */
    @Test
    void mergeMinIsCommutative() {
        ParsleyClock a = ParsleyClock.empty().observe(T1_ID, 0, 7).observe(T2_ID, 0, 1);
        ParsleyClock b = ParsleyClock.empty().observe(T1_ID, 0, 3).observe(T2_ID, 0, 8);
        assertEquals(a.mergeMin(b), b.mergeMin(a),
                "mergeMin must be commutative: the order of arguments must not affect the result");
    }
}
