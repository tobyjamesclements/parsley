package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyFrontier}'s single-value persistence: the frontier clock and the per-channel
 * clocks both round-trip through the one {@code "f"} key of the frontier store.
 */
class ParsleyFrontierTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    private static final Uuid ANC_ID = Uuid.randomUuid();

    /**
     * The frontier clock and the channel clocks survive a reload from the same store — a fresh
     * {@link ParsleyFrontier} over the store reproduces both the delivered frontier and completeness,
     * without replaying any records.
     */
    @Test
    void frontierClockAndChannelsRoundTripThroughTheSingleFBlob() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");

        ParsleyFrontier original = new ParsleyFrontier(store, new MockForwardedIndex());
        // Advance the contiguous frontier on T1 and record channel clocks for two inputs.
        original.deliver(T1_ID, 0, 0);
        original.deliver(T1_ID, 0, 1);
        original.channelUpdate(T1_ID, 0, ParsleyClock.empty().observe(ANC_ID, 0, 4));
        original.channelUpdate(T2_ID, 0, ParsleyClock.empty().observe(ANC_ID, 0, 7));

        ParsleyClock frontierBefore = original.snapshot();
        ParsleyClock completenessBefore = original.completeness();

        // Reload: a fresh frontier over the same store restores from the "f" blob alone.
        ParsleyFrontier restored = new ParsleyFrontier(store, new MockForwardedIndex());

        assertEquals(frontierBefore, restored.snapshot(),
                "the contiguous frontier clock must round-trip through the \"f\" blob");
        assertEquals(1L, restored.snapshot().offsetFor(T1_ID, 0),
                "T1 must restore at its delivered offset 1");
        assertEquals(completenessBefore, restored.completeness(),
                "completeness must be identical after reload — both channel clocks restored");
        assertEquals(4L, restored.completeness().offsetFor(ANC_ID, 0),
                "the shared ancestor must restore at the min across channels: min(4, 7) = 4");
    }

    // --- Epoch flooring (WS1) -------------------------------------------------------------------
    //
    // Each below floors T1 at startsAt = 100; every other coordinate is unbounded (NO_BOUND). The
    // invariant: no causal clock the frontier builds carries an entry below its coordinate's floor.

    /** T1 floored at 100; every other coordinate unbounded. */
    private static final ParsleyEpoch FLOOR_T1_AT_100 = (topicId, partition) ->
            topicId.equals(T1_ID) ? 100L : ParsleyEpoch.NO_BOUND;

    private static ParsleyFrontier flooredFrontier(ParsleyEpoch epoch) {
        return new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), true, epoch);
    }

    /**
     * A below-floor delivery feeds state but must not advance the causal frontier: delivering T1@5
     * under floor 100 leaves the frontier with no recorded T1 position (it stays at the epoch origin),
     * so completeness carries no T1 either.
     *
     * Asserts the frontier and completeness both omit T1 after a below-floor delivery.
     */
    @Test
    void belowFloorDeliveryDoesNotAdvanceTheCausalFrontier() {
        ParsleyFrontier frontier = flooredFrontier(FLOOR_T1_AT_100);

        frontier.deliver(T1_ID, 0, 5);

        assertEquals(-1L, frontier.snapshot().offsetFor(T1_ID, 0),
                "a below-floor delivery (T1@5 under floor 100) must not advance the causal frontier");
        assertEquals(-1L, frontier.completeness().offsetFor(T1_ID, 0),
                "the below-floor position must not surface in the completeness frontier");
    }

    /**
     * The first sighting of a coordinate seeds the causal frontier at the epoch origin
     * ({@code startsAt - 1}), not at the below-floor offset actually seen: seeing T1@5 under floor 100
     * establishes the origin at 99, so a later in-domain T1@100 walks contiguously 99 → 100.
     *
     * Asserts the seed lands at the origin and an in-domain delivery then advances from it.
     */
    @Test
    void seedEstablishesTheEpochOriginForANewCoordinate() {
        ParsleyFrontier frontier = flooredFrontier(FLOOR_T1_AT_100);

        assertTrue(frontier.seedIfFirstSeen(T1_ID, 0, 5),
                "the first sighting of T1 must seed the frontier");
        assertEquals(99L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the seed must land at the epoch origin (startsAt - 1 = 99), not the below-floor offset 5");

        frontier.deliver(T1_ID, 0, 100);
        assertEquals(100L, frontier.snapshot().offsetFor(T1_ID, 0),
                "an in-domain delivery (T1@100) walks contiguously from the origin 99 to 100");
        assertEquals(100L, frontier.completeness().offsetFor(T1_ID, 0),
                "the in-domain position surfaces in completeness once delivered");
    }

    /**
     * A node replaying a topic from offset 0 to rebuild state participates causally only from the
     * floor: below-floor history — even with gaps — feeds state without anchoring the frontier, and
     * the frontier only advances once an in-domain offset is delivered. This is the case that proves
     * frontier flooring is load-bearing: a below-floor gap (T1@6 missing) must not stall the in-domain
     * frontier, or a record depending on the delivered T1@100 would be held forever.
     *
     * Asserts the frontier reaches 100 despite a below-floor gap, and completeness confirms T1@100.
     */
    @Test
    void newNodeReplayingFromZeroParticipatesOnlyFromTheFloor() {
        ParsleyFrontier frontier = flooredFrontier(FLOOR_T1_AT_100);

        // Replay below-floor history with a gap at offset 6 — all out of domain.
        frontier.seedIfFirstSeen(T1_ID, 0, 5);
        frontier.deliver(T1_ID, 0, 5);
        frontier.deliver(T1_ID, 0, 7);   // gap at 6 — would stall a naive contiguous walk

        assertEquals(99L, frontier.snapshot().offsetFor(T1_ID, 0),
                "below-floor replay (with a gap) must leave the frontier at the epoch origin, not stalled below it");
        assertEquals(-1L, frontier.completeness().offsetFor(T1_ID, 0),
                "no in-domain T1 has been delivered yet, so completeness omits T1");

        // The first in-domain delivery advances the frontier from the origin, unaffected by the gap.
        frontier.deliver(T1_ID, 0, 100);
        assertEquals(100L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the below-floor gap must not stall the in-domain frontier; T1@100 walks 99 → 100");
        assertTrue(frontier.completeness().offsetFor(T1_ID, 0) >= 100L,
                "completeness must confirm the delivered in-domain T1@100");
    }

    /**
     * Completeness is floored: a channel advertising a below-floor position on T1 does not let that
     * position gate anything. With one channel advertising T1@5 (below floor 100) and another
     * advertising an in-domain T1@150, the floored min omits T1 entirely — the below-floor channel has
     * confirmed nothing in-domain.
     *
     * Asserts the below-floor advertisement is stripped so completeness carries no T1.
     */
    @Test
    void completenessIsFlooredToTheEpochBound() {
        ParsleyFrontier frontier = flooredFrontier(FLOOR_T1_AT_100);

        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty().observe(T1_ID, 0, 5));    // below floor
        frontier.channelUpdate(ANC_ID, 0, ParsleyClock.empty().observe(T1_ID, 0, 150)); // in domain

        assertEquals(-1L, frontier.completeness().offsetFor(T1_ID, 0),
                "a below-floor channel advertisement must be stripped, so completeness confirms no T1");
    }

    /**
     * With no epoch floor ({@link ParsleyEpoch#NONE}) the seed and delivery behave exactly as before:
     * a first sighting at offset 5 seeds the origin at 4 and a contiguous delivery advances normally —
     * the WS1 flooring is a no-op in epoch 0.
     *
     * Asserts the unbounded frontier reproduces the original seed-to-{@code offset - 1} behaviour.
     */
    @Test
    void withoutAnEpochFloorSeedAndDeliverAreUnchanged() {
        ParsleyFrontier frontier = flooredFrontier(ParsleyEpoch.NONE);

        assertTrue(frontier.seedIfFirstSeen(T1_ID, 0, 5),
                "the first sighting seeds the frontier even with no epoch floor");
        assertEquals(4L, frontier.snapshot().offsetFor(T1_ID, 0),
                "with no floor the seed lands at offset - 1 = 4, exactly as before epochs");

        frontier.deliver(T1_ID, 0, 5);
        assertEquals(5L, frontier.snapshot().offsetFor(T1_ID, 0),
                "a contiguous delivery advances the frontier normally under NONE");
        assertFalse(frontier.completeness().isEmpty(),
                "completeness reflects the delivered position with no flooring applied");
    }
}
