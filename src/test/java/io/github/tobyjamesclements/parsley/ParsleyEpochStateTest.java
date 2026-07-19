package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyEpochState}: the settled floor it exposes as a {@link ParsleyEpoch}, and the
 * overlapping-epoch transition (marker receipt per channel, promote-on-close, and blob round-trip).
 */
class ParsleyEpochStateTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * A fresh state is epoch 0: every coordinate is unbounded ({@link ParsleyEpoch#NO_BOUND}), so the
     * strip and floor are no-ops until a boundary is adopted.
     */
    @Test
    void freshStateIsEpochZeroUnbounded() {
        ParsleyEpochState state = new ParsleyEpochState();
        assertEquals(0L, state.settledEpochId(), "a fresh state is epoch 0");
        assertEquals(ParsleyEpoch.NO_BOUND, state.startsAt(T1_ID, 0), "epoch 0 leaves every coordinate unbounded");
        assertFalse(state.isTransitioning(), "a fresh state has no transition in progress");
    }

    /**
     * {@code startsAt} reads the settled floor: a coordinate with a recorded bound returns it, one
     * without returns {@link ParsleyEpoch#NO_BOUND}.
     */
    @Test
    void startsAtReadsTheSettledFloorPerCoordinate() {
        ParsleyEpochState state = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 100), 1);
        assertEquals(100L, state.startsAt(T1_ID, 0), "T1 is floored at its settled startsAt");
        assertEquals(ParsleyEpoch.NO_BOUND, state.startsAt(T2_ID, 0), "an unrecorded coordinate is unbounded");
    }

    /**
     * While a transition is in progress the effective floor stays the <em>old</em> settled floor
     * {@code F_{e-1}} — the pending floor {@code F_e} does not take effect until the window closes. This
     * is what lets in-flight epoch-(e-1) records keep being gated against their own epoch's floor.
     */
    @Test
    void effectiveFloorStaysTheOldFloorWhilePending() {
        ParsleyEpochState state = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 5), 1);

        state.onBoundary(2, ParsleyVectorClock.empty().observe(T1_ID, 0, 20), T1_ID, 0);

        assertTrue(state.isTransitioning(), "receiving a boundary starts a transition");
        assertEquals(5L, state.startsAt(T1_ID, 0),
                "the effective floor stays F_{e-1}=5 while the window is open, not the pending F_e=20");
        assertEquals(1L, state.settledEpochId(), "the settled epoch does not advance until the window closes");
    }

    /**
     * A boundary for an epoch at or below the settled epoch is a stale/duplicate re-delivery and is
     * ignored — the floor never regresses.
     */
    @Test
    void aStaleBoundaryAtOrBelowTheSettledEpochIsIgnored() {
        ParsleyEpochState state = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 20), 2);

        boolean began = state.onBoundary(2, ParsleyVectorClock.empty().observe(T1_ID, 0, 5), T1_ID, 0);

        assertFalse(began, "a boundary for the already-settled epoch must not begin a transition");
        assertFalse(state.isTransitioning(), "no transition starts for a stale epoch");
        assertEquals(20L, state.startsAt(T1_ID, 0), "the settled floor must not regress");
    }

    /**
     * The window closes only by {@link ParsleyEpochState#promote}, which the caller invokes once the
     * marker is on every channel and the delivered frontier dominates {@code F_e}. Promotion makes
     * {@code F_e} the settled floor and ends the transition.
     */
    @Test
    void promoteAdvancesTheSettledFloorAndEndsTheTransition() {
        ParsleyEpochState state = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 5), 1);
        state.onBoundary(2, ParsleyVectorClock.empty().observe(T1_ID, 0, 20), T1_ID, 0);

        state.promote();

        assertFalse(state.isTransitioning(), "promotion ends the transition");
        assertEquals(2L, state.settledEpochId(), "the settled epoch advances to the pending one");
        assertEquals(20L, state.startsAt(T1_ID, 0), "the pending floor F_e=20 is now the effective floor");
    }

    /**
     * Marker receipt is tracked per input channel, so a caller can tell when the boundary has arrived on
     * every channel (the Chandy-Lamport window-engaged condition).
     */
    @Test
    void markerReceiptIsTrackedPerChannel() {
        ParsleyEpochState state = new ParsleyEpochState();
        ParsleyVectorClock floor = ParsleyVectorClock.empty().observe(T1_ID, 0, 20);

        state.onBoundary(1, floor, T1_ID, 0);

        assertTrue(state.hasMarker(T1_ID, 0), "the channel the marker arrived on is recorded");
        assertFalse(state.hasMarker(T2_ID, 0), "a channel with no marker yet is not recorded");

        state.onBoundary(1, floor, T2_ID, 0);
        assertTrue(state.hasMarker(T2_ID, 0), "a second channel's marker is recorded under the same epoch");
    }

    /**
     * {@code onBoundary} reports whether the marker was <em>newly recorded</em> — the signal the relaying
     * processor gates its downstream boundary relay on (fixing B1: relay on first sight of the boundary
     * even when its carried completeness taught the channel nothing new). It is true when a marker begins
     * a transition and true again for each new channel under that transition, but false for a duplicate
     * on a channel already seen — so a cyclic topology relays each boundary exactly once per channel and
     * never ping-pongs it.
     */
    @Test
    void onBoundaryReportsWhetherTheMarkerWasNewlyRecorded() {
        ParsleyEpochState state = new ParsleyEpochState();
        ParsleyVectorClock floor = ParsleyVectorClock.empty().observe(T1_ID, 0, 20);

        assertTrue(state.onBoundary(1, floor, T1_ID, 0),
                "the first marker begins the transition and is newly recorded");
        assertFalse(state.onBoundary(1, floor, T1_ID, 0),
                "a duplicate on the same channel records nothing new — the cycle-safety guarantee");
        assertTrue(state.onBoundary(1, floor, T2_ID, 0),
                "a marker on a second channel under the same epoch is newly recorded");
    }

    /**
     * The state round-trips through its {@link ParsleyEpochState#toBytes serialised} form, including an
     * in-progress transition with its pending floor and per-channel markers — so a mid-window restart
     * resumes the transition rather than losing it.
     */
    @Test
    void roundTripsThroughSerialisationIncludingAnInProgressTransition() {
        ParsleyEpochState original = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 5), 3);
        original.onBoundary(4, ParsleyVectorClock.empty().observe(T1_ID, 0, 20).observe(T2_ID, 0, 7), T1_ID, 0);

        ParsleyEpochState restored = new ParsleyEpochState();
        restored.restore(original.toBytes());

        assertEquals(3L, restored.settledEpochId(), "the settled epoch survives the round-trip");
        assertEquals(5L, restored.startsAt(T1_ID, 0), "the settled floor survives (effective floor still F_{e-1})");
        assertTrue(restored.isTransitioning(), "the in-progress transition survives");
        assertTrue(restored.hasMarker(T1_ID, 0), "the per-channel marker survives");
        assertFalse(restored.hasMarker(T2_ID, 0), "a channel without a marker stays without one");

        restored.promote();
        assertEquals(20L, restored.startsAt(T1_ID, 0), "the restored pending floor promotes correctly to F_e=20");
        assertEquals(4L, restored.settledEpochId(), "the restored pending epoch id promotes correctly");
    }
}
