package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
        assertEquals(7L, restored.completeness().offsetFor(ANC_ID, 0),
                "the shared ancestor must restore at the max across channels: max(4, 7) = 7 — a single "
                        + "genuine witness suffices, so the higher advertised value wins, not the lower one");
    }

    /**
     * {@code pruneToScope} prunes each surviving channel's advertised clock to the scope, not just
     * the frontier entries and the channel keys. A channel's clock is monotonic (channelUpdate only
     * ever max-merges) and feeds completeness — the outbound stamp — so a transitive entry for a
     * coordinate that has left the topology (a retired topic, or one recreated under a new UUID)
     * would otherwise be re-advertised downstream forever, where every receiver's fail-closed gate
     * rejects it as an unreachable dependency: a permanent crash loop regenerated from this store on
     * every restart.
     *
     * Asserts that after pruning to a scope without the retired ancestor coordinate, completeness no
     * longer carries it — while the in-scope channel entry and frontier survive — and that the prune
     * persists (a reload sees the same pruned completeness).
     */
    @Test
    void pruneToScopeAlsoPrunesRetiredCoordinatesOutOfChannelClockValues() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyFrontier frontier = new ParsleyFrontier(store, new MockForwardedIndex());

        // T1's channel advertises transitive ancestry on ANC (an upstream topic) and on T2 (a
        // consumed sibling); ANC is then retired from the topology.
        frontier.deliver(T1_ID, 0, 0);
        frontier.deliver(T1_ID, 0, 1);
        frontier.channelUpdate(T1_ID, 0,
                ParsleyClock.empty().observe(ANC_ID, 0, 4).observe(T2_ID, 0, 2));
        assertEquals(4L, frontier.completeness().offsetFor(ANC_ID, 0),
                "precondition: the retired ancestor is advertised before the prune");

        // The new scope: T1 and T2 on partition 0 — ANC has left the topology.
        frontier.pruneToScope((topicId, partition) ->
                partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID)));

        assertEquals(-1L, frontier.completeness().offsetFor(ANC_ID, 0),
                "the retired coordinate must be pruned out of the channel clock's value, or the stamp "
                        + "would re-advertise it downstream forever");
        assertEquals(2L, frontier.completeness().offsetFor(T2_ID, 0),
                "live transitive ancestry inside the same channel clock must survive the prune");
        assertEquals(1L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the in-scope frontier entry must survive the prune");

        ParsleyFrontier reloaded = new ParsleyFrontier(store, new MockForwardedIndex());
        assertEquals(-1L, reloaded.completeness().offsetFor(ANC_ID, 0),
                "the prune must persist: a reload must not resurrect the retired coordinate");
    }

    // --- Epoch flooring (WS1) -------------------------------------------------------------------
    //
    // Each below floors T1 at startsAt = 100; every other coordinate is unbounded (NO_BOUND). The
    // invariant: no causal clock the frontier builds carries an entry below its coordinate's floor.

    /**
     * An epoch-transition window closes only when this node's OWN contiguous frontier dominates the
     * pending floor — a channel's advertised claim (hearsay from a peer's watermark) must never close
     * it, or the raised floor would strip a held e-1 record's below-floor dependencies before this
     * node had actually delivered them, releasing it out of causal order.
     */
    @Test
    void epochWindowClosesOnOwnFrontierNotOnAChannelsAdvertisedClaim() {
        ParsleyEpochState state = new ParsleyEpochState();
        ParsleyFrontier frontier =
                new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), true, state);
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());          // the node's one input channel
        frontier.recordEpochMarker(1, ParsleyClock.empty().observe(T1_ID, 0, 3), T1_ID, 0);

        // A peer's watermark claims T1@3 on the channel clock — completeness now dominates the floor,
        // but this node has delivered nothing itself.
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty().observe(T1_ID, 0, 3));
        assertFalse(frontier.tryAdvanceEpoch(),
                "a channel's advertised claim of T1@3 must not close the window — this node has not "
                        + "delivered T1@0..3 itself, so e-1 is not provably drained here");

        for (long offset = 0; offset <= 3; offset++) {
            frontier.deliver(T1_ID, 0, offset);
        }
        assertTrue(frontier.tryAdvanceEpoch(),
                "once this node's own contiguous frontier reaches the floor, the window closes");
        assertEquals(1L, state.settledEpochId(),
                "the pending epoch must be promoted to settled when the window closes");
    }

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
        assertTrue(frontier.completeness().offsetFor(T1_ID, 0) < 100L,
                "no in-domain T1 has been delivered yet, so completeness sits below the floor (at the origin)");

        // The first in-domain delivery advances the frontier from the origin, unaffected by the gap.
        frontier.deliver(T1_ID, 0, 100);
        assertEquals(100L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the below-floor gap must not stall the in-domain frontier; T1@100 walks 99 → 100");
        assertTrue(frontier.completeness().offsetFor(T1_ID, 0) >= 100L,
                "completeness must confirm the delivered in-domain T1@100");
    }

    /**
     * Completeness is the unfloored delivered frontier — the epoch does not strip it. A channel
     * advertising a below-floor position is carried through as-is; flooring a <em>dependency</em> on it
     * is the delivery gate's job (against the effective floor), not completeness's. This is the WS2
     * reversal of WS1's completeness flooring: the stamp is the plain delivered frontier so the epoch
     * transition stays invisible in the data plane.
     *
     * <p>Isolated to a single channel's advertisement (rather than two channels merged) so the below-floor
     * value is directly observable: under max-merge, a second, higher channel value would dominate the
     * result regardless of flooring, which would not distinguish "unfloored" from "floored-then-merged".
     *
     * Asserts the below-floor channel advertisement survives in completeness, unstripped.
     */
    @Test
    void completenessIsTheUnflooredDeliveredFrontier() {
        ParsleyFrontier frontier = flooredFrontier(FLOOR_T1_AT_100);

        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty().observe(T1_ID, 0, 5)); // below floor

        assertEquals(5L, frontier.completeness().offsetFor(T1_ID, 0),
                "completeness is unfloored: the below-floor T1@5 advertisement is carried through, not stripped");
    }

    /**
     * The epoch state persists inside the frontier's {@code "f"} blob: a store-backed frontier over a
     * live {@link ParsleyEpochState} that adopts a boundary reloads with the same settled floor and the
     * in-progress transition intact, so a mid-window restart resumes the transition. The frontier's
     * epoch reference is restored in place.
     */
    @Test
    void epochStateRoundTripsThroughTheFrontierBlob() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyClock.empty().observe(T1_ID, 0, 5), 1);
        ParsleyFrontier original = new ParsleyFrontier(store, new MockForwardedIndex(), epoch);

        // Adopt an epoch-2 boundary (marker on one channel); the window stays open (nothing dominates it).
        original.recordEpochMarker(2, ParsleyClock.empty().observe(T1_ID, 0, 20), T1_ID, 0);
        assertTrue(epoch.isTransitioning(), "the boundary starts a transition");

        // Reload into a fresh epoch state over the same store.
        ParsleyEpochState reloadedEpoch = new ParsleyEpochState();
        new ParsleyFrontier(store, new MockForwardedIndex(), reloadedEpoch);

        assertEquals(1L, reloadedEpoch.settledEpochId(), "the settled epoch survives the blob round-trip");
        assertEquals(5L, reloadedEpoch.startsAt(T1_ID, 0), "the effective floor stays F_{e-1}=5 mid-window after restart");
        assertTrue(reloadedEpoch.isTransitioning(), "the in-progress transition survives the restart");
        assertTrue(reloadedEpoch.hasMarker(T1_ID, 0), "the per-channel marker survives the restart");
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

    // --- bridge(): crossing consumer-skipped (EOS marker / aborted-txn) offsets --------------------
    //
    // A read_committed consumer never returns a transaction commit/abort marker or an aborted record,
    // so a transactionally-produced topic's log rec@0,1,2,MARKER@3,rec@4,5,6 reaches the consumer as
    // offsets 0,1,2,4,5,6 with a permanent hole at 3. bridge() marks such skipped offsets so the
    // contiguous walk crosses them instead of wedging at the first hole. Each helper call models one
    // receive: bridge() then, for a deliverable record, deliver().

    /** Models receive of a deliverable record: bridge the channel below it, then deliver it. */
    private static void receiveAndDeliver(ParsleyFrontier frontier, Uuid topicId, int partition, long offset) {
        frontier.bridge(topicId, partition, offset);
        frontier.deliver(topicId, partition, offset);
    }

    /**
     * The contiguous frontier tracks a transactionally-produced input across a commit-marker hole: given
     * the consumer-visible sequence 0,1,2,4,5,6 (offset 3 skipped), the frontier reaches 6, not stalling
     * at 2 the way the raw {@code +1} walk would. This is the frontier-level analogue of the EOS
     * frontier-density integration repro.
     */
    @Test
    void bridgeCrossesAConsumerSkippedOffsetSoTheContiguousWalkReachesTheEnd() {
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex());

        for (long offset : new long[] {0, 1, 2, 4, 5, 6}) {   // 3 is a commit marker the consumer skips
            receiveAndDeliver(frontier, T1_ID, 0, offset);
        }

        assertEquals(6L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the frontier must advance past the skipped marker offset 3 to cover all delivered "
                        + "records (0,1,2,4,5,6); a stall at 2 is the density bug");
    }

    /**
     * bridge() returns {@code true} exactly when it advanced the contiguous frontier — the signal the
     * engine uses to decide whether to cascade. Crossing a marker that unblocks the walk returns true; a
     * first sighting and an at-least-once replay both return false.
     */
    @Test
    void bridgeReturnsTrueOnlyWhenItAdvancesTheFrontier() {
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex());

        assertFalse(frontier.bridge(T1_ID, 0, 0),
                "the first sighting of a channel bridges nothing (its baseline is the seed's concern)");
        frontier.deliver(T1_ID, 0, 0);
        receiveAndDeliver(frontier, T1_ID, 0, 1);
        receiveAndDeliver(frontier, T1_ID, 0, 2);

        assertTrue(frontier.bridge(T1_ID, 0, 4),
                "bridging the marker at 3 (frontier at 2) advances the frontier to 3 — the walk crosses "
                        + "the hole — so it must return true to trigger a cascade");
        assertEquals(3L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the frontier advances to the marker offset itself once crossed; the real record at 4 is "
                        + "delivered separately");
        assertFalse(frontier.bridge(T1_ID, 0, 4),
                "a repeat bridge at an already-received offset is an at-least-once replay: a no-op");
    }

    /**
     * bridge() never advances the frontier past a still-held business record: the walk's contiguity is
     * the protecting invariant. With offset 2 received but not yet delivered (held), bridging a later
     * marker at 3 marks the marker but the frontier stays at 1 — 2 blocks the walk. Only once 2 is
     * delivered does the frontier absorb 2 and the bridged 3 together.
     */
    @Test
    void bridgeDoesNotAdvancePastAHeldBusinessRecord() {
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex());

        receiveAndDeliver(frontier, T1_ID, 0, 0);
        receiveAndDeliver(frontier, T1_ID, 0, 1);
        // Record at 2 is received (bridge records it) but HELD — deliver() is not called for it.
        frontier.bridge(T1_ID, 0, 2);
        // Record at 4 arrives; offset 3 between the held 2 and 4 is a marker.
        assertFalse(frontier.bridge(T1_ID, 0, 4),
                "bridging 3 must not advance the frontier while 2 is still held");
        assertEquals(1L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the held record at 2 blocks the walk; the frontier stays at 1 despite the bridged marker");

        // Once the held 2 is delivered, the walk absorbs 2 and the previously-bridged 3 in one run.
        frontier.deliver(T1_ID, 0, 2);
        assertEquals(3L, frontier.snapshot().offsetFor(T1_ID, 0),
                "delivering the held 2 lets the walk absorb 2 and the bridged marker 3 together");
    }

    /**
     * The bridge fast path folds a large consumer-skipped run (e.g. a big aborted transaction) straight
     * into the frontier without marking each skipped offset in the forwarded index — so a huge gap costs
     * O(1) index work, not O(gap), which could otherwise blow the EOS transaction. It applies whenever the
     * frontier is caught up to the previous highest received (no held record sits in the run), so the whole
     * run is contiguous markers.
     */
    @Test
    void bridgeFastPathFoldsALargeSkippedRunWithoutMarkingEachOffset() {
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), forwardedIndex);

        receiveAndDeliver(frontier, T1_ID, 0, 0);          // frontier and highest-received both at 0

        // A record at 1_000_000 with the entire (0, 1_000_000) run consumer-skipped (a large aborted txn).
        assertTrue(frontier.bridge(T1_ID, 0, 1_000_000L),
                "bridging a large skipped run must advance the frontier");
        assertEquals(999_999L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the whole skipped run folds into the frontier, up to just below the received offset");
        assertTrue(forwardedIndex.forwardedAfter(T1_ID, 0, -1L).isEmpty(),
                "the fast path must mark none of the skipped offsets in the forwarded index — O(1), not O(gap)");
    }

    /**
     * The per-channel highest-received offset persists in the {@code "f"} blob, so bridge()'s skip
     * detection is exact across a restart: after reloading a frontier that had received up to offset 4,
     * a record arriving at 6 (offset 5 a marker) is correctly bridged rather than misread as a first
     * sighting. Without persisting the highest-received offset the reloaded frontier would treat 6 as a
     * first sighting, bridge nothing, and stall at 4.
     */
    @Test
    void highestReceivedPersistsSoBridgeStaysExactAcrossRestart() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyFrontier original = new ParsleyFrontier(store, new MockForwardedIndex());
        for (long offset : new long[] {0, 1, 2, 4}) {   // 3 skipped; highest received becomes 4
            receiveAndDeliver(original, T1_ID, 0, offset);
        }
        assertEquals(4L, original.snapshot().offsetFor(T1_ID, 0), "precondition: frontier reached 4 before restart");

        // Reload from the same store: the highest-received map restores from the "f" blob alone.
        ParsleyFrontier restored = new ParsleyFrontier(store, new MockForwardedIndex());

        // A record at 6 arrives (offset 5 a marker). Because highest-received restored as 4, the gap at
        // 5 is recognised and bridged.
        assertTrue(restored.bridge(T1_ID, 0, 6),
                "the restored highest-received (4) lets bridge recognise the marker gap at 5 and advance");
        assertEquals(5L, restored.snapshot().offsetFor(T1_ID, 0),
                "the frontier crosses the bridged marker 5; a first-sighting misread would have stalled at 4");
        restored.deliver(T1_ID, 0, 6);
        assertEquals(6L, restored.snapshot().offsetFor(T1_ID, 0),
                "delivering the real record at 6 then advances the frontier to 6");
    }

    // --- Cross-store tear regression (BACKLOG.md: torn changelog flush under at-least-once) --------
    //
    // The forwarded index and the frontier's "f" blob are two separate changelog-backed stores with
    // no cross-store atomicity. deliver() must persist the new frontier value before pruning the
    // forwarded-index entries it absorbed, so a crash between the two writes always tears toward "a
    // redundant forwarded-index entry lingers below an already-advanced frontier" (harmless — see the
    // BACKLOG.md LOW item on replayed already-delivered offsets) rather than "the frontier advance is
    // lost after the forwarded-index entry backing it is already gone" (a permanent wedge).

    /**
     * Proves the ordering directly: a {@link ParsleyForwardedIndex} that, at the exact moment {@code
     * unmark()} is called, opens a second {@link ParsleyFrontier} over the same store must already see
     * the new frontier value — i.e. {@code persist()} has already committed by the time any absorbed
     * entry is pruned.
     */
    @Test
    void deliverPersistsTheNewFrontierValueBeforePruningTheEntriesItAbsorbed() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        List<Long> persistedOffsetAtUnmarkTime = new ArrayList<>();
        MockForwardedIndex delegate = new MockForwardedIndex();
        ParsleyForwardedIndex recordingIndex = new ParsleyForwardedIndex() {
            @Override
            public void mark(Uuid topicId, int partition, long offset) {
                delegate.mark(topicId, partition, offset);
            }

            @Override
            public List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset) {
                return delegate.forwardedAfter(topicId, partition, frontierOffset);
            }

            @Override
            public void unmark(Uuid topicId, int partition, long offset) {
                // A fresh frontier over the same store sees only what has actually been written —
                // not this call's own in-progress in-memory state.
                ParsleyFrontier persistedView = new ParsleyFrontier(store, new MockForwardedIndex());
                persistedOffsetAtUnmarkTime.add(persistedView.snapshot().offsetFor(T1_ID, 0));
                delegate.unmark(topicId, partition, offset);
            }

            @Override
            public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
                delegate.pruneAtOrBelow(topicId, partition, watermark);
            }
        };

        ParsleyFrontier frontier = new ParsleyFrontier(store, recordingIndex);
        frontier.deliver(T1_ID, 0, 0);

        assertEquals(List.of(0L), persistedOffsetAtUnmarkTime,
                "by the time the absorbed entry is pruned, the frontier store must already durably "
                        + "reflect the new value — persist() must run before unmark()");
    }

    /**
     * Regression test for the wedge this ordering prevents. A crash landing between {@code persist()}
     * and {@code unmark()} is simulated by a forwarded index that swallows one specific {@code
     * unmark()} call, standing in for "that changelog write never landed". The surviving state must be
     * only a harmless stale forwarded-index entry sitting below the (already-advanced) frontier — never
     * a stall, and never a reversal that would leave the frontier unable to cross this offset again.
     */
    @Test
    void aCrashBetweenPersistAndUnmarkLeavesOnlyAHarmlessStaleEntryNeverAWedge() {
        MockForwardedIndex delegate = new MockForwardedIndex();
        ParsleyForwardedIndex crashyIndex = new ParsleyForwardedIndex() {
            @Override
            public void mark(Uuid topicId, int partition, long offset) {
                delegate.mark(topicId, partition, offset);
            }

            @Override
            public List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset) {
                return delegate.forwardedAfter(topicId, partition, frontierOffset);
            }

            @Override
            public void unmark(Uuid topicId, int partition, long offset) {
                if (offset == 0) return; // the changelog write for this one unmark never lands
                delegate.unmark(topicId, partition, offset);
            }

            @Override
            public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
                delegate.pruneAtOrBelow(topicId, partition, watermark);
            }
        };
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), crashyIndex);

        frontier.deliver(T1_ID, 0, 0);

        assertEquals(0L, frontier.snapshot().offsetFor(T1_ID, 0),
                "the frontier already advanced to 0 — persist() ran before the (lost) unmark");
        assertEquals(List.of(0L), delegate.forwardedAfter(T1_ID, 0, -1),
                "the now-redundant entry for the already-absorbed offset lingers, harmlessly, in the "
                        + "forwarded index");

        // A later delivery must proceed normally despite the leaked entry sitting below the frontier —
        // this is the crux of the regression: the tear must never strand the coordinate.
        frontier.deliver(T1_ID, 0, 1);

        assertEquals(1L, frontier.snapshot().offsetFor(T1_ID, 0),
                "a subsequent delivery must advance normally, unaffected by the leaked stale entry "
                        + "below the frontier");
    }

    /**
     * Regression test for BACKLOG.md's LOW item: an at-least-once replay of an already-delivered offset
     * ({@code deliver(C, k)} with {@code frontier(C) >= k}) must never mark it in the forwarded index.
     * The absorb walk only ever scans strictly above the watermark, so a mark at or below it could never
     * be found and unmarked again — it would otherwise leak in the changelog-backed store forever, purely
     * cosmetic growth with no effect on gating.
     *
     * Asserts a replay at or below the current watermark leaves the frontier unchanged and marks nothing
     * in the forwarded index.
     */
    @Test
    void replayingAnAlreadyDeliveredOffsetLeavesNoForwardedIndexEntry() {
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), forwardedIndex);

        frontier.deliver(T1_ID, 0, 0);
        frontier.deliver(T1_ID, 0, 1);
        assertEquals(1L, frontier.snapshot().offsetFor(T1_ID, 0), "frontier advances normally through 0, 1");

        // Replay: offset 0 (below the watermark) and offset 1 (exactly at it) redelivered.
        frontier.deliver(T1_ID, 0, 0);
        frontier.deliver(T1_ID, 0, 1);

        assertEquals(1L, frontier.snapshot().offsetFor(T1_ID, 0),
                "a replay of an already-delivered offset must not move the frontier");
        assertTrue(forwardedIndex.forwardedAfter(T1_ID, 0, -1).isEmpty(),
                "a replayed already-delivered offset must never be marked in the forwarded index — the "
                        + "absorb walk only scans strictly above the watermark, so it could never be "
                        + "found and unmarked again");
    }

    /**
     * Regression test for BACKLOG.md's LOW item: a stale below-watermark forwarded-index entry — left
     * over from the acknowledged benign tear direction {@link #deliver}'s Javadoc describes (frontier
     * persisted, unmark lost) — is never reachable by the absorb walk again (it only scans strictly
     * above the watermark), so it would otherwise linger in the changelog-backed store forever. A fresh
     * {@link ParsleyFrontier} over a restored store must sweep it away once, at load.
     *
     * <p>Mirrors {@link #aCrashBetweenPersistAndUnmarkLeavesOnlyAHarmlessStaleEntryNeverAWedge}'s setup
     * (a forwarded index that swallows one {@code unmark} call) but then reloads a second frontier over
     * the same durable store, asserting the leaked entry is gone afterwards.
     *
     * Asserts the stale entry lingers before restore and is swept away by the restoring constructor.
     */
    @Test
    void restoringADurableFrontierSweepsStaleBelowWatermarkForwardedIndexEntries() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        MockForwardedIndex delegate = new MockForwardedIndex();
        ParsleyForwardedIndex crashyIndex = new ParsleyForwardedIndex() {
            @Override
            public void mark(Uuid topicId, int partition, long offset) {
                delegate.mark(topicId, partition, offset);
            }

            @Override
            public List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset) {
                return delegate.forwardedAfter(topicId, partition, frontierOffset);
            }

            @Override
            public void unmark(Uuid topicId, int partition, long offset) {
                if (offset == 0) return; // the changelog write for this one unmark never lands
                delegate.unmark(topicId, partition, offset);
            }

            @Override
            public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
                delegate.pruneAtOrBelow(topicId, partition, watermark);
            }
        };

        ParsleyFrontier original = new ParsleyFrontier(store, crashyIndex);
        original.deliver(T1_ID, 0, 0); // offset 0 absorbed, but its unmark is lost — leaks below the watermark
        original.deliver(T1_ID, 0, 1); // watermark advances to 1; the offset-0 entry is now stale

        assertEquals(List.of(0L), delegate.forwardedAfter(T1_ID, 0, -1),
                "the stale entry must still be sitting in the forwarded index before any restore");

        // Restore: a fresh frontier over the same durable store must sweep the stale entry away.
        new ParsleyFrontier(store, delegate);

        assertTrue(delegate.forwardedAfter(T1_ID, 0, -1).isEmpty(),
                "restoring the frontier must sweep the stale below-watermark entry left by the crash");
    }
}
