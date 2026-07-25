package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyChannels}'s single-value persistence: the frontier clock and the per-channel
 * clocks both round-trip through the one {@code "frontier"} key of the frontier store.
 */
class ParsleyChannelsTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid ANC_ID = Uuid.randomUuid();

    /**
     * The frontier clock and the channel clocks survive a reload from the same store — a fresh
     * {@link ParsleyChannels} over the store reproduces both the delivered frontier and completeness,
     * without replaying any records.
     */
    @Test
    void frontierClockAndChannelsRoundTripThroughTheSingleFBlob() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");

        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        // Advance the contiguous frontier on C1 and record channel clocks for two inputs.
        original.delivered(C1_ID, 0, 0);
        original.delivered(C1_ID, 0, 1);
        original.channelUpdate(C1_ID, 0, ParsleyVectorClock.empty().observe(ANC_ID, 0, 4));
        original.channelUpdate(C2_ID, 0, ParsleyVectorClock.empty().observe(ANC_ID, 0, 7));

        ParsleyVectorClock frontierBefore = original.frontier();
        ParsleyVectorClock completenessBefore = original.completeness();

        // Reload: a fresh frontier over the same store restores from the frontier value alone.
        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());

        assertEquals(frontierBefore, restored.frontier(),
                "the contiguous frontier clock must round-trip through the \"f\" blob");
        assertEquals(1L, restored.frontier().offsetFor(C1_ID, 0),
                "C1 must restore at its delivered offset 1");
        assertEquals(completenessBefore, restored.completeness(),
                "completeness must be identical after reload — both channel clocks restored");
        assertEquals(7L, restored.completeness().offsetFor(ANC_ID, 0),
                "the shared ancestor must restore at the max across channels: max(4, 7) = 7 — a single "
                        + "genuine witness suffices, so the higher advertised value wins, not the lower one");
    }

    /**
     * A record delivered out of order <em>above</em> a contiguous-frontier gap (non-head-of-line
     * delivery: a later offset forwards while an earlier one is still held) must be claimed by the
     * outbound stamp even though the frontier cannot reach it: an output emitted from that delivery
     * is causally after it, and a downstream consumer of both topics gates only on what the stamp
     * claims. The frontier (the gate's view) and completeness (the interim floor-publication view)
     * must both stay below the gap — only {@code stamp()} carries the above-gap claim, exactly the
     * split T2.3 established for {@code ownOutputs}.
     *
     * Asserts the stamp claims the above-gap offset while frontier and completeness stay at the
     * contiguous prefix.
     */
    @Test
    void stampClaimsADeliveryAboveTheContiguousFrontierGap() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());

        channels.receive(C1_ID, 0, 0);
        channels.delivered(C1_ID, 0, 0);
        channels.receive(C1_ID, 0, 1);
        channels.delivered(C1_ID, 0, 1);
        channels.receive(C1_ID, 0, 2); // received but held — a genuine gap, not a consumer skip
        channels.receive(C1_ID, 0, 3);
        channels.delivered(C1_ID, 0, 3); // delivered above the gap at offset 2

        assertEquals(1L, channels.frontier().offsetFor(C1_ID, 0),
                "the contiguous frontier must not advance past the held offset 2");
        assertEquals(1L, channels.completeness().offsetFor(C1_ID, 0),
                "completeness (the floor-publication view) must not claim above the gap either");
        assertEquals(3L, channels.stamp().offsetFor(C1_ID, 0),
                "the outbound stamp must claim the record delivered above the gap — its coordinate "
                        + "is real delivered causal past, and omitting it lets a downstream consumer "
                        + "deliver a derived output before this cause");
    }

    /**
     * {@code highestDelivered} is deliberately not persisted: an above-gap delivered offset is
     * exactly a forwarded-index mark, committed in the same EOS transaction as the {@code "frontier"}
     * value, so a fresh {@link ParsleyChannels} over the same stores must reconstruct the stamp's
     * above-gap claim from the index alone.
     *
     * Asserts the restored instance's stamp still claims the above-gap offset while its frontier
     * stays at the contiguous prefix.
     */
    @Test
    void stampsAboveGapClaimIsReconstructedFromTheForwardedIndexOnRestore() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyChannels original = new ParsleyChannels(store, forwardedIndex);
        original.receive(C1_ID, 0, 0);
        original.delivered(C1_ID, 0, 0);
        original.receive(C1_ID, 0, 1); // received but held
        original.receive(C1_ID, 0, 2);
        original.delivered(C1_ID, 0, 2); // delivered above the gap at offset 1

        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);

        assertEquals(0L, restored.frontier().offsetFor(C1_ID, 0),
                "the restored frontier must still sit below the gap");
        assertEquals(2L, restored.stamp().offsetFor(C1_ID, 0),
                "the restored stamp must reconstruct the above-gap delivered claim from the "
                        + "forwarded index — losing it across a restart would let post-restart "
                        + "outputs under-claim delivered causal past");
    }

    /**
     * A scope shrink re-homes an above-gap delivered offset on the retiring channel into the
     * carried-ancestry clock, like any other delivered causal past (T3.0 A6: skipped, never
     * dropped). Without this, a restart that removes an input would erase the stamp's claim to a
     * record the node genuinely delivered — the same under-claim the re-homing rule exists to
     * prevent for frontier entries.
     *
     * Asserts the stamp still claims the retired channel's above-gap offset after the shrink,
     * across a store-backed restart.
     */
    @Test
    void rescopeReHomesAnAboveGapDeliveryIntoTheCarriedAncestryClock() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyChannels original = new ParsleyChannels(store, forwardedIndex);
        original.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        original.receive(C2_ID, 0, 0);
        original.delivered(C2_ID, 0, 0);
        original.receive(C2_ID, 0, 1); // received but held
        original.receive(C2_ID, 0, 2);
        original.delivered(C2_ID, 0, 2); // delivered above the gap at offset 1

        // Restart with C2 removed from the declared inputs — the A6 shrink path.
        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);
        restored.rescope(Map.of("C1", C1_ID), 0);

        assertEquals(2L, restored.stamp().offsetFor(C2_ID, 0),
                "the retired channel's above-gap delivered offset must survive the shrink in the "
                        + "carried ancestry — delivered causal past is re-homed, never dropped");
    }

    /**
     * A recreated input's old UUID leaves the above-gap delivered claim with everything else: the
     * old coordinates can never be delivered by any receiver again (E1), which is I9's one
     * permitted removal from stamp-feeding state.
     *
     * Asserts the stamp carries no claim at all for the destroyed UUID after the rescope.
     */
    @Test
    void rescopeDestroysARecreatedInputsAboveGapClaimWithItsUuid() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyChannels original = new ParsleyChannels(store, forwardedIndex);
        original.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        original.receive(C2_ID, 0, 0);
        original.delivered(C2_ID, 0, 0);
        original.receive(C2_ID, 0, 1); // received but held
        original.receive(C2_ID, 0, 2);
        original.delivered(C2_ID, 0, 2); // delivered above the gap at offset 1

        // Restart with C2 recreated: same name, new UUID — the destroyed-coordinate path.
        Uuid recreatedC2 = Uuid.randomUuid();
        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);
        restored.rescope(Map.of("C1", C1_ID, "C2", recreatedC2), 0);

        assertEquals(-1L, restored.stamp().offsetFor(C2_ID, 0),
                "a destroyed (recreated) UUID's above-gap claim must leave the stamp outright — "
                        + "no receiver can ever deliver the old coordinates (E1)");
    }

    /**
     * {@code rescope} re-homes — never drops — the ancestry a scope shrink retires (T3.0 A6). A
     * channel-clock entry for a topic that has left the input set folds into the carried-ancestry
     * clock, so completeness (the outbound stamp) is unchanged by the prune: dropping it, as the old
     * {@code pruneToScope} did, would under-claim every subsequent stamp (I2) and let a third party
     * downstream reorder the retired channel's causes against their effects (I9).
     *
     * Asserts that after rescoping to an input set without the retired ancestor's channel,
     * completeness still carries the ancestor at its full value, the frontier no longer gates on the
     * retired coordinate, and the re-homed value persists across a reload.
     */
    @Test
    void rescopeReHomesRetiredAncestryIntoTheCarriedAncestryClock() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());

        // C1's channel advertises transitive ancestry on ANC (an upstream topic) and on C2 (a
        // consumed sibling); ANC is delivered on directly too, then retired from the input set.
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID, "ANC", ANC_ID), 0);
        channels.delivered(C1_ID, 0, 0);
        channels.delivered(C1_ID, 0, 1);
        channels.seedIfFirstSeen(ANC_ID, 0, 9);
        channels.delivered(ANC_ID, 0, 9);
        channels.channelUpdate(C1_ID, 0,
                ParsleyVectorClock.empty().observe(ANC_ID, 0, 4).observe(C2_ID, 0, 2));
        assertEquals(9L, channels.completeness().offsetFor(ANC_ID, 0),
                "precondition: the soon-retired ancestor is delivered and advertised before the rescope");

        // The new input set: C1 and C2 only — ANC has left the topology.
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);

        assertEquals(9L, channels.completeness().offsetFor(ANC_ID, 0),
                "the retired coordinate must re-home into the carried ancestry at its full delivered "
                        + "value — dropping it would under-claim the stamp (I2/I9, T3.0 A6)");
        assertEquals(-1L, channels.frontier().offsetFor(ANC_ID, 0),
                "the retired coordinate must leave the frontier — the gate view — even as the stamp "
                        + "keeps carrying it");
        assertEquals(2L, channels.completeness().offsetFor(C2_ID, 0),
                "live transitive ancestry inside the surviving channel clock must survive the rescope");
        assertEquals(1L, channels.frontier().offsetFor(C1_ID, 0),
                "the in-scope frontier entry must survive the rescope");

        ParsleyChannels reloaded = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(9L, reloaded.completeness().offsetFor(ANC_ID, 0),
                "the carried ancestry must persist in the \"f\" blob: a reload must keep stamping it");
    }

    /**
     * {@code rescope} treats a recreated input — the same topic name declared with a different UUID —
     * as provably destroyed: the old UUID's entries leave the frontier, the channel clocks, and the
     * carried ancestry outright (E1: a recreated topic's offsets rebind to different records, so no
     * receiver can ever deliver them), while everything else re-homes as usual.
     *
     * Asserts the old UUID vanishes from completeness after the rescope and the new UUID starts
     * fresh, with the destruction persisted.
     */
    @Test
    void rescopeDestroysARecreatedInputsOldUuidOutright() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());

        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        channels.seedIfFirstSeen(C1_ID, 0, 5);
        channels.delivered(C1_ID, 0, 5);
        channels.channelUpdate(C2_ID, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 3));
        assertEquals(5L, channels.completeness().offsetFor(C1_ID, 0),
                "precondition: the soon-destroyed UUID is delivered and advertised before the rescope");

        // C1 is deleted and recreated: same name, new UUID.
        Uuid recreatedC1 = Uuid.randomUuid();
        channels.rescope(Map.of("C1", recreatedC1, "C2", C2_ID), 0);

        assertEquals(-1L, channels.completeness().offsetFor(C1_ID, 0),
                "the destroyed UUID must leave every stamp-feeding structure — it can never be "
                        + "delivered by any receiver (E1), so re-homing it would carry a dead claim forever");
        assertEquals(-1L, channels.frontier().offsetFor(recreatedC1, 0),
                "the recreated topic's new UUID has no carried ancestry, so it starts unseeded");

        ParsleyChannels reloaded = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(-1L, reloaded.completeness().offsetFor(C1_ID, 0),
                "the destruction must persist: a reload must not resurrect the dead UUID");
    }

    /**
     * {@code rescope} seeds an added input's frontier at the node's carried-ancestry value for that
     * coordinate (T3.0 A5 — "skip what you already ignored"): an input removed in one deployment and
     * re-added in a later one must not re-deliver the prefix this node already delivered or carried,
     * and the forwarded index is pruned at or below the seed to match. A genuinely new input with no
     * carried entry seeds nothing and starts like any first sighting.
     *
     * Asserts the re-added coordinate's frontier seeds at the re-homed value, the new topic stays
     * unseeded, and {@code alreadyDelivered} then reports the carried prefix as delivered.
     */
    @Test
    void rescopeSeedsAnAddedInputsFrontierFromCarriedAncestryNeverLogStart() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        MockForwardedIndex forwardedIndex = new MockForwardedIndex();
        ParsleyChannels channels = new ParsleyChannels(store, forwardedIndex);

        // Deployment 1: C1 and C2 consumed; C2 delivered up to 7.
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        channels.delivered(C1_ID, 0, 0);
        channels.seedIfFirstSeen(C2_ID, 0, 7);
        channels.delivered(C2_ID, 0, 7);
        // A stale out-of-order forwarded entry for C2 below the eventual seed survives in its store.
        forwardedIndex.mark(C2_ID, 0, 5);

        // Deployment 2: C2 removed — its history re-homes into the carried ancestry.
        channels.rescope(Map.of("C1", C1_ID), 0);
        assertEquals(-1L, channels.frontier().offsetFor(C2_ID, 0),
                "precondition: the removed input left the frontier at the shrink");

        // Deployment 3: C2 re-added — the frontier seeds at the carried value, not log-start.
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID, "C3", ANC_ID), 0);

        assertEquals(7L, channels.frontier().offsetFor(C2_ID, 0),
                "the re-added input must seed at the carried-ancestry value 7 — replaying the prefix "
                        + "at or below what this node already delivered would be cause-after-effect (A5)");
        assertFalse(forwardedIndex.contains(C2_ID, 0, 5),
                "the forwarded index must be pruned at or below the seed, mirroring the restore-time sweep");
        assertEquals(-1L, channels.frontier().offsetFor(ANC_ID, 0),
                "a genuinely new input with no carried ancestry seeds nothing — its history has no "
                        + "delivered descendants here (I2), so replaying it is ordinary delivery");
        assertTrue(channels.alreadyDelivered(C2_ID, 0, 7),
                "the seeded prefix must read as already delivered, so the receive path skips its replay");
        assertFalse(channels.alreadyDelivered(C2_ID, 0, 8),
                "the first offset above the seed is undelivered — replay resumes normal delivery there");
    }

    /**
     * {@code rescope}'s growth seed reads {@link ParsleyChannels#stamp()}, not
     * {@code completeness()} ("skip what you already claimed" extends A5's "skip what you already
     * ignored", T2.2 → T2.3): an added input that is this node's own former sink seeds at the
     * ownOutputs position its stamps already claimed — delivering that prefix into surviving state
     * would replay records every downstream gate already treats as this node's causal past.
     *
     * Asserts the added own-former-sink input seeds at the ownOutputs value even though nothing was
     * ever delivered or carried on it.
     */
    @Test
    void rescopeSeedsAnAddedFormerSinkFromOwnOutputsNotJustCompleteness() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());

        // Deployment 1: only C1 consumed; C2 is a pure sink whose sends were acked up to 9 — it is
        // claimed by every stamp (completeness ∪ ownOutputs) without ever being delivered here.
        channels.rescope(Map.of("C1", C1_ID), 0);
        channels.acknowledge(C2_ID, 0, 9);
        assertEquals(-1L, channels.completeness().offsetFor(C2_ID, 0),
                "precondition: the sink coordinate is not in completeness — only ownOutputs claims it");

        // Deployment 2: C2 added as an input (the node now consumes its own former sink).
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);

        assertEquals(9L, channels.frontier().offsetFor(C2_ID, 0),
                "the added former sink must seed at the ownOutputs value its stamps already "
                        + "claimed — an under-seed would re-deliver a prefix downstream gates "
                        + "already order behind this node's outputs");
    }

    /**
     * {@link ParsleyChannels#stamp()} is {@code completeness ∪ ownOutputs} (D2): the outbound
     * vector timestamp carries the acked own-output positions, and equally serves as the node's
     * total knowledge (the I6 relay bound), while {@code completeness()} itself stays free of
     * {@code ownOutputs} (it reports what this node has delivered, never what it produced).
     *
     * Asserts stamp = completeness merged with ownOutputs and completeness excludes ownOutputs.
     */
    @Test
    void stampIsCompletenessMergedWithOwnOutputs() {
        ParsleyChannels channels =
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.seedIfFirstSeen(C1_ID, 0, 3);
        channels.delivered(C1_ID, 0, 3);
        channels.acknowledge(C4_ID, 0, 6);

        assertEquals(3L, channels.stamp().offsetFor(C1_ID, 0),
                "the stamp must carry the delivered frontier");
        assertEquals(6L, channels.stamp().offsetFor(C4_ID, 0),
                "the stamp must carry the acked own-output position (D2)");
        assertEquals(-1L, channels.completeness().offsetFor(C4_ID, 0),
                "completeness must stay free of ownOutputs — only the stamp unions them");
    }

    /**
     * An empty declared input set round-trips through the frontier value: a node that has never
     * rescoped persists an empty declaration (the section is always present, empty or not), and a
     * fresh instance over the same store loads it as empty — so the first {@code rescope} has nothing
     * to diff and simply records the current declaration without seeding anything.
     *
     * Asserts the reloaded declared set is empty and that the first rescope over it neither seeds nor
     * destroys surviving state.
     */
    @Test
    void anEmptyDeclaredInputSetLoadsAndRescopesAsUnchanged() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        // No rescope ever ran here, so the persisted declared-input set is empty. The first rescope
        // must treat that as a fresh declaration with nothing to diff.
        original.seedIfFirstSeen(C1_ID, 0, 3);
        original.delivered(C1_ID, 0, 3);

        ParsleyChannels reloaded = new ParsleyChannels(store, new MockForwardedIndex());
        assertTrue(reloaded.declaredInputs().isEmpty(),
                "a blob with nothing declared must load as an empty input set, not fail");

        reloaded.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        assertEquals(3L, reloaded.frontier().offsetFor(C1_ID, 0),
                "the first rescope over an undeclared blob must keep surviving in-scope state");
        assertEquals(-1L, reloaded.frontier().offsetFor(C2_ID, 0),
                "with no persisted declaration there is no added-input diff, so nothing seeds");
        assertEquals(Map.of("C1", C1_ID, "C2", C2_ID), reloaded.declaredInputs(),
                "the rescope must record the current declaration for the next init to diff against");
    }

    // --- Own outputs (D2, T2.2) -----------------------------------------------------------------
    //
    // The ownOutputs clock is stamp-side state only: acknowledge() folds producer acks (and the
    // init-time end-offset seed) monotonically, and nothing here may leak into completeness() until
    // T2.3 changes the stamp to completeness ∪ ownOutputs.

    private static final Uuid C4_ID = Uuid.randomUuid();

    /**
     * {@code acknowledge} folds monotonically into the {@code ownOutputs} clock: entries only ever
     * rise (a lower or equal offset is a no-op, a negative offset is ignored), which is the
     * property I3 leans on once T2.3 folds this clock into the stamp — and which makes re-draining
     * the interceptor registry idempotent.
     */
    @Test
    void acknowledgeFoldsMonotonicallyIntoOwnOutputs() {
        ParsleyChannels channels =
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        channels.acknowledge(C4_ID, 0, 5);
        assertEquals(5L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "a first ack must establish the coordinate's own-output position");

        channels.acknowledge(C4_ID, 0, 3);
        channels.acknowledge(C4_ID, 0, 5);
        channels.acknowledge(C4_ID, 0, -1);
        assertEquals(5L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "a lower, equal, or negative offset must never lower an own-output entry");

        channels.acknowledge(C4_ID, 0, 9);
        channels.acknowledge(C4_ID, 1, 2);
        assertEquals(9L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "a higher ack must raise the entry");
        assertEquals(2L, channels.ownOutputs().offsetFor(C4_ID, 1),
                "partitions of one sink are independent own-output coordinates (T3.0 A7)");
    }

    /**
     * {@code ownOutputs} is stamp-side only and, until T2.3, not part of the stamp at all:
     * {@link ParsleyChannels#completeness()} must not carry an acknowledged own-output coordinate —
     * T2.2 lands the tracking with the outbound stamp byte-identical to before.
     */
    @Test
    void ownOutputsDoesNotLeakIntoCompletenessBeforeStampIntegration() {
        ParsleyChannels channels =
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.delivered(C1_ID, 0, 0);
        ParsleyVectorClock before = channels.completeness();

        channels.acknowledge(C4_ID, 0, 41);

        assertEquals(before, channels.completeness(),
                "acknowledging own outputs must leave the outbound stamp unchanged until T2.3");
        assertEquals(-1L, channels.completeness().offsetFor(C4_ID, 0),
                "the acked sink coordinate must not appear in completeness yet");
    }

    /**
     * The {@code ownOutputs} clock round-trips through the frontier value: acknowledged sink
     * positions persist and a fresh instance over the same store restores them, without disturbing
     * the sections serialised before it.
     */
    @Test
    void ownOutputsRoundTripsThroughTheFrontierValue() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        original.delivered(C1_ID, 0, 2);
        original.acknowledge(C4_ID, 0, 7);
        original.acknowledge(C4_ID, 1, 3);

        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(7L, restored.ownOutputs().offsetFor(C4_ID, 0),
                "own-output positions must restore from the frontier value");
        assertEquals(3L, restored.ownOutputs().offsetFor(C4_ID, 1),
                "every persisted own-output coordinate must restore");
        assertEquals(original.frontier(), restored.frontier(),
                "the own-outputs section must not disturb the sections before it");
    }

    /**
     * The abort/restart tear the design tolerates (O1): the persisted blob can trail the last
     * transaction's acks, because store caches flush before the producer flush completes acks — and
     * the init-time end-offset seed heals exactly that window. A restored clock missing the final
     * acks is raised by the seed (an {@code acknowledge} at the sink's last appended position), and
     * a late replayed ack below the healed value is a no-op (I8: entries only ever rise).
     */
    @Test
    void restoredOwnOutputsTrailingTheLastTransactionIsHealedByTheEndOffsetSeed() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        // Acks up to offset 10 were persisted; the crashed transaction's acks at 11..12 were not.
        original.acknowledge(C4_ID, 0, 10);

        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(10L, restored.ownOutputs().offsetFor(C4_ID, 0),
                "precondition: the restored clock trails the crashed transaction's acks");

        // Init-time seed: the sink's end offset is 13, so its last appended position is 12.
        restored.acknowledge(C4_ID, 0, 12);
        assertEquals(12L, restored.ownOutputs().offsetFor(C4_ID, 0),
                "the end-offset seed must heal the trailing clock up to the last appended position");

        restored.acknowledge(C4_ID, 0, 11);
        assertEquals(12L, restored.ownOutputs().offsetFor(C4_ID, 0),
                "a replayed ack below the healed value must never lower the clock (I8)");
    }

    /**
     * The end-offset seed's over-claim path (I8): the seed claims the sink's last appended position
     * even when this task produced none of it — a sibling's records on a shared sink, an aborted
     * tail, or a transaction marker may sit there. The claim can only delay downstream delivery,
     * never reorder it, and the clock never recedes toward the "truthful" lower value afterwards.
     */
    @Test
    void endOffsetSeedOverClaimsSoundlyAndNeverRecedes() {
        ParsleyChannels channels =
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        // Seed from end offset 42 (last appended position 41) with nothing produced by this task.
        channels.acknowledge(C4_ID, 0, 41);
        assertEquals(41L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "the seed must claim the last appended position regardless of who appended it");

        // This task's first real ack lands far below the seed: the over-claim must stand.
        channels.acknowledge(C4_ID, 0, 7);
        assertEquals(41L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "a real ack below the seed must not lower the entry — I8 mechanisms only ever raise");
    }

    /**
     * {@code foldAcknowledgedOutputs} drains the bound source through the sink-name → UUID map:
     * a coordinate whose topic name resolves folds into {@code ownOutputs}; a name absent from the
     * map (a sink whose UUID could not be resolved at init) is skipped rather than failing; and
     * with no source bound the fold is a no-op (the TopologyTestDriver case).
     */
    @Test
    void foldAcknowledgedOutputsDrainsTheBoundSourceThroughTheNameToUuidMap() {
        ParsleyChannels channels =
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.foldAcknowledgedOutputs();
        assertTrue(channels.ownOutputs().isEmpty(),
                "an unbound fold must be a no-op, not a failure");

        channels.bindOwnOutputSource(consumer -> {
            consumer.accept("OUT", 0, 6);
            consumer.accept("unresolved-sink", 0, 99);
        }, (except, timeoutMs) -> { }, Map.of("OUT", C4_ID), 1_000L);
        channels.foldAcknowledgedOutputs();

        assertEquals(6L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "an ack whose topic name resolves must fold under the sink's UUID identity");
        assertEquals(ParsleyVectorClock.empty().observe(C4_ID, 0, 6), channels.ownOutputs(),
                "an ack for an unresolvable sink name must be skipped — no other entry may appear");
    }

    /**
     * {@code rescope}'s destroyed-coordinate rule reaches {@code ownOutputs} (I9's one permitted
     * removal from stamp-feeding state): an input topic this node also produces (a cycle) that was
     * deleted and recreated purges its old UUID from the own-output clock — no receiver can ever
     * deliver the old UUID's coordinates (E1) — while entries for unrelated sinks survive, and an
     * ordinary input-set shrink or growth never touches the clock at all (it is sink-keyed).
     */
    @Test
    void rescopePurgesOnlyDestroyedCoordinatesFromOwnOutputs() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());
        // C1 is both input and own sink (a cycle); SINK is an ordinary, non-input sink.
        channels.rescope(Map.of("C1", C1_ID, "C2", C2_ID), 0);
        channels.acknowledge(C1_ID, 0, 4);
        channels.acknowledge(C4_ID, 0, 8);

        // An ordinary shrink (C2 leaves) and growth (C3 arrives) must leave ownOutputs alone.
        Uuid c3 = Uuid.randomUuid();
        channels.rescope(Map.of("C1", C1_ID, "C3", c3), 0);
        assertEquals(4L, channels.ownOutputs().offsetFor(C1_ID, 0),
                "an input-set change must not prune own outputs — the clock is sink-keyed");
        assertEquals(8L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "a non-input sink's entry is untouched by any input-set change");

        // C1 recreated: same name, new UUID — the old UUID is provably destroyed.
        Uuid recreated = Uuid.randomUuid();
        channels.rescope(Map.of("C1", recreated, "C3", c3), 0);
        assertEquals(-1L, channels.ownOutputs().offsetFor(C1_ID, 0),
                "a destroyed coordinate must leave the own-output clock — no receiver can deliver it");
        assertEquals(8L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "unrelated own-output entries must survive the destruction");
    }

    /**
     * The declared-sink set (name → UUID) round-trips through the frontier value (T3.4): the next
     * init reads it to heal the restored {@code ownOutputs} clock's trailing acks for topics that are
     * no longer sinks then. A value written before any declaration carries an empty set.
     *
     * Asserts the persisted declaration is reproduced by a fresh instance over the same store, and
     * that a value written before any declaration loads empty.
     */
    @Test
    void declaredSinksRoundTripThroughTheFrontierValue() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        original.delivered(C1_ID, 0, 0);
        assertEquals(Map.of(), new ParsleyChannels(store, new MockForwardedIndex()).declaredSinks(),
                "a value written before any sink declaration must load an empty declared-sink set");

        original.declareSinks(Map.of("c4", C4_ID));

        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(Map.of("c4", C4_ID), restored.declaredSinks(),
                "the declared-sink set must round-trip through the frontier value");
        assertEquals(0L, restored.frontier().offsetFor(C1_ID, 0),
                "the earlier sections must be unaffected by the trailing sink declaration");
    }

    /**
     * {@code destroyOwnOutput} removes every entry of a provably destroyed sink topic from the
     * {@code ownOutputs} clock and persists — I9's one permitted removal from stamp-feeding state
     * (a deleted or recreated topic's records can never be delivered by any receiver, E1). Used by
     * the init-time former-sink heal.
     *
     * Asserts the destroyed topic's entries leave the clock (all partitions), unrelated entries
     * survive, and the purge is durable across a reload.
     */
    @Test
    void destroyOwnOutputPurgesADestroyedSinksClaimsDurably() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels channels = new ParsleyChannels(store, new MockForwardedIndex());
        channels.acknowledge(C4_ID, 0, 8);
        channels.acknowledge(C4_ID, 1, 3);
        channels.acknowledge(C1_ID, 0, 5);

        channels.destroyOwnOutput(C4_ID);

        assertEquals(-1L, channels.ownOutputs().offsetFor(C4_ID, 0),
                "the destroyed sink's partition-0 claim must leave the clock");
        assertEquals(-1L, channels.ownOutputs().offsetFor(C4_ID, 1),
                "the destroyed sink's partition-1 claim must leave the clock");
        assertEquals(5L, channels.ownOutputs().offsetFor(C1_ID, 0),
                "an unrelated sink's claim must survive the purge");
        assertEquals(-1L, new ParsleyChannels(store, new MockForwardedIndex())
                        .ownOutputs().offsetFor(C4_ID, 0),
                "the purge must be persisted — a reload must not resurrect the destroyed claims");
    }

    // --- bridge(): crossing consumer-skipped (EOS marker / aborted-txn) offsets --------------------
    //
    // A read_committed consumer never returns a transaction commit/abort marker or an aborted record,
    // so a transactionally-produced topic's log rec@0,1,2,MARKER@3,rec@4,5,6 reaches the consumer as
    // offsets 0,1,2,4,5,6 with a permanent hole at 3. bridge() marks such skipped offsets so the
    // contiguous walk crosses them instead of wedging at the first hole. Each helper call models one
    // receive: bridge() then, for a deliverable record, deliver().

    /** Models receive of a deliverable record: bridge the channel below it, then deliver it. */
    private static void receiveAndDeliver(ParsleyChannels frontier, Uuid topicId, int partition, long offset) {
        frontier.bridge(topicId, partition, offset);
        frontier.delivered(topicId, partition, offset);
    }

    /**
     * The contiguous frontier tracks a transactionally-produced input across a commit-marker hole: given
     * the consumer-visible sequence 0,1,2,4,5,6 (offset 3 skipped), the frontier reaches 6, not stalling
     * at 2 the way the raw {@code +1} walk would. This is the frontier-level analogue of the EOS
     * frontier-density integration repro.
     */
    @Test
    void bridgeCrossesAConsumerSkippedOffsetSoTheContiguousWalkReachesTheEnd() {
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        for (long offset : new long[] {0, 1, 2, 4, 5, 6}) {   // 3 is a commit marker the consumer skips
            receiveAndDeliver(frontier, C1_ID, 0, offset);
        }

        assertEquals(6L, frontier.frontier().offsetFor(C1_ID, 0),
                "the frontier must advance past the skipped marker offset 3 to cover all delivered "
                        + "records (0,1,2,4,5,6); a stall at 2 is the density bug");
    }

    /**
     * bridge() returns {@code true} exactly when it advanced the contiguous frontier — the signal the
     * core uses to decide whether to cascade. Crossing a marker that unblocks the walk returns true; a
     * first sighting and an at-least-once replay both return false.
     */
    @Test
    void bridgeReturnsTrueOnlyWhenItAdvancesTheFrontier() {
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        assertFalse(frontier.bridge(C1_ID, 0, 0),
                "the first sighting of a channel bridges nothing (its baseline is the seed's concern)");
        frontier.delivered(C1_ID, 0, 0);
        receiveAndDeliver(frontier, C1_ID, 0, 1);
        receiveAndDeliver(frontier, C1_ID, 0, 2);

        assertTrue(frontier.bridge(C1_ID, 0, 4),
                "bridging the marker at 3 (frontier at 2) advances the frontier to 3 — the walk crosses "
                        + "the hole — so it must return true to trigger a cascade");
        assertEquals(3L, frontier.frontier().offsetFor(C1_ID, 0),
                "the frontier advances to the marker offset itself once crossed; the real record at 4 is "
                        + "delivered separately");
        assertFalse(frontier.bridge(C1_ID, 0, 4),
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
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        receiveAndDeliver(frontier, C1_ID, 0, 0);
        receiveAndDeliver(frontier, C1_ID, 0, 1);
        // Record at 2 is received (bridge records it) but HELD — deliver() is not called for it.
        frontier.bridge(C1_ID, 0, 2);
        // Record at 4 arrives; offset 3 between the held 2 and 4 is a marker.
        assertFalse(frontier.bridge(C1_ID, 0, 4),
                "bridging 3 must not advance the frontier while 2 is still held");
        assertEquals(1L, frontier.frontier().offsetFor(C1_ID, 0),
                "the held record at 2 blocks the walk; the frontier stays at 1 despite the bridged marker");

        // Once the held 2 is delivered, the walk absorbs 2 and the previously-bridged 3 in one run.
        frontier.delivered(C1_ID, 0, 2);
        assertEquals(3L, frontier.frontier().offsetFor(C1_ID, 0),
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
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);

        receiveAndDeliver(frontier, C1_ID, 0, 0);          // frontier and highest-received both at 0

        // A record at 1_000_000 with the entire (0, 1_000_000) run consumer-skipped (a large aborted txn).
        assertTrue(frontier.bridge(C1_ID, 0, 1_000_000L),
                "bridging a large skipped run must advance the frontier");
        assertEquals(999_999L, frontier.frontier().offsetFor(C1_ID, 0),
                "the whole skipped run folds into the frontier, up to just below the received offset");
        assertTrue(forwardedIndex.forwardedAfter(C1_ID, 0, -1L).isEmpty(),
                "the fast path must mark none of the skipped offsets in the forwarded index — O(1), not O(gap)");
    }

    /**
     * The per-channel highest-received offset persists in the {@code "frontier"} value, so bridge()'s skip
     * detection is exact across a restart: after reloading a frontier that had received up to offset 4,
     * a record arriving at 6 (offset 5 a marker) is correctly bridged rather than misread as a first
     * sighting. Without persisting the highest-received offset the reloaded frontier would treat 6 as a
     * first sighting, bridge nothing, and stall at 4.
     */
    @Test
    void highestReceivedPersistsSoBridgeStaysExactAcrossRestart() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        for (long offset : new long[] {0, 1, 2, 4}) {   // 3 skipped; highest received becomes 4
            receiveAndDeliver(original, C1_ID, 0, offset);
        }
        assertEquals(4L, original.frontier().offsetFor(C1_ID, 0), "precondition: frontier reached 4 before restart");

        // Reload from the same store: the highest-received map restores from the frontier value alone.
        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());

        // A record at 6 arrives (offset 5 a marker). Because highest-received restored as 4, the gap at
        // 5 is recognised and bridged.
        assertTrue(restored.bridge(C1_ID, 0, 6),
                "the restored highest-received (4) lets bridge recognise the marker gap at 5 and advance");
        assertEquals(5L, restored.frontier().offsetFor(C1_ID, 0),
                "the frontier crosses the bridged marker 5; a first-sighting misread would have stalled at 4");
        restored.delivered(C1_ID, 0, 6);
        assertEquals(6L, restored.frontier().offsetFor(C1_ID, 0),
                "delivering the real record at 6 then advances the frontier to 6");
    }

    // --- Cross-store tear regression (BACKLOG.md: torn changelog flush under at-least-once) --------
    //
    // The forwarded index and the frontier value are two separate changelog-backed stores with
    // no cross-store atomicity. deliver() must persist the new frontier value before pruning the
    // forwarded-index entries it absorbed, so a crash between the two writes always tears toward "a
    // redundant forwarded-index entry lingers below an already-advanced frontier" (harmless — see the
    // BACKLOG.md LOW item on replayed already-delivered offsets) rather than "the frontier advance is
    // lost after the forwarded-index entry backing it is already gone" (a permanent wedge).

    /**
     * Proves the ordering directly: a {@link ParsleyForwardedIndex} that, at the exact moment {@code
     * unmark()} is called, opens a second {@link ParsleyChannels} over the same store must already see
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
            public boolean contains(Uuid topicId, int partition, long offset) {
                return delegate.contains(topicId, partition, offset);
            }

            @Override
            public void unmark(Uuid topicId, int partition, long offset) {
                // A fresh frontier over the same store sees only what has actually been written —
                // not this call's own in-progress in-memory state.
                ParsleyChannels persistedView = new ParsleyChannels(store, new MockForwardedIndex());
                persistedOffsetAtUnmarkTime.add(persistedView.frontier().offsetFor(C1_ID, 0));
                delegate.unmark(topicId, partition, offset);
            }

            @Override
            public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
                delegate.pruneAtOrBelow(topicId, partition, watermark);
            }
        };

        ParsleyChannels frontier = new ParsleyChannels(store, recordingIndex);
        frontier.delivered(C1_ID, 0, 0);

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
            public boolean contains(Uuid topicId, int partition, long offset) {
                return delegate.contains(topicId, partition, offset);
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
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), crashyIndex);

        frontier.delivered(C1_ID, 0, 0);

        assertEquals(0L, frontier.frontier().offsetFor(C1_ID, 0),
                "the frontier already advanced to 0 — persist() ran before the (lost) unmark");
        assertEquals(List.of(0L), delegate.forwardedAfter(C1_ID, 0, -1),
                "the now-redundant entry for the already-absorbed offset lingers, harmlessly, in the "
                        + "forwarded index");

        // A later delivery must proceed normally despite the leaked entry sitting below the frontier —
        // this is the crux of the regression: the tear must never strand the coordinate.
        frontier.delivered(C1_ID, 0, 1);

        assertEquals(1L, frontier.frontier().offsetFor(C1_ID, 0),
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
        ParsleyChannels frontier = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);

        frontier.delivered(C1_ID, 0, 0);
        frontier.delivered(C1_ID, 0, 1);
        assertEquals(1L, frontier.frontier().offsetFor(C1_ID, 0), "frontier advances normally through 0, 1");

        // Replay: offset 0 (below the watermark) and offset 1 (exactly at it) redelivered.
        frontier.delivered(C1_ID, 0, 0);
        frontier.delivered(C1_ID, 0, 1);

        assertEquals(1L, frontier.frontier().offsetFor(C1_ID, 0),
                "a replay of an already-delivered offset must not move the frontier");
        assertTrue(forwardedIndex.forwardedAfter(C1_ID, 0, -1).isEmpty(),
                "a replayed already-delivered offset must never be marked in the forwarded index — the "
                        + "absorb walk only scans strictly above the watermark, so it could never be "
                        + "found and unmarked again");
    }

    /**
     * Regression test for BACKLOG.md's LOW item: a stale below-watermark forwarded-index entry — left
     * over from the acknowledged benign tear direction {@link #delivered}'s Javadoc describes (frontier
     * persisted, unmark lost) — is never reachable by the absorb walk again (it only scans strictly
     * above the watermark), so it would otherwise linger in the changelog-backed store forever. A fresh
     * {@link ParsleyChannels} over a restored store must sweep it away once, at load.
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
            public boolean contains(Uuid topicId, int partition, long offset) {
                return delegate.contains(topicId, partition, offset);
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

        ParsleyChannels original = new ParsleyChannels(store, crashyIndex);
        original.delivered(C1_ID, 0, 0); // offset 0 absorbed, but its unmark is lost — leaks below the watermark
        original.delivered(C1_ID, 0, 1); // watermark advances to 1; the offset-0 entry is now stale

        assertEquals(List.of(0L), delegate.forwardedAfter(C1_ID, 0, -1),
                "the stale entry must still be sitting in the forwarded index before any restore");

        // Restore: a fresh frontier over the same durable store must sweep the stale entry away.
        new ParsleyChannels(store, delegate);

        assertTrue(delegate.forwardedAfter(C1_ID, 0, -1).isEmpty(),
                "restoring the frontier must sweep the stale below-watermark entry left by the crash");
    }
}
