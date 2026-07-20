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
 * clocks both round-trip through the one {@code "f"} key of the frontier store.
 */
class ParsleyChannelsTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
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
        // Advance the contiguous frontier on T1 and record channel clocks for two inputs.
        original.delivered(T1_ID, 0, 0);
        original.delivered(T1_ID, 0, 1);
        original.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty().observe(ANC_ID, 0, 4));
        original.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty().observe(ANC_ID, 0, 7));

        ParsleyVectorClock frontierBefore = original.frontier();
        ParsleyVectorClock completenessBefore = original.completeness();

        // Reload: a fresh frontier over the same store restores from the "f" blob alone.
        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());

        assertEquals(frontierBefore, restored.frontier(),
                "the contiguous frontier clock must round-trip through the \"f\" blob");
        assertEquals(1L, restored.frontier().offsetFor(T1_ID, 0),
                "T1 must restore at its delivered offset 1");
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

        channels.receive(T1_ID, 0, 0);
        channels.delivered(T1_ID, 0, 0);
        channels.receive(T1_ID, 0, 1);
        channels.delivered(T1_ID, 0, 1);
        channels.receive(T1_ID, 0, 2); // received but held — a genuine gap, not a consumer skip
        channels.receive(T1_ID, 0, 3);
        channels.delivered(T1_ID, 0, 3); // delivered above the gap at offset 2

        assertEquals(1L, channels.frontier().offsetFor(T1_ID, 0),
                "the contiguous frontier must not advance past the held offset 2");
        assertEquals(1L, channels.completeness().offsetFor(T1_ID, 0),
                "completeness (the floor-publication view) must not claim above the gap either");
        assertEquals(3L, channels.stamp().offsetFor(T1_ID, 0),
                "the outbound stamp must claim the record delivered above the gap — its coordinate "
                        + "is real delivered causal past, and omitting it lets a downstream consumer "
                        + "deliver a derived output before this cause");
    }

    /**
     * {@code highestDelivered} is deliberately not persisted: an above-gap delivered offset is
     * exactly a forwarded-index mark, committed in the same EOS transaction as the {@code "f"}
     * blob, so a fresh {@link ParsleyChannels} over the same stores must reconstruct the stamp's
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
        original.receive(T1_ID, 0, 0);
        original.delivered(T1_ID, 0, 0);
        original.receive(T1_ID, 0, 1); // received but held
        original.receive(T1_ID, 0, 2);
        original.delivered(T1_ID, 0, 2); // delivered above the gap at offset 1

        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);

        assertEquals(0L, restored.frontier().offsetFor(T1_ID, 0),
                "the restored frontier must still sit below the gap");
        assertEquals(2L, restored.stamp().offsetFor(T1_ID, 0),
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
        original.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        original.receive(T2_ID, 0, 0);
        original.delivered(T2_ID, 0, 0);
        original.receive(T2_ID, 0, 1); // received but held
        original.receive(T2_ID, 0, 2);
        original.delivered(T2_ID, 0, 2); // delivered above the gap at offset 1

        // Restart with T2 removed from the declared inputs — the A6 shrink path.
        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);
        restored.rescope(Map.of("T1", T1_ID), 0);

        assertEquals(2L, restored.stamp().offsetFor(T2_ID, 0),
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
        original.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        original.receive(T2_ID, 0, 0);
        original.delivered(T2_ID, 0, 0);
        original.receive(T2_ID, 0, 1); // received but held
        original.receive(T2_ID, 0, 2);
        original.delivered(T2_ID, 0, 2); // delivered above the gap at offset 1

        // Restart with T2 recreated: same name, new UUID — the destroyed-coordinate path.
        Uuid recreatedT2 = Uuid.randomUuid();
        ParsleyChannels restored = new ParsleyChannels(store, forwardedIndex);
        restored.rescope(Map.of("T1", T1_ID, "T2", recreatedT2), 0);

        assertEquals(-1L, restored.stamp().offsetFor(T2_ID, 0),
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

        // T1's channel advertises transitive ancestry on ANC (an upstream topic) and on T2 (a
        // consumed sibling); ANC is delivered on directly too, then retired from the input set.
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID, "ANC", ANC_ID), 0);
        channels.delivered(T1_ID, 0, 0);
        channels.delivered(T1_ID, 0, 1);
        channels.seedIfFirstSeen(ANC_ID, 0, 9);
        channels.delivered(ANC_ID, 0, 9);
        channels.channelUpdate(T1_ID, 0,
                ParsleyVectorClock.empty().observe(ANC_ID, 0, 4).observe(T2_ID, 0, 2));
        assertEquals(9L, channels.completeness().offsetFor(ANC_ID, 0),
                "precondition: the soon-retired ancestor is delivered and advertised before the rescope");

        // The new input set: T1 and T2 only — ANC has left the topology.
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);

        assertEquals(9L, channels.completeness().offsetFor(ANC_ID, 0),
                "the retired coordinate must re-home into the carried ancestry at its full delivered "
                        + "value — dropping it would under-claim the stamp (I2/I9, T3.0 A6)");
        assertEquals(-1L, channels.frontier().offsetFor(ANC_ID, 0),
                "the retired coordinate must leave the frontier — the gate view — even as the stamp "
                        + "keeps carrying it");
        assertEquals(2L, channels.completeness().offsetFor(T2_ID, 0),
                "live transitive ancestry inside the surviving channel clock must survive the rescope");
        assertEquals(1L, channels.frontier().offsetFor(T1_ID, 0),
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

        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        channels.seedIfFirstSeen(T1_ID, 0, 5);
        channels.delivered(T1_ID, 0, 5);
        channels.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 3));
        assertEquals(5L, channels.completeness().offsetFor(T1_ID, 0),
                "precondition: the soon-destroyed UUID is delivered and advertised before the rescope");

        // T1 is deleted and recreated: same name, new UUID.
        Uuid recreatedT1 = Uuid.randomUuid();
        channels.rescope(Map.of("T1", recreatedT1, "T2", T2_ID), 0);

        assertEquals(-1L, channels.completeness().offsetFor(T1_ID, 0),
                "the destroyed UUID must leave every stamp-feeding structure — it can never be "
                        + "delivered by any receiver (E1), so re-homing it would carry a dead claim forever");
        assertEquals(-1L, channels.frontier().offsetFor(recreatedT1, 0),
                "the recreated topic's new UUID has no carried ancestry, so it starts unseeded");

        ParsleyChannels reloaded = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(-1L, reloaded.completeness().offsetFor(T1_ID, 0),
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

        // Deployment 1: T1 and T2 consumed; T2 delivered up to 7.
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        channels.delivered(T1_ID, 0, 0);
        channels.seedIfFirstSeen(T2_ID, 0, 7);
        channels.delivered(T2_ID, 0, 7);
        // A stale out-of-order forwarded entry for T2 below the eventual seed survives in its store.
        forwardedIndex.mark(T2_ID, 0, 5);

        // Deployment 2: T2 removed — its history re-homes into the carried ancestry.
        channels.rescope(Map.of("T1", T1_ID), 0);
        assertEquals(-1L, channels.frontier().offsetFor(T2_ID, 0),
                "precondition: the removed input left the frontier at the shrink");

        // Deployment 3: T2 re-added — the frontier seeds at the carried value, not log-start.
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID, "T3", ANC_ID), 0);

        assertEquals(7L, channels.frontier().offsetFor(T2_ID, 0),
                "the re-added input must seed at the carried-ancestry value 7 — replaying the prefix "
                        + "at or below what this node already delivered would be cause-after-effect (A5)");
        assertFalse(forwardedIndex.contains(T2_ID, 0, 5),
                "the forwarded index must be pruned at or below the seed, mirroring the restore-time sweep");
        assertEquals(-1L, channels.frontier().offsetFor(ANC_ID, 0),
                "a genuinely new input with no carried ancestry seeds nothing — its history has no "
                        + "delivered descendants here (I2), so replaying it is ordinary delivery");
        assertTrue(channels.alreadyDelivered(T2_ID, 0, 7),
                "the seeded prefix must read as already delivered, so the receive path skips its replay");
        assertFalse(channels.alreadyDelivered(T2_ID, 0, 8),
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

        // Deployment 1: only T1 consumed; T2 is a pure sink whose sends were acked up to 9 — it is
        // claimed by every stamp (completeness ∪ ownOutputs) without ever being delivered here.
        channels.rescope(Map.of("T1", T1_ID), 0);
        channels.acknowledge(T2_ID, 0, 9);
        assertEquals(-1L, channels.completeness().offsetFor(T2_ID, 0),
                "precondition: the sink coordinate is not in completeness — only ownOutputs claims it");

        // Deployment 2: T2 added as an input (the node now consumes its own former sink).
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);

        assertEquals(9L, channels.frontier().offsetFor(T2_ID, 0),
                "the added former sink must seed at the ownOutputs value its stamps already "
                        + "claimed — an under-seed would re-deliver a prefix downstream gates "
                        + "already order behind this node's outputs");
    }

    /**
     * {@link ParsleyChannels#stamp()} is {@code completeness ∪ ownOutputs} (D2): the outbound
     * vector timestamp carries the acked own-output positions, and equally serves as the node's
     * total knowledge (the I6 relay bound), while {@code completeness()} itself stays free of
     * {@code ownOutputs} (the interim epoch-floor publication path reads it).
     *
     * Asserts stamp = completeness merged with ownOutputs and completeness excludes ownOutputs.
     */
    @Test
    void stampIsCompletenessMergedWithOwnOutputs() {
        ParsleyChannels channels =
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.seedIfFirstSeen(T1_ID, 0, 3);
        channels.delivered(T1_ID, 0, 3);
        channels.acknowledge(SINK_ID, 0, 6);

        assertEquals(3L, channels.stamp().offsetFor(T1_ID, 0),
                "the stamp must carry the delivered frontier");
        assertEquals(6L, channels.stamp().offsetFor(SINK_ID, 0),
                "the stamp must carry the acked own-output position (D2)");
        assertEquals(-1L, channels.completeness().offsetFor(SINK_ID, 0),
                "completeness must stay free of ownOutputs — only the stamp unions them");
    }

    /**
     * The declared input set and the carried ancestry are trailing-optional sections of the {@code
     * "f"} blob: a blob written before they existed (simulated by serialising with none recorded)
     * still loads, reporting an empty declared set — so the first {@code rescope} over an upgraded
     * store has nothing to diff and simply records the current declaration without seeding anything.
     *
     * Asserts a pre-section blob loads with empty declared inputs and that the first rescope over it
     * neither seeds nor destroys surviving state.
     */
    @Test
    void aBlobWithoutTheDeclaredInputSectionLoadsAndRescopesAsUnchanged() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        // No rescope ever ran here: the blob carries frontier/channel state but an empty declared set,
        // standing in for a pre-T1.3 blob (the sections are also simply absent on truncation — load()
        // treats both identically).
        original.seedIfFirstSeen(T1_ID, 0, 3);
        original.delivered(T1_ID, 0, 3);

        ParsleyChannels reloaded = new ParsleyChannels(store, new MockForwardedIndex());
        assertTrue(reloaded.declaredInputs().isEmpty(),
                "a blob with nothing declared must load as an empty input set, not fail");

        reloaded.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        assertEquals(3L, reloaded.frontier().offsetFor(T1_ID, 0),
                "the first rescope over an undeclared blob must keep surviving in-scope state");
        assertEquals(-1L, reloaded.frontier().offsetFor(T2_ID, 0),
                "with no persisted declaration there is no added-input diff, so nothing seeds");
        assertEquals(Map.of("T1", T1_ID, "T2", T2_ID), reloaded.declaredInputs(),
                "the rescope must record the current declaration for the next init to diff against");
    }

    // --- Own outputs (D2, T2.2) -----------------------------------------------------------------
    //
    // The ownOutputs clock is stamp-side state only: acknowledge() folds producer acks (and the
    // init-time end-offset seed) monotonically, and nothing here may leak into completeness() until
    // T2.3 changes the stamp to completeness ∪ ownOutputs.

    private static final Uuid SINK_ID = Uuid.randomUuid();

    /**
     * {@code acknowledge} folds monotonically into the {@code ownOutputs} clock: entries only ever
     * rise (a lower or equal offset is a no-op, a negative offset is ignored), which is the
     * property I3 leans on once T2.3 folds this clock into the stamp — and which makes re-draining
     * the interceptor registry idempotent.
     */
    @Test
    void acknowledgeFoldsMonotonicallyIntoOwnOutputs() {
        ParsleyChannels channels =
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        channels.acknowledge(SINK_ID, 0, 5);
        assertEquals(5L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "a first ack must establish the coordinate's own-output position");

        channels.acknowledge(SINK_ID, 0, 3);
        channels.acknowledge(SINK_ID, 0, 5);
        channels.acknowledge(SINK_ID, 0, -1);
        assertEquals(5L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "a lower, equal, or negative offset must never lower an own-output entry");

        channels.acknowledge(SINK_ID, 0, 9);
        channels.acknowledge(SINK_ID, 1, 2);
        assertEquals(9L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "a higher ack must raise the entry");
        assertEquals(2L, channels.ownOutputs().offsetFor(SINK_ID, 1),
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
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.delivered(T1_ID, 0, 0);
        ParsleyVectorClock before = channels.completeness();

        channels.acknowledge(SINK_ID, 0, 41);

        assertEquals(before, channels.completeness(),
                "acknowledging own outputs must leave the outbound stamp unchanged until T2.3");
        assertEquals(-1L, channels.completeness().offsetFor(SINK_ID, 0),
                "the acked sink coordinate must not appear in completeness yet");
    }

    /**
     * The {@code ownOutputs} clock round-trips through its trailing section of the {@code "f"}
     * blob, and a blob written before the section existed (simulated by a pre-T2.2 write path:
     * nothing acknowledged persists an empty clock — load() treats a truncated blob identically)
     * loads with an empty clock rather than failing.
     */
    @Test
    void ownOutputsRoundTripsThroughTheFBlob() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        original.delivered(T1_ID, 0, 2);
        original.acknowledge(SINK_ID, 0, 7);
        original.acknowledge(SINK_ID, 1, 3);

        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(7L, restored.ownOutputs().offsetFor(SINK_ID, 0),
                "own-output positions must restore from the \"f\" blob's trailing section");
        assertEquals(3L, restored.ownOutputs().offsetFor(SINK_ID, 1),
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
        original.acknowledge(SINK_ID, 0, 10);

        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());
        assertEquals(10L, restored.ownOutputs().offsetFor(SINK_ID, 0),
                "precondition: the restored clock trails the crashed transaction's acks");

        // Init-time seed: the sink's end offset is 13, so its last appended position is 12.
        restored.acknowledge(SINK_ID, 0, 12);
        assertEquals(12L, restored.ownOutputs().offsetFor(SINK_ID, 0),
                "the end-offset seed must heal the trailing clock up to the last appended position");

        restored.acknowledge(SINK_ID, 0, 11);
        assertEquals(12L, restored.ownOutputs().offsetFor(SINK_ID, 0),
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
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        // Seed from end offset 42 (last appended position 41) with nothing produced by this task.
        channels.acknowledge(SINK_ID, 0, 41);
        assertEquals(41L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "the seed must claim the last appended position regardless of who appended it");

        // This task's first real ack lands far below the seed: the over-claim must stand.
        channels.acknowledge(SINK_ID, 0, 7);
        assertEquals(41L, channels.ownOutputs().offsetFor(SINK_ID, 0),
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
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        channels.foldAcknowledgedOutputs();
        assertTrue(channels.ownOutputs().isEmpty(),
                "an unbound fold must be a no-op, not a failure");

        channels.bindOwnOutputSource(consumer -> {
            consumer.accept("OUT", 0, 6);
            consumer.accept("unresolved-sink", 0, 99);
        }, (except, timeoutMs) -> { }, Map.of("OUT", SINK_ID), 1_000L);
        channels.foldAcknowledgedOutputs();

        assertEquals(6L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "an ack whose topic name resolves must fold under the sink's UUID identity");
        assertEquals(ParsleyVectorClock.empty().observe(SINK_ID, 0, 6), channels.ownOutputs(),
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
        // T1 is both input and own sink (a cycle); SINK is an ordinary, non-input sink.
        channels.rescope(Map.of("T1", T1_ID, "T2", T2_ID), 0);
        channels.acknowledge(T1_ID, 0, 4);
        channels.acknowledge(SINK_ID, 0, 8);

        // An ordinary shrink (T2 leaves) and growth (T3 arrives) must leave ownOutputs alone.
        Uuid t3 = Uuid.randomUuid();
        channels.rescope(Map.of("T1", T1_ID, "T3", t3), 0);
        assertEquals(4L, channels.ownOutputs().offsetFor(T1_ID, 0),
                "an input-set change must not prune own outputs — the clock is sink-keyed");
        assertEquals(8L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "a non-input sink's entry is untouched by any input-set change");

        // T1 recreated: same name, new UUID — the old UUID is provably destroyed.
        Uuid recreated = Uuid.randomUuid();
        channels.rescope(Map.of("T1", recreated, "T3", t3), 0);
        assertEquals(-1L, channels.ownOutputs().offsetFor(T1_ID, 0),
                "a destroyed coordinate must leave the own-output clock — no receiver can deliver it");
        assertEquals(8L, channels.ownOutputs().offsetFor(SINK_ID, 0),
                "unrelated own-output entries must survive the destruction");
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
        ParsleyChannels frontier =
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex(), true, state);
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());          // the node's one input channel
        frontier.recordEpochMarker(1, ParsleyVectorClock.empty().observe(T1_ID, 0, 3), T1_ID, 0);

        // A peer's watermark claims T1@3 on the channel clock — completeness now dominates the floor,
        // but this node has delivered nothing itself.
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 3));
        assertFalse(frontier.tryAdvanceEpoch(),
                "a channel's advertised claim of T1@3 must not close the window — this node has not "
                        + "delivered T1@0..3 itself, so e-1 is not provably drained here");

        for (long offset = 0; offset <= 3; offset++) {
            frontier.delivered(T1_ID, 0, offset);
        }
        assertTrue(frontier.tryAdvanceEpoch(),
                "once this node's own contiguous frontier reaches the floor, the window closes");
        assertEquals(1L, state.settledEpochId(),
                "the pending epoch must be promoted to settled when the window closes");
    }

    /** T1 floored at 100; every other coordinate unbounded. */
    private static final ParsleyEpoch FLOOR_T1_AT_100 = (topicId, partition) ->
            topicId.equals(T1_ID) ? 100L : ParsleyEpoch.NO_BOUND;

    private static ParsleyChannels flooredFrontier(ParsleyEpoch epoch) {
        return new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex(), true, epoch);
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
        ParsleyChannels frontier = flooredFrontier(FLOOR_T1_AT_100);

        frontier.delivered(T1_ID, 0, 5);

        assertEquals(-1L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = flooredFrontier(FLOOR_T1_AT_100);

        assertTrue(frontier.seedIfFirstSeen(T1_ID, 0, 5),
                "the first sighting of T1 must seed the frontier");
        assertEquals(99L, frontier.frontier().offsetFor(T1_ID, 0),
                "the seed must land at the epoch origin (startsAt - 1 = 99), not the below-floor offset 5");

        frontier.delivered(T1_ID, 0, 100);
        assertEquals(100L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = flooredFrontier(FLOOR_T1_AT_100);

        // Replay below-floor history with a gap at offset 6 — all out of domain.
        frontier.seedIfFirstSeen(T1_ID, 0, 5);
        frontier.delivered(T1_ID, 0, 5);
        frontier.delivered(T1_ID, 0, 7);   // gap at 6 — would stall a naive contiguous walk

        assertEquals(99L, frontier.frontier().offsetFor(T1_ID, 0),
                "below-floor replay (with a gap) must leave the frontier at the epoch origin, not stalled below it");
        assertTrue(frontier.completeness().offsetFor(T1_ID, 0) < 100L,
                "no in-domain T1 has been delivered yet, so completeness sits below the floor (at the origin)");

        // The first in-domain delivery advances the frontier from the origin, unaffected by the gap.
        frontier.delivered(T1_ID, 0, 100);
        assertEquals(100L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = flooredFrontier(FLOOR_T1_AT_100);

        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 5)); // below floor

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
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T1_ID, 0, 5), 1);
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex(), epoch);

        // Adopt an epoch-2 boundary (marker on one channel); the window stays open (nothing dominates it).
        original.recordEpochMarker(2, ParsleyVectorClock.empty().observe(T1_ID, 0, 20), T1_ID, 0);
        assertTrue(epoch.isTransitioning(), "the boundary starts a transition");

        // Reload into a fresh epoch state over the same store.
        ParsleyEpochState reloadedEpoch = new ParsleyEpochState();
        new ParsleyChannels(store, new MockForwardedIndex(), reloadedEpoch);

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
        ParsleyChannels frontier = flooredFrontier(ParsleyEpoch.NONE);

        assertTrue(frontier.seedIfFirstSeen(T1_ID, 0, 5),
                "the first sighting seeds the frontier even with no epoch floor");
        assertEquals(4L, frontier.frontier().offsetFor(T1_ID, 0),
                "with no floor the seed lands at offset - 1 = 4, exactly as before epochs");

        frontier.delivered(T1_ID, 0, 5);
        assertEquals(5L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        for (long offset : new long[] {0, 1, 2, 4, 5, 6}) {   // 3 is a commit marker the consumer skips
            receiveAndDeliver(frontier, T1_ID, 0, offset);
        }

        assertEquals(6L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        assertFalse(frontier.bridge(T1_ID, 0, 0),
                "the first sighting of a channel bridges nothing (its baseline is the seed's concern)");
        frontier.delivered(T1_ID, 0, 0);
        receiveAndDeliver(frontier, T1_ID, 0, 1);
        receiveAndDeliver(frontier, T1_ID, 0, 2);

        assertTrue(frontier.bridge(T1_ID, 0, 4),
                "bridging the marker at 3 (frontier at 2) advances the frontier to 3 — the walk crosses "
                        + "the hole — so it must return true to trigger a cascade");
        assertEquals(3L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());

        receiveAndDeliver(frontier, T1_ID, 0, 0);
        receiveAndDeliver(frontier, T1_ID, 0, 1);
        // Record at 2 is received (bridge records it) but HELD — deliver() is not called for it.
        frontier.bridge(T1_ID, 0, 2);
        // Record at 4 arrives; offset 3 between the held 2 and 4 is a marker.
        assertFalse(frontier.bridge(T1_ID, 0, 4),
                "bridging 3 must not advance the frontier while 2 is still held");
        assertEquals(1L, frontier.frontier().offsetFor(T1_ID, 0),
                "the held record at 2 blocks the walk; the frontier stays at 1 despite the bridged marker");

        // Once the held 2 is delivered, the walk absorbs 2 and the previously-bridged 3 in one run.
        frontier.delivered(T1_ID, 0, 2);
        assertEquals(3L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex);

        receiveAndDeliver(frontier, T1_ID, 0, 0);          // frontier and highest-received both at 0

        // A record at 1_000_000 with the entire (0, 1_000_000) run consumer-skipped (a large aborted txn).
        assertTrue(frontier.bridge(T1_ID, 0, 1_000_000L),
                "bridging a large skipped run must advance the frontier");
        assertEquals(999_999L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels original = new ParsleyChannels(store, new MockForwardedIndex());
        for (long offset : new long[] {0, 1, 2, 4}) {   // 3 skipped; highest received becomes 4
            receiveAndDeliver(original, T1_ID, 0, offset);
        }
        assertEquals(4L, original.frontier().offsetFor(T1_ID, 0), "precondition: frontier reached 4 before restart");

        // Reload from the same store: the highest-received map restores from the "f" blob alone.
        ParsleyChannels restored = new ParsleyChannels(store, new MockForwardedIndex());

        // A record at 6 arrives (offset 5 a marker). Because highest-received restored as 4, the gap at
        // 5 is recognised and bridged.
        assertTrue(restored.bridge(T1_ID, 0, 6),
                "the restored highest-received (4) lets bridge recognise the marker gap at 5 and advance");
        assertEquals(5L, restored.frontier().offsetFor(T1_ID, 0),
                "the frontier crosses the bridged marker 5; a first-sighting misread would have stalled at 4");
        restored.delivered(T1_ID, 0, 6);
        assertEquals(6L, restored.frontier().offsetFor(T1_ID, 0),
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
                persistedOffsetAtUnmarkTime.add(persistedView.frontier().offsetFor(T1_ID, 0));
                delegate.unmark(topicId, partition, offset);
            }

            @Override
            public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
                delegate.pruneAtOrBelow(topicId, partition, watermark);
            }
        };

        ParsleyChannels frontier = new ParsleyChannels(store, recordingIndex);
        frontier.delivered(T1_ID, 0, 0);

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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), crashyIndex);

        frontier.delivered(T1_ID, 0, 0);

        assertEquals(0L, frontier.frontier().offsetFor(T1_ID, 0),
                "the frontier already advanced to 0 — persist() ran before the (lost) unmark");
        assertEquals(List.of(0L), delegate.forwardedAfter(T1_ID, 0, -1),
                "the now-redundant entry for the already-absorbed offset lingers, harmlessly, in the "
                        + "forwarded index");

        // A later delivery must proceed normally despite the leaked entry sitting below the frontier —
        // this is the crux of the regression: the tear must never strand the coordinate.
        frontier.delivered(T1_ID, 0, 1);

        assertEquals(1L, frontier.frontier().offsetFor(T1_ID, 0),
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
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex);

        frontier.delivered(T1_ID, 0, 0);
        frontier.delivered(T1_ID, 0, 1);
        assertEquals(1L, frontier.frontier().offsetFor(T1_ID, 0), "frontier advances normally through 0, 1");

        // Replay: offset 0 (below the watermark) and offset 1 (exactly at it) redelivered.
        frontier.delivered(T1_ID, 0, 0);
        frontier.delivered(T1_ID, 0, 1);

        assertEquals(1L, frontier.frontier().offsetFor(T1_ID, 0),
                "a replay of an already-delivered offset must not move the frontier");
        assertTrue(forwardedIndex.forwardedAfter(T1_ID, 0, -1).isEmpty(),
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
        original.delivered(T1_ID, 0, 0); // offset 0 absorbed, but its unmark is lost — leaks below the watermark
        original.delivered(T1_ID, 0, 1); // watermark advances to 1; the offset-0 entry is now stale

        assertEquals(List.of(0L), delegate.forwardedAfter(T1_ID, 0, -1),
                "the stale entry must still be sitting in the forwarded index before any restore");

        // Restore: a fresh frontier over the same durable store must sweep the stale entry away.
        new ParsleyChannels(store, delegate);

        assertTrue(delegate.forwardedAfter(T1_ID, 0, -1).isEmpty(),
                "restoring the frontier must sweep the stale below-watermark entry left by the crash");
    }
}
