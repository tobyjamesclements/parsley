package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The T2.4 scope-change property tests (T3.0 A5/A6, over {@link ParsleyTopologySim}): I2 must
 * survive a restart that shrinks or grows the declared input set, under randomised pre- and
 * post-restart interleavings. The one principle both directions share — <em>the causal past a node
 * has delivered or carried may be skipped, but never dropped and never re-entered</em> — splits
 * into: shrink re-homes every retired channel's ancestry into the carried-ancestry clock the
 * stamp keeps merging (A6); growth seeds an added channel at the node's already-claimed value, so
 * the prefix it previously ignored (or itself produced) is skipped, never delivered into
 * surviving state (A5, extended to "skip what you already claimed" for a former own sink).
 * The sim's continuous invariants (I2/I3/I9, ground-truth delivery order, own-output coverage)
 * stay armed throughout every run here.
 */
class ParsleyScopeChangePropertyTest {

    private static final int SEEDS = 10;

    /**
     * A6 — scope shrink re-homes, never drops. Node X consumes t1 and t2 and produces t3; after a
     * random schedule it restarts with t2 removed. Its post-restart stamp must dominate its
     * pre-restart stamp (retired-channel values re-homed into carried ancestry) and every
     * coordinate of its ground-truth causal past — including all t2 ancestry it can no longer
     * observe — and the downstream consumer Y keeps order-checking X's post-shrink outputs, whose
     * clocks still carry the retired ancestry (this is what protects third parties from an
     * effect stamped as if its retired-channel cause never existed).
     *
     * Asserts stamp dominance across the shrink and a further clean randomised run.
     */
    @Test
    void scopeShrinkReHomesRetiredChannelAncestryIntoTheStamp() {
        for (long seed = 6000; seed < 6000 + SEEDS; seed++) {
            ParsleyTopologySim sim = new ParsleyTopologySim(seed);
            sim.externalTopic("t1");
            sim.node("A", Set.of("t1"), List.of("t2"), 0.8);
            sim.node("X", Set.of("t1", "t2"), List.of("t3"), 0.8);
            sim.node("Y", Set.of("t1", "t2", "t3"), List.of(), 0.0);
            sim.run(250);
            ParsleyTopologySim.SimNode x = sim.nodeNamed("X");
            ParsleyVectorClock stampBefore = x.channels.stamp();
            Set<ParsleyTopologySim.SimCoord> truePastBefore = Set.copyOf(x.truePast);
            assertTrue(stampBefore.offsetFor(sim.topicId("t2"), 0) >= 0,
                    "[seed " + seed + "] vacuity guard: X must have delivered t2 ancestry to retire");

            sim.restartWithInputs("X", Set.of("t1"));

            ParsleyVectorClock stampAfter = x.channels.stamp();
            assertTrue(stampAfter.dominates(stampBefore),
                    "[seed " + seed + "] the post-shrink stamp must dominate the pre-shrink stamp — "
                            + "retired-channel values are re-homed into carried ancestry, never dropped (A6)");
            for (ParsleyTopologySim.SimCoord coord : truePastBefore) {
                assertTrue(stampAfter.offsetFor(coord.topicId(), 0) >= coord.offset(),
                        "[seed " + seed + "] the post-shrink stamp must still claim delivered "
                                + "causal past at offset " + coord.offset() + " on a retired channel");
            }
            sim.run(150);
        }
    }

    /**
     * A5 — scope growth seeds the added channel from carried ancestry, never log-start. Node X
     * consumes only t2, which carries nothing but null messages from the silent node P — so X's
     * stamp comes to claim t1 ancestry X has never consumed (the I9 custody chain). X then
     * restarts with t1 added as an input. The added channel must seed at exactly the value X
     * already claimed ("skip what you already ignored"): the prefix at or below it must never
     * reach the delegate — X's own earlier stamps told the world that history was in its past, so
     * delivering it now into surviving state would re-enter skipped causal past — while records
     * above the seed deliver normally.
     *
     * Asserts the seeded frontier equals the pre-restart claim, no delivery at or below it, and
     * genuine deliveries above it.
     */
    @Test
    void scopeGrowthSeedsAnAddedChannelFromTheCarriedClaimAndSkipsItsPrefix() {
        for (long seed = 7000; seed < 7000 + SEEDS; seed++) {
            ParsleyTopologySim sim = new ParsleyTopologySim(seed);
            sim.externalTopic("t1");
            sim.node("P", Set.of("t1"), List.of("t2"), 0.0);
            sim.node("X", Set.of("t2"), List.of("t3"), 0.8);
            sim.produceExternal("t1");
            sim.run(250);
            ParsleyTopologySim.SimNode x = sim.nodeNamed("X");
            long claimedBefore = x.channels.stamp().offsetFor(sim.topicId("t1"), 0);
            assertTrue(claimedBefore >= 0,
                    "[seed " + seed + "] vacuity guard: X must have come to claim t1 ancestry "
                            + "through P's carried clocks before the growth means anything");

            sim.restartWithInputs("X", Set.of("t2", "t1"));

            assertEquals(claimedBefore, x.channels.frontier().offsetFor(sim.topicId("t1"), 0),
                    "[seed " + seed + "] the added channel must seed at exactly the node's carried "
                            + "claim — never log-start (A5)");
            for (int i = 0; i < 5; i++) {
                sim.produceExternal("t1");
            }
            sim.run(150);
            List<Long> deliveredT1 = x.deliveredOffsetsByTopic.getOrDefault("t1", List.of());
            assertTrue(deliveredT1.stream().allMatch(offset -> offset > claimedBefore),
                    "[seed " + seed + "] no record at or below the carried claim " + claimedBefore
                            + " may reach the delegate — that prefix was skipped, and skipped causal "
                            + "past is never re-entered (delivered: " + deliveredT1 + ")");
            assertTrue(!deliveredT1.isEmpty(),
                    "[seed " + seed + "] vacuity guard: records above the seed must deliver "
                            + "normally — the skip is a prefix, not a dead channel");
        }
    }

    /**
     * A5's own-former-sink extension — "skip what you already claimed". Node X produces t2 and
     * later restarts with t2 added as an <em>input</em> (a cycle onto its own former sink). The
     * growth seed reads the ownOutputs-inclusive {@code stamp()}, so X must never be re-delivered
     * its own pre-restart outputs (their positions were already claimed by its stamps via
     * {@code ownOutputs}); outputs it produces after the restart flow back to it normally.
     *
     * Asserts nothing at or below the pre-restart acked position reaches the delegate, and the
     * post-restart cycle genuinely delivers newer own outputs.
     */
    @Test
    void scopeGrowthOntoAFormerOwnSinkSkipsTheAlreadyClaimedPrefix() {
        for (long seed = 8000; seed < 8000 + SEEDS; seed++) {
            ParsleyTopologySim sim = new ParsleyTopologySim(seed);
            sim.externalTopic("t1");
            // The output probability must stay subcritical: once t2 becomes an input too, each
            // delivered own record emits another with this probability — 1.0 would never quiesce.
            sim.node("X", Set.of("t1"), List.of("t2"), 0.6);
            sim.produceExternal("t1");
            sim.run(250);
            ParsleyTopologySim.SimNode x = sim.nodeNamed("X");
            long ackedBefore = x.channels.ownOutputs().offsetFor(sim.topicId("t2"), 0);
            assertTrue(ackedBefore >= 0,
                    "[seed " + seed + "] vacuity guard: X must have acked t2 outputs to skip");

            sim.restartWithInputs("X", Set.of("t1", "t2"));

            for (int i = 0; i < 5; i++) {
                sim.produceExternal("t1");
            }
            sim.run(150);
            List<Long> deliveredOwn = x.deliveredOffsetsByTopic.getOrDefault("t2", List.of());
            assertTrue(deliveredOwn.stream().allMatch(offset -> offset > ackedBefore),
                    "[seed " + seed + "] X must never be re-delivered its own already-claimed "
                            + "outputs at or below " + ackedBefore + " (delivered: " + deliveredOwn + ")");
            assertTrue(!deliveredOwn.isEmpty(),
                    "[seed " + seed + "] vacuity guard: the post-restart cycle must deliver X's "
                            + "newer own outputs back to it");
        }
    }

    /**
     * The held-record disposition rule at a scope shrink: a restart that removes an input while
     * records from that input are still held must fail init loudly (they can be neither delivered
     * — no registered source — nor silently discarded), and a restart with no such held records
     * proceeds cleanly with the A6 re-homing intact. Runs the random schedule WITHOUT the settle
     * drain, so seeds genuinely reach the restart with records still held.
     *
     * Asserts, per seed, whichever branch the interleaving produced — with a vacuity guard that
     * at least one seed exercised the loud-failure branch.
     */
    @Test
    void scopeShrinkWithHeldRecordsFromTheRemovedInputFailsInitLoudly() {
        int loudFailures = 0;
        for (long seed = 9000; seed < 9000 + SEEDS; seed++) {
            ParsleyTopologySim sim = new ParsleyTopologySim(seed);
            sim.externalTopic("t1");
            sim.node("A", Set.of("t1"), List.of("t2"), 0.8);
            sim.node("X", Set.of("t1", "t2"), List.of("t3"), 0.8);
            ParsleyTopologySim.SimNode x = sim.nodeNamed("X");
            // Walk the schedule in short bursts, stopping the moment X holds a t2 record — a
            // transient state a fixed-length run would usually sail past (holds resolve quickly).
            for (int burst = 0; burst < 50 && !x.heldSourceTopics().contains("t2"); burst++) {
                sim.runWithoutDrain(5);
            }

            if (x.heldSourceTopics().contains("t2")) {
                IllegalStateException thrown = assertThrows(IllegalStateException.class,
                        () -> sim.restartWithInputs("X", Set.of("t1")),
                        "[seed " + seed + "] a shrink that would orphan held t2 records must fail "
                                + "init loudly, never restore them undeliverable or drop them");
                assertTrue(thrown.getMessage().contains("t2"),
                        "[seed " + seed + "] the failure must name the removed topic: "
                                + thrown.getMessage());
                loudFailures++;
            } else {
                ParsleyVectorClock stampBefore = x.channels.stamp();
                sim.restartWithInputs("X", Set.of("t1"));
                assertTrue(x.channels.stamp().dominates(stampBefore),
                        "[seed " + seed + "] a legal shrink (no held t2 records) must keep the A6 "
                                + "re-homing: the post-shrink stamp dominates the pre-shrink stamp");
            }
        }
        assertTrue(loudFailures > 0,
                "vacuity guard: at least one seed must reach the shrink with held t2 records, or "
                        + "the disposition rule was never exercised");
    }
}
