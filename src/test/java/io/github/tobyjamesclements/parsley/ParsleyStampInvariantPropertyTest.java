package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The property tests for stamp transitive completeness, per-producer stamp
 * monotonicity, and unconditional merge (stamps carry unconsumed-channel ancestry): the
 * certification the gate's unconditional ignore branch leans on. Every
 * invariant is asserted <em>continuously inside</em> {@link ParsleyTopologySim} — at every
 * emission and every delivery, under seeded-random interleavings — so each test here is one
 * topology + a seed sweep + vacuity guards proving the interesting paths (holds, out-of-order
 * deliveries above gaps, carried-clock folds, restarts) genuinely ran. A failure message always
 * carries the seed; re-running the single seed reproduces the schedule deterministically.
 *
 * <p>The standing topology (single-partition topics c1..c6):
 * <pre>
 *   external → c1 (clockless producer: causally minimal by definition)
 *   A {c1} → c2          D {c1} → c2      (two producers sharing sink c2: interleaved stamps on
 *                                          one partition are how out-of-order delivery above a
 *                                          contiguous-frontier gap arises downstream)
 *   B {c1,c2,c3} → c3                     (self-consuming cycle: reflected own-sink claims through
 *                                          the consumed branch, relay quiescence)
 *   C {c1,c2,c3} → c4
 *   W {c1,c2,c3,c4} → c5 (silent delegate: c5 carries only null messages)
 *   N {c5} → c6          (consumes only null messages — its stamps must still carry c1..c5
 *                          ancestry it never consumed: the custody chain, gate-free)
 * </pre>
 *
 * <p>The chain topology (see {@link #differingScopeChain}) adds business consumers whose scopes do
 * <em>not</em> cover the coordinates their records' clocks claim — each hop consumes only its
 * predecessor's sink, so upstream claims land in the gate's ignore branch at every node, and
 * the same continuous properties then certify the ignore branch end to end.
 */
class ParsleyStampInvariantPropertyTest {

    private static final int SEEDS = 20;
    private static final int STEPS = 400;

    private static ParsleyTopologySim standingTopology(long seed) {
        ParsleyTopologySim sim = new ParsleyTopologySim(seed);
        sim.externalTopic("c1");
        sim.node("A", Set.of("c1"), List.of("c2"), 0.7);
        sim.node("D", Set.of("c1"), List.of("c2"), 0.7);
        sim.node("B", Set.of("c1", "c2", "c3"), List.of("c3"), 0.6);
        sim.node("C", Set.of("c1", "c2", "c3"), List.of("c4"), 0.7);
        sim.node("W", Set.of("c1", "c2", "c3", "c4"), List.of("c5"), 0.0);
        sim.node("N", Set.of("c5"), List.of("c6"), 0.0);
        return sim;
    }

    /**
     * Stamp transitive completeness, both forms, plus the ground-truth causal delivery order,
     * across random interleavings:
     * at every emission the stamp dominates (a) the dependency clocks and coordinates of every
     * event the node has delivered and (b) the node's exact delegate-visible causal past; at every
     * delivery, every consumed cause in the record's true history was already delivered locally.
     * All asserted inside the sim on every event — this test sweeps seeds and then proves the runs
     * were not vacuous: records were genuinely held and released, and records were genuinely
     * delivered out of order above contiguous-frontier gaps (the {@code highestDelivered} path).
     *
     * Asserts every seed completes with empty hold-back queues (liveness: everything eventually
     * delivered) and that holds, gaps, and null-message folds all occurred across the sweep.
     */
    @Test
    void stampsDominateDeliveredDependenciesAndGroundTruthAcrossRandomInterleavings() {
        int held = 0;
        int outOfOrder = 0;
        int folded = 0;
        for (long seed = 1000; seed < 1000 + SEEDS; seed++) {
            ParsleyTopologySim sim = standingTopology(seed);
            sim.run(STEPS);
            for (String node : List.of("A", "D", "B", "C", "W", "N")) {
                assertEquals(0, sim.nodeNamed(node).bufferSize(),
                        "[seed " + seed + "] node " + node + " must end fully drained — a record "
                                + "held forever means a stamped claim nothing appended can satisfy");
            }
            held += sim.recordsHeld;
            outOfOrder += sim.outOfOrderDeliveries;
            folded += sim.nullMessagesDelivered;
        }
        assertTrue(held > 0,
                "vacuity guard: no seed ever held a record — the gate was never genuinely exercised");
        assertTrue(outOfOrder > 0,
                "vacuity guard: no record was ever delivered above a contiguous-frontier gap — the "
                        + "shared-sink interleaving must produce non-head-of-line deliveries");
        assertTrue(folded > 0,
                "vacuity guard: no null message was ever delivered — carried-clock custody was "
                        + "never genuinely exercised");
    }

    /**
     * Per-producer stamp monotonicity across random interleavings: a node's successive stamps are
     * vector-monotone (asserted in
     * the sim at every single emission), which is what makes non-head-of-line delivery preserve
     * FIFO-per-producer downstream — if an earlier record from a producer is held, every later one
     * is held too. The shared sink c2 is the load-bearing case: A's and D's interleaved records on
     * one partition each stay individually monotone, which is exactly what lets B deliver one
     * producer's record above the other's held one without reordering either producer's sequence.
     *
     * Asserts the sweep genuinely emitted from both c2 producers in every seed.
     */
    @Test
    void successiveStampsArePerProducerMonotoneAcrossRandomInterleavings() {
        for (long seed = 2000; seed < 2000 + SEEDS; seed++) {
            ParsleyTopologySim sim = standingTopology(seed);
            sim.run(STEPS);
            assertTrue(sim.nodeNamed("A").task(0).ownBusinessSends.size() + sim.nodeNamed("D").task(0).ownBusinessSends.size() > 0,
                    "[seed " + seed + "] vacuity guard: the shared sink c2 must see business "
                            + "emissions for the per-producer monotonicity to be exercised");
        }
    }

    /**
     * The transitive-chain scenario: the origin record c1@0's coordinate must be claimed directly
     * by the stamp of every node whose true causal past contains it, however many hops from c1 it
     * sits — the custody chain that makes the gate's ignore branch sound (a consumed ancestor is
     * always claimed <em>in the record's own clock</em>, never only via intermediaries).
     *
     * Asserts, per seed, that the chain genuinely propagated (C's ground-truth past contains the
     * origin) and that C's final stamp claims the origin coordinate.
     */
    @Test
    void transitiveChainClaimsTheOriginCoordinateAtEveryHop() {
        for (long seed = 3000; seed < 3000 + SEEDS; seed++) {
            ParsleyTopologySim sim = standingTopology(seed);
            sim.produceExternal("c1"); // the tagged origin: c1@0, before any random scheduling
            sim.run(STEPS);
            ParsleyTopologySim.SimCoord origin = new ParsleyTopologySim.SimCoord(sim.topicId("c1"), 0, 0);
            ParsleyTopologySim.SimTask c = sim.nodeNamed("C").task(0);
            assertTrue(c.truePast.contains(origin),
                    "[seed " + seed + "] vacuity guard: the origin c1@0 must reach C's causal past "
                            + "through the chain (drain() delivers all backlog)");
            assertTrue(c.channels.stamp().offsetFor(sim.topicId("c1"), 0) >= 0,
                    "[seed " + seed + "] C's stamp must claim the origin coordinate c1@0 directly "
                            + "(transitively complete, unconditionally merged)");
        }
    }

    /**
     * The gate-free custody path: node N consumes only c5 — null messages from W — yet its
     * outbound stamps must carry the c1..c4 ancestry those carried clocks name, none of which N
     * consumes. This is the unconditional merge working across differing consumption sets, the
     * null-message half of the same custody chain the two-branch gate relies on for business
     * records.
     *
     * Asserts N genuinely folded carried clocks, never consumed c1, and still stamps a c1 claim.
     */
    @Test
    void unconsumedChannelAncestryIsCarriedThroughNullMessageCustody() {
        for (long seed = 4000; seed < 4000 + SEEDS; seed++) {
            ParsleyTopologySim sim = standingTopology(seed);
            sim.produceExternal("c1");
            sim.run(STEPS);
            ParsleyTopologySim.SimTask n = sim.nodeNamed("N").task(0);
            assertTrue(n.carriedClocksFolded > 0,
                    "[seed " + seed + "] vacuity guard: N must have folded at least one carried clock");
            assertTrue(!n.node.inputs.contains("c1"),
                    "topology self-check: N must not consume c1 for this property to mean anything");
            assertTrue(n.channels.stamp().offsetFor(sim.topicId("c1"), 0) >= 0,
                    "[seed " + seed + "] N's stamp must claim c1 ancestry it learned only from "
                            + "carried clocks on c5 — the merge may never strip unconsumed channels");
        }
    }

    /**
     * The differing-scope chain: no consumer past A consumes c1, so every record's clock
     * claims coordinates its receiver has no channel for — the shape the ignore branch exists to
     * handle. K's two consumed channels keep the consumed branch genuinely exercised
     * beside the ignore branch: a c3 record (from A2, whose stamps claim c2 ancestry) can arrive
     * at K before its c2 cause, so K holds it — while the same clock's c1 claims are ignored. L
     * consumes only c7 and ignores everything upstream (claims on c1, c2, c3 alike).
     */
    private static ParsleyTopologySim differingScopeChain(long seed) {
        ParsleyTopologySim sim = new ParsleyTopologySim(seed);
        sim.externalTopic("c1");
        sim.node("A", Set.of("c1"), List.of("c2"), 0.8);
        sim.node("A2", Set.of("c2"), List.of("c3"), 0.8);
        sim.node("K", Set.of("c2", "c3"), List.of("c7"), 0.8);
        sim.node("L", Set.of("c7"), List.of("c8"), 0.8);
        return sim;
    }

    /**
     * The two-branch gate's ignore branch under the full property sweep: a chain whose
     * business consumers do not consume their ancestors' topics. Every upstream claim a record
     * carries lands in the ignore branch at its consumer, yet all of the sim's continuous
     * invariants — stamp dominance in both forms, stamp monotonicity, the custody chain, and the
     * ground-truth causal delivery
     * order — must hold exactly as in a fully-covering topology, and every seed must still drain
     * to empty hold-back queues (an ignore that wrongly gated would strand records; one that
     * wrongly satisfied a consumed dependency would break the order check at K).
     *
     * Asserts every seed drains, the origin c1@0 genuinely traverses the chain to L in the sweep,
     * L's stamp then claims the origin it never consumed (custody through ignoring consumers), and
     * records were genuinely held at K (the consumed branch ran beside the ignore branch).
     */
    @Test
    void differingScopeChainCertifiesTheIgnoreBranchEndToEnd() {
        int held = 0;
        int originsReachedL = 0;
        for (long seed = 9000; seed < 9000 + SEEDS; seed++) {
            ParsleyTopologySim sim = differingScopeChain(seed);
            sim.produceExternal("c1"); // the tagged origin: c1@0, before any random scheduling
            sim.run(STEPS);
            for (String node : List.of("A", "A2", "K", "L")) {
                assertEquals(0, sim.nodeNamed(node).bufferSize(),
                        "[seed " + seed + "] node " + node + " must end fully drained — an ignored "
                                + "coordinate must never strand a record");
            }
            ParsleyTopologySim.SimCoord origin = new ParsleyTopologySim.SimCoord(sim.topicId("c1"), 0, 0);
            ParsleyTopologySim.SimTask l = sim.nodeNamed("L").task(0);
            if (l.truePast.contains(origin)) {
                originsReachedL++;
                assertTrue(l.channels.stamp().offsetFor(sim.topicId("c1"), 0) >= 0,
                        "[seed " + seed + "] L's stamp must claim the origin c1@0 it never consumed "
                                + "— the custody chain through ignoring consumers");
            }
            held += sim.recordsHeld;
        }
        assertTrue(originsReachedL > 0,
                "vacuity guard: the origin must traverse the full differing-scope chain to L in at "
                        + "least one seed for the custody property to mean anything");
        assertTrue(held > 0,
                "vacuity guard: the sweep must genuinely hold records at K — the consumed branch "
                        + "has to be exercised alongside the ignore branch");
    }

    /**
     * All of the above under in-place restarts: nodes are torn down mid-schedule and rebuilt from
     * their durable stores (the {@code "frontier"} value, buffer, candidate and forwarded-index stores,
     * the sink end-offset {@code ownOutputs} seed, and the forwarded-index reconstruction of
     * {@code highestDelivered}), and every continuous invariant — including stamp monotonicity
     * <em>across</em> the restart boundary — must hold exactly as in an uninterrupted run.
     *
     * Asserts restarts genuinely occurred and every seed still drains to empty hold-back queues.
     */
    @Test
    void invariantsHoldAcrossInPlaceRestartsFromDurableState() {
        int restarts = 0;
        for (long seed = 5000; seed < 5000 + SEEDS; seed++) {
            ParsleyTopologySim sim = standingTopology(seed).withRestarts();
            sim.run(STEPS);
            for (String node : List.of("A", "D", "B", "C", "W", "N")) {
                assertEquals(0, sim.nodeNamed(node).bufferSize(),
                        "[seed " + seed + "] node " + node + " must end fully drained after restarts");
            }
            restarts += sim.restarts;
        }
        assertTrue(restarts > 0, "vacuity guard: the sweep must include at least one in-place restart");
    }
}
