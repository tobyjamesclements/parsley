package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The soak tier of the sim explorer: one long run — default 500k scheduler steps, segmented —
 * with every fault mode armed, hunting the failure class the per-run tiers cannot see: slow
 * state leaks and stalls. Off by default (it runs minutes); enable with
 * {@code -Dparsley.sim.soak=true}, scale with {@code -Dparsley.sim.soak.steps=N}.
 *
 * <p>Trace recording is disabled ({@code withNoTrace()}) — half a million actions would dominate
 * the heap, and a soak failure reproduces from its seed. Ground-truth history tracking is also
 * disabled ({@code withoutGroundTruthHistories()}): its per-event cost grows with the causal
 * past, which is affordable at the CI and deep tiers' run lengths where those exhaustive
 * invariants live, and quadratic here; the soak keeps every bounded-cost check armed.
 *
 * <p>Each segment ends with a settle drain and three checks: every buffer empty (no record held
 * across segments — a leak in the buffer or candidate index would surface here), the external
 * log strictly growing (the run is genuinely progressing, not idling), and the runaway guard's
 * bound on log growth (a supercritical or relay-loop regression aborts the segment loudly).
 */
class ParsleySimSoakTest {

    private static final int TOTAL_STEPS = Integer.getInteger("parsley.sim.soak.steps", 500_000);
    private static final int SEGMENT_STEPS = 5_000;

    /** The standing six-node topology plus a self-loop — every structural feature in one spec. */
    private static ParsleySimTrace.SimSpec soakSpec() {
        // parsley.sim.soak.partitions widens the soak to the T10 multi-partition dimension
        // (per-partition tasks + stamped-external cross-partition claims); the default keeps the
        // historical single-partition spec so existing soak seeds stay replayable.
        return new ParsleySimTrace.SimSpec(List.of("c1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c1"), List.of("c2"), 0.4),
                new ParsleySimTrace.SimSpec.NodeSpec("D", List.of("c1"), List.of("c2"), 0.4),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1", "c2", "c3"), List.of("c3"), 0.4),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("c1", "c2", "c3"), List.of("c4"), 0.4),
                new ParsleySimTrace.SimSpec.NodeSpec("W", List.of("c1", "c2", "c3", "c4"), List.of("c5"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("N", List.of("c5"), List.of("c6"), 0.0)),
                Integer.getInteger("parsley.sim.soak.partitions", 1));
    }

    /**
     * Segments of random scheduling under all fault modes, each ending settled: buffers empty,
     * frontier advanced. A stall (frontier stuck), a leak (buffer non-empty after drain), or a
     * runaway (guard) fails with the segment number and seed; rerunning the single seed at the
     * failing segment count reproduces it.
     */
    @Test
    void longRunUnderAllFaultModesStaysBoundedAndProgresses() {
        assumeTrue(Boolean.getBoolean("parsley.sim.soak"),
                "soak tier is opt-in: -Dparsley.sim.soak=true");
        long seed = Long.getLong("parsley.sim.soak.seed", 71_000L);
        ParsleySimTrace.SimSpec spec = soakSpec();
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, seed).withNoTrace()
                .withoutGroundTruthHistories()
                .withRestarts().withCrashes().withRollbacks().withScopeChanges();
        if (spec.partitions() > 1) {
            sim.withEdgeProducers();
        }
        long lastFrontier = -1;
        int segments = TOTAL_STEPS / SEGMENT_STEPS;
        for (int segment = 0; segment < segments; segment++) {
            sim.runWithoutDrain(SEGMENT_STEPS);
            sim.drain();
            for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
                assertEquals(0, sim.nodeNamed(node.name()).bufferSize(),
                        "[seed " + seed + " segment " + segment + "] node " + node.name()
                                + " must be empty after the segment drain — a held record "
                                + "surviving a full settle is a leak or a stall");
            }
            // Progress witness robust to scope changes (a shrink legitimately retires a node's
            // c1 channel, re-homing its value into carried ancestry — frontier() then reads -1):
            // the external log itself must grow every segment (the scheduler produces ~1 in 10
            // steps), which together with the settled drain above means the system keeps
            // ingesting whatever it is currently scoped to.
            long produced = sim.logSize("c1");
            assertTrue(produced > lastFrontier,
                    "[seed " + seed + " segment " + segment + "] the external c1 log must grow "
                            + "every segment (was " + lastFrontier + ", still " + produced + ")");
            lastFrontier = produced;
        }
        assertTrue(sim.businessDeliveries > 0 && sim.crashes > 0 && sim.rollbacks > 0
                        && sim.rescopes > 0,
                "vacuity guard: the soak must genuinely deliver and exercise every fault mode "
                        + "(deliveries=" + sim.businessDeliveries + " crashes=" + sim.crashes
                        + " rollbacks=" + sim.rollbacks + " rescopes=" + sim.rescopes + ")");
    }
}
