package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The random-topology explorer: every run draws a generated topology
 * ({@link ParsleyTopologyGen}), a seeded schedule, and a fault profile, with the sim's continuous
 * invariants (I1 ground-truth delivery order, I2 both forms, I3, I9, D2, no duplicate delegate
 * delivery) armed throughout and drain-to-empty asserted at the end of every run. On a failure
 * the recorded trace is delta-debugged ({@link ParsleySimShrinker}) and the minimised repro is
 * printed in {@link ParsleySimTrace} text form, ready for {@code parse} + {@code replay}.
 *
 * <p>Scale is system-property-tunable so one test serves two tiers: the CI defaults keep it in
 * seconds; a deep sweep passes {@code -Dparsley.sim.topologies=5000 -Dparsley.sim.steps=2000}
 * (and, being seed-deterministic, explores the SAME population on every machine). The soak tier
 * is separate ({@code ParsleySimSoakTest}).
 *
 * <p>Vacuity is asserted at the population level, not per run: the sweep must have generated
 * every {@link ParsleyTopologyGen.Feature}, and the interesting counters must have fired
 * somewhere — otherwise a quietly-boring generator would green-light without testing anything.
 * Supercritical topologies (feedback amplification too hot, a configuration problem the runaway
 * guard classifies as such) are skipped and counted loudly, never silently.
 */
class ParsleyRandomTopologyPropertyTest {

    private static final int TOPOLOGIES = Integer.getInteger("parsley.sim.topologies", 80);
    private static final int STEPS = Integer.getInteger("parsley.sim.steps", 400);
    private static final int SEEDS_PER_TOPOLOGY = Integer.getInteger("parsley.sim.seeds", 2);
    private static final long TOPOLOGY_SEED_BASE = 41_000;
    private static final long RUN_SEED_BASE = 91_000;

    /**
     * The sweep: {@code TOPOLOGIES × SEEDS_PER_TOPOLOGY} runs, fault profiles rotating from
     * plain through restarts, crashes, and the full set (rollbacks + scope changes), each run
     * ending drained. Population-level vacuity guards close the test — see the class Javadoc.
     */
    @Test
    void generatedTopologiesUnderRandomSchedulesAndFaultsHoldEveryInvariantAndDrain() {
        EnumSet<ParsleyTopologyGen.Feature> seen = EnumSet.noneOf(ParsleyTopologyGen.Feature.class);
        int held = 0;
        int outOfOrder = 0;
        int nullsDelivered = 0;
        int deliveries = 0;
        int restarts = 0;
        int crashes = 0;
        int rollbacks = 0;
        int rescopes = 0;
        int stampedExternals = 0;
        int crossPartitionIgnores = 0;
        int supercriticalSkips = 0;
        for (int t = 0; t < TOPOLOGIES; t++) {
            ParsleySimTrace.SimSpec spec = ParsleyTopologyGen.generate(TOPOLOGY_SEED_BASE + t);
            seen.addAll(ParsleyTopologyGen.classify(spec));
            for (int s = 0; s < SEEDS_PER_TOPOLOGY; s++) {
                long runSeed = RUN_SEED_BASE + (long) t * SEEDS_PER_TOPOLOGY + s;
                ParsleyTopologySim sim = withProfile(ParsleyTopologySim.fromSpec(spec, runSeed),
                        t * SEEDS_PER_TOPOLOGY + s);
                if (spec.partitions() > 1) {
                    // The cross-partition claim source (T10): only multi-partition specs have a
                    // partitioner with work to do, so the flag never perturbs legacy schedules.
                    sim.withEdgeProducers();
                }
                try {
                    sim.run(STEPS);
                    assertDrained(sim, spec, "[seed " + runSeed + "] ");
                } catch (ParsleyTopologySim.SupercriticalTopologyException hot) {
                    supercriticalSkips++;
                    continue;
                } catch (Throwable failure) {
                    throw shrunkRepro(spec, sim.trace(), runSeed, failure);
                }
                held += sim.recordsHeld;
                outOfOrder += sim.outOfOrderDeliveries;
                nullsDelivered += sim.nullMessagesDelivered;
                deliveries += sim.businessDeliveries;
                restarts += sim.restarts;
                crashes += sim.crashes;
                rollbacks += sim.rollbacks;
                rescopes += sim.rescopes;
                stampedExternals += sim.stampedExternals;
                crossPartitionIgnores += sim.crossPartitionIgnoredCauses;
            }
        }

        // Skips are reported, never silent — and must stay the exception, not the population.
        assertTrue(supercriticalSkips <= TOPOLOGIES * SEEDS_PER_TOPOLOGY / 10,
                "supercritical skips must stay rare or the sweep is quietly not testing: skipped "
                        + supercriticalSkips + " of " + TOPOLOGIES * SEEDS_PER_TOPOLOGY + " runs");
        for (ParsleyTopologyGen.Feature feature : ParsleyTopologyGen.Feature.values()) {
            assertTrue(seen.contains(feature),
                    "population vacuity guard: the sweep never generated a " + feature
                            + " topology — grow the sweep or fix the generator");
        }
        assertTrue(deliveries > 0, "vacuity guard: the sweep must genuinely deliver records");
        assertTrue(held > 0, "vacuity guard: the sweep must genuinely hold records (the gate must run)");
        assertTrue(outOfOrder > 0, "vacuity guard: the sweep must deliver above a contiguous-frontier gap");
        assertTrue(nullsDelivered > 0, "vacuity guard: the sweep must exercise carried-clock custody");
        assertTrue(restarts > 0, "vacuity guard: the sweep must include clean restarts");
        assertTrue(crashes > 0, "vacuity guard: the sweep must include dirty restarts");
        assertTrue(rollbacks > 0, "vacuity guard: the sweep must include redelivery rollbacks");
        assertTrue(rescopes > 0, "vacuity guard: the sweep must include scope-change restarts");
        assertTrue(stampedExternals > 0,
                "vacuity guard: the sweep must produce stamped externals — the cross-partition "
                        + "claim source (T10) never ran");
        assertTrue(crossPartitionIgnores > 0,
                "vacuity guard: no delivery ever carried a foreign-partition cause — the gate's "
                        + "partition dimension (ignore branch) was never genuinely exercised");
    }

    /** Fault profiles rotate deterministically across runs, plain through everything-at-once. */
    private static ParsleyTopologySim withProfile(ParsleyTopologySim sim, int runIndex) {
        return switch (runIndex % 4) {
            case 0 -> sim;
            case 1 -> sim.withRestarts();
            case 2 -> sim.withRestarts().withCrashes();
            default -> sim.withRestarts().withCrashes().withRollbacks().withScopeChanges();
        };
    }

    /**
     * Liveness at the end of a drained run: no node still holds a buffered record — a record
     * held forever means a stamped claim nothing appended can satisfy.
     */
    private static void assertDrained(ParsleyTopologySim sim, ParsleySimTrace.SimSpec spec, String label) {
        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            assertEquals(0, sim.nodeNamed(node.name()).bufferSize(),
                    label + "node " + node.name() + " must end fully drained");
        }
    }

    /**
     * The failure path: shrink the recorded schedule against a replay oracle (same end check),
     * and raise an error carrying the minimised repro text. Shrinking is best-effort — if it
     * cannot run (e.g. the failure does not reproduce under replay, which the fidelity suite
     * makes a bug in itself), the full trace is printed instead.
     */
    private static AssertionError shrunkRepro(ParsleySimTrace.SimSpec spec,
                                              List<ParsleySimTrace.Action> trace,
                                              long runSeed, Throwable failure) {
        ParsleySimShrinker.Oracle oracle = candidate -> {
            try {
                ParsleyTopologySim replayed = ParsleyTopologySim.replay(spec, candidate);
                // Settle before asserting liveness: a truncated candidate trivially leaves
                // records buffered, which is not the defect — "cannot drain even when fully
                // polled" is. The replay-mode drain forwards no business records (no recorded
                // decisions remain), which is conservative in the right direction: releases come
                // from polling existing records, never from appending new ones.
                replayed.drain();
                assertDrained(replayed, spec, "[replay] ");
                return null;
            } catch (Throwable replayFailure) {
                return ParsleySimShrinker.signatureOf(replayFailure);
            }
        };
        String repro;
        try {
            ParsleySimShrinker.Shrunk shrunk = ParsleySimShrinker.shrink(trace, oracle);
            repro = "minimised to " + shrunk.trace().size() + " of " + trace.size() + " actions ("
                    + shrunk.oracleRuns() + " oracle runs):\n"
                    + ParsleySimTrace.print(spec, shrunk.trace());
        } catch (RuntimeException shrinkFailed) {
            repro = "shrinking unavailable (" + shrinkFailed.getMessage() + "); full trace:\n"
                    + ParsleySimTrace.print(spec, trace);
        }
        return new AssertionError("[seed " + runSeed + "] random-topology sweep failure: "
                + failure.getMessage() + "\nrepro (ParsleySimTrace.parse → ParsleyTopologySim.replay):\n"
                + repro, failure);
    }
}
