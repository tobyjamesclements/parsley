package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The load-bearing guarantee under the whole shrinking story ({@link ParsleySimTrace},
 * {@link ParsleySimShrinker}): a schedule recorded by a generate-mode {@link ParsleyTopologySim}
 * run, replayed against a fresh sim built from the same spec, reproduces the run exactly —
 * PRNG-free. Equality is {@link ParsleyTopologySim#stateDigest()}: logs with stamps and causal
 * histories, per-node protocol + ground-truth state, and every counter. If these tests hold,
 * a shrinker counterexample is a faithful repro, not an artifact of replay drift.
 */
class ParsleySimReplayFidelityTest {

    /** The standing six-node topology of {@link ParsleyStampInvariantPropertyTest}, by value. */
    private static ParsleySimTrace.SimSpec standingSpec() {
        return new ParsleySimTrace.SimSpec(List.of("t1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("t1"), List.of("t2"), 0.7),
                new ParsleySimTrace.SimSpec.NodeSpec("D", List.of("t1"), List.of("t2"), 0.7),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("t1", "t2", "t3"), List.of("t3"), 0.6),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("t1", "t2", "t3"), List.of("t4"), 0.7),
                new ParsleySimTrace.SimSpec.NodeSpec("W", List.of("t1", "t2", "t3", "t4"), List.of("t5"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("N", List.of("t5"), List.of("t6"), 0.0)));
    }

    /**
     * A recorded generate-mode schedule (restarts on) replays to a byte-identical state digest on
     * a fresh sim — the recorded Poll emissions substitute exactly for the PRNG draws. Ten seeds;
     * a vacuity guard proves the sweep genuinely delivered, emitted, and restarted, so the
     * digests being compared describe non-trivial runs.
     */
    @Test
    void replayOfARecordedTraceReproducesTheExactFinalState() {
        int deliveries = 0;
        int restarts = 0;
        for (long seed = 100; seed < 110; seed++) {
            ParsleyTopologySim generated = ParsleyTopologySim.fromSpec(standingSpec(), seed).withRestarts();
            generated.run(300);
            ParsleyTopologySim replayed = ParsleyTopologySim.replay(standingSpec(), generated.trace());
            assertEquals(generated.stateDigest(), replayed.stateDigest(),
                    "[seed " + seed + "] a full-trace replay must reproduce the generate-mode run "
                            + "exactly — any drift makes shrunken repros untrustworthy");
            deliveries += generated.businessDeliveries;
            restarts += generated.restarts;
        }
        assertTrue(deliveries > 0, "vacuity guard: the sweep must genuinely deliver records");
        assertTrue(restarts > 0, "vacuity guard: the sweep must genuinely restart nodes");
    }

    /**
     * Fidelity holds with every fault mode armed at once — crashes, rollbacks, and scope-change
     * restarts are recorded and replayed like any other action. Vacuity guards prove each fault
     * genuinely fired somewhere in the sweep (rollbacks especially: a rewound cursor must
     * re-poll, and the replays must be absorbed, for the digest equality to mean anything).
     */
    @Test
    void replayReproducesFaultModeRunsExactly() {
        int crashes = 0;
        int rollbacks = 0;
        int rescopes = 0;
        for (long seed = 200; seed < 212; seed++) {
            ParsleyTopologySim generated = ParsleyTopologySim.fromSpec(standingSpec(), seed)
                    .withRestarts().withCrashes().withRollbacks().withScopeChanges();
            generated.run(300);
            ParsleyTopologySim replayed = ParsleyTopologySim.replay(standingSpec(), generated.trace());
            assertEquals(generated.stateDigest(), replayed.stateDigest(),
                    "[seed " + seed + "] a fault-mode trace must replay to the identical digest");
            crashes += generated.crashes;
            rollbacks += generated.rollbacks;
            rescopes += generated.rescopes;
        }
        assertTrue(crashes > 0, "vacuity guard: the sweep must include at least one dirty restart");
        assertTrue(rollbacks > 0, "vacuity guard: the sweep must include at least one cursor rollback");
        assertTrue(rescopes > 0, "vacuity guard: the sweep must include at least one scope change");
    }

    /**
     * Replay is itself deterministic: replaying the same trace twice produces the same digest.
     * Together with the generate/replay equality above this pins the whole pipeline as a pure
     * function of (spec, trace).
     */
    @Test
    void replayingTheSameTraceTwiceIsDeterministic() {
        ParsleyTopologySim generated = ParsleyTopologySim.fromSpec(standingSpec(), 42).withRestarts();
        generated.run(300);
        List<ParsleySimTrace.Action> trace = generated.trace();
        assertEquals(ParsleyTopologySim.replay(standingSpec(), trace).stateDigest(),
                ParsleyTopologySim.replay(standingSpec(), trace).stateDigest(),
                "two replays of one trace must agree byte-for-byte");
    }

    /**
     * The text format round-trips: print(spec, trace) parses back to the identical spec and
     * action list, so a repro pasted from a failure message replays the same schedule. The trace
     * includes polls with emissions, acks, external produces, and rescopes (restarts on), which
     * covers every action shape the printer must encode.
     */
    @Test
    void specAndTraceRoundTripThroughPrintAndParse() {
        ParsleyTopologySim generated = ParsleyTopologySim.fromSpec(standingSpec(), 7)
                .withRestarts().withCrashes().withRollbacks().withScopeChanges();
        generated.run(250);
        String text = ParsleySimTrace.print(standingSpec(), generated.trace());
        ParsleySimTrace.Repro parsed = ParsleySimTrace.parse(text);
        assertEquals(standingSpec(), parsed.spec(), "the printed spec must parse back identically");
        assertEquals(generated.trace(), parsed.trace(), "the printed trace must parse back identically");
        assertEquals(generated.stateDigest(),
                ParsleyTopologySim.replay(parsed.spec(), parsed.trace()).stateDigest(),
                "a parsed repro must replay to the original run's exact state");
    }
}
