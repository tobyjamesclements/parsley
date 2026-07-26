package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sim's storm classifier, on a feedback cycle whose <em>business</em> amplification, not its
 * null-message relay, is what fails to settle. A topology whose consumers re-emit onto their own
 * upstream cycle behaves as a branching process: one record's expected offspring is the loop gain,
 * and above gain 1 a single external record echoes around the cycle multiplying every lap, so the
 * settle {@link ParsleyTopologySim#drain} never terminates. That is a configuration pathology (the
 * streaming equivalent of acoustic feedback), not a Parsley defect — every record is still
 * delivered in causal order — so the sim classifies it as a {@link
 * ParsleyTopologySim.SupercriticalTopologyException} the random-topology explorer skips and counts,
 * distinct from a relay loop (an {@link AssertionError}, a genuine non-quiescence). This pins
 * that classification: both the record-count guard and the drain wall-clock guard reach the same
 * verdict from the log's composition, so a tight run timeout can never relabel a supercritical
 * topology as a relay bug (issue #32).
 *
 * <p>The shape is minimal: p1 injects one record onto both cycle channels; p2 relays c2 to c3;
 * p3 re-emits c3 onto both c2 and c3 (the self-loop that pushes the loop gain above 1). The
 * relay itself is quiescent on shared-sink cycles — that is pinned separately in
 * {@link ParsleyGossipCycleQuiescenceTest#threeProducerSharedSinkChordedCycleQuiescesWithSilentDelegates}.
 */
class ParsleySupercriticalTopologyClassificationTest {

    private static ParsleySimTrace.SimSpec cycle(double businessOutputProbability) {
        double p = businessOutputProbability;
        return new ParsleySimTrace.SimSpec(List.of("c1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("p1", List.of("c1"), List.of("c2", "c3"), p),
                new ParsleySimTrace.SimSpec.NodeSpec("p2", List.of("c2"), List.of("c3"), p),
                new ParsleySimTrace.SimSpec.NodeSpec("p3", List.of("c3"), List.of("c2", "c3"), p)));
    }

    /**
     * Above the critical gain (delegates always forward, loop gain about 1.6) the drain cannot
     * settle, and the sim must classify the runaway as a supercritical topology — the skippable
     * configuration verdict — never as a non-quiescing relay. Ground-truth histories are off only
     * for speed (the record-count guard trips deterministically in well under a second); the
     * schedule and the emissions are identical either way.
     */
    @Test
    void supercriticalBusinessFeedbackIsClassifiedSkippableNotARelayBug() {
        ParsleyTopologySim sim =
                ParsleyTopologySim.fromSpec(cycle(1.0), 1L).withoutGroundTruthHistories();
        sim.produceExternal("c1");
        assertThrows(ParsleyTopologySim.SupercriticalTopologyException.class, sim::drain,
                "a loop-gain > 1 business feedback cycle must be classified supercritical (a "
                        + "configuration pathology the explorer skips), not a relay non-quiescence");
    }

    /**
     * The same wiring below the critical gain (delegates forward only sometimes, loop gain about
     * 0.65) settles to a bounded log — the proof that the storm above is amplification driven by
     * the loop gain, not a protocol loop, which would be independent of the forward probability
     * (and which the {@code p = 0.0} gossip-quiescence pins already rule out).
     */
    @Test
    void subcriticalBusinessFeedbackSettles() {
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(cycle(0.4), 1L);
        sim.produceExternal("c1");
        assertDoesNotThrow(sim::drain,
                "below the critical gain the same cycle must settle — the storm is amplification, "
                        + "not a p-independent protocol loop");
        assertTrue(sim.logSize("c2") + sim.logSize("c3") < 2_000,
                "a subcritical cycle settles to a bounded log, not an unbounded storm");
    }
}
