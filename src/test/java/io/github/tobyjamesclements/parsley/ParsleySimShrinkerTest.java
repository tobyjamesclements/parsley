package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The delta-debugging machinery of {@link ParsleySimShrinker}, exercised against synthetic
 * oracles (pure predicates over the action list — no sim involved), so minimality claims are
 * checkable exactly. The end-to-end pairing with real replays lives in the random-topology
 * sweep's failure path; these tests pin the algorithm itself.
 */
class ParsleySimShrinkerTest {

    private static final ParsleySimTrace.Action TRIGGER = new ParsleySimTrace.ProduceExternal("c1");
    private static final ParsleySimTrace.Action VICTIM = new ParsleySimTrace.Crash("X");

    /** Noise actions the oracles below never look at. */
    private static List<ParsleySimTrace.Action> noise(int count) {
        List<ParsleySimTrace.Action> actions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            actions.add(new ParsleySimTrace.AckAll("A"));
        }
        return actions;
    }

    /**
     * ddmin reduces a 40-action trace to exactly the two interacting actions when the failure
     * needs a trigger followed by a victim — the canonical two-event counterexample a human
     * actually wants to read.
     */
    @Test
    void shrinkReducesToTheMinimalFailingPair() {
        List<ParsleySimTrace.Action> trace = new ArrayList<>(noise(15));
        trace.add(TRIGGER);
        trace.addAll(noise(10));
        trace.add(VICTIM);
        trace.addAll(noise(13));
        ParsleySimShrinker.Oracle oracle = candidate -> {
            int trigger = candidate.indexOf(TRIGGER);
            int victim = candidate.lastIndexOf(VICTIM);
            return trigger >= 0 && victim > trigger ? "boom" : null;
        };

        ParsleySimShrinker.Shrunk shrunk = ParsleySimShrinker.shrink(trace, oracle);

        assertEquals(List.of(TRIGGER, VICTIM), shrunk.trace(),
                "the shrunken trace must be exactly the trigger followed by the victim");
        assertEquals("boom", shrunk.signature(), "the target signature must be preserved");
    }

    /**
     * The prefix pass alone handles the common case — a failure raised at one action — by
     * discarding the entire tail after it, and ddmin then strips the leading noise: a
     * single-action counterexample comes back as a single action.
     */
    @Test
    void shrinkReducesASingleActionFailureToThatAction() {
        List<ParsleySimTrace.Action> trace = new ArrayList<>(noise(10));
        trace.add(VICTIM);
        trace.addAll(noise(40));
        ParsleySimShrinker.Oracle oracle =
                candidate -> candidate.contains(VICTIM) ? "boom" : null;

        ParsleySimShrinker.Shrunk shrunk = ParsleySimShrinker.shrink(trace, oracle);

        assertEquals(List.of(VICTIM), shrunk.trace(),
                "a failure caused by one action must shrink to exactly that action");
    }

    /**
     * A candidate that fails with a DIFFERENT signature is rejected: removing the ack here makes
     * the oracle fail differently, so the shrinker must keep it — the minimised trace still
     * fails the original way, never a new way.
     */
    @Test
    void shrinkRejectsCandidatesThatFailWithADifferentSignature() {
        ParsleySimTrace.Action ack = new ParsleySimTrace.AckAll("A");
        List<ParsleySimTrace.Action> trace = new ArrayList<>(noise(5));
        trace.add(ack);
        trace.addAll(List.of(new ParsleySimTrace.ProduceExternal("c9"), VICTIM));
        ParsleySimShrinker.Oracle oracle = candidate -> {
            if (!candidate.contains(VICTIM)) {
                return null;
            }
            return candidate.contains(ack) ? "boom" : "other";
        };

        ParsleySimShrinker.Shrunk shrunk = ParsleySimShrinker.shrink(trace, oracle);

        assertEquals("boom", shrunk.signature(), "the original signature must be preserved");
        assertTrue(shrunk.trace().contains(ack),
                "the ack is load-bearing for the original signature and must survive");
        assertTrue(shrunk.trace().contains(VICTIM), "the victim must survive");
        assertEquals(2, shrunk.trace().size(),
                "everything not load-bearing for the original signature must be removed");
    }

    /**
     * Signature normalisation: two failures of the same shape thrown under different seeds, with
     * different offsets and counts in the message, share one signature — otherwise a shrunken
     * schedule (whose offsets shift) could never match its own failure.
     */
    @Test
    void signatureNormalisationStripsSeedLabelsAndDigits() {
        AssertionError first = new AssertionError(
                "[seed 4711] B: delivered c2@17 before its consumed cause c1@3");
        AssertionError second = new AssertionError(
                "[replay] B: delivered c2@5 before its consumed cause c1@1");
        assertEquals(ParsleySimShrinker.signatureOf(first), ParsleySimShrinker.signatureOf(second),
                "seed labels and shifted coordinates must not split one defect into two signatures");
    }
}
