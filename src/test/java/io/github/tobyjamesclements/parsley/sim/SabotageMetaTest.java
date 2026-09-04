package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.sim.TargetedScenarioTest.Rig;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the suite catches each violation class.
 *
 * <p>Runs the harness against deliberately broken engines and asserts that the oracle fails.
 * A test that stays green against a broken engine is evidence of nothing, so this is what
 * makes the rest of the suite load-bearing.
 */
class SabotageMetaTest {
    /**
     * A seed the sweep catches IGNORE_RECREATION on under the current generator (D43's
     * rule). Assumption 2 is judged at the moment a process commits a step while receiving
     * a dead incarnation whose name is bound to a live other id — the same judgement the
     * host's identity report makes — rather than at the end of the run, where a process
     * that later failed closed for another reason, or whose fresh incarnation was itself
     * killed, used to be excused: 177 of 300 seeds catch the mode this way, against 5 of
     * 300 judged at the end (D115).
     */
    static final long RECREATION_SEED = 1;
    /** Half of the recreation catches measured over 300 seeds under the current generator (177). */
    static final long RECREATION_FLOOR = 88;
    /**
     * Half of the host-reset catches measured over 120 seeds under the current generator
     * (68: 42 through the Safety 8 obligation judged from world truth, the rest through the
     * delivery-time Safety 1 check alone).
     */
    static final long HOST_RESET_FLOOR = 34;
    /** A seed the sweep catches the host reset on through the Safety 8 obligation. */
    static final long HOST_RESET_SEED = 4;

    /** Delivering despite unsatisfied causes is caught. */
    @Test
    void deliveringDespiteCausesIsCaught() {
        Rig rig = TargetedScenarioTest.diamond(SabotageMode.IGNORE_CAUSES);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1")),
                () -> "expected a Safety 1 violation, got: " + violations);
    }

    /** Delivering past a blocked head is caught. */
    @Test
    void deliveringPastABlockedHeadIsCaught() {
        Rig rig = TargetedScenarioTest.fifoHold(SabotageMode.NO_FIFO);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertFalse(violations.isEmpty(), "delivering past a blocked head must be flagged");
    }

    /** Redelivering refed messages is caught. */
    @Test
    void redeliveringRefedMessagesIsCaught() {
        Rig rig = TargetedScenarioTest.rewindDedupe(SabotageMode.REDELIVER_REFEEDS);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 2")),
                () -> "expected a Safety 2 violation, got: " + violations);
    }

    /** Treating undecodable metadata as absent is caught. */
    @Test
    void treatingUndecodableMetadataAsAbsentIsCaught() {
        Rig rig = TargetedScenarioTest.undecodableMetadata(SabotageMode.UNDECODABLE_AS_ABSENT);
        SimProcess p = rig.proc("p");
        SimWorld.SimChannel c1 = rig.chans.get("c1");

        assertDoesNotThrow(() -> p.feedOne(c1));
        p.drain();
        p.commitStep();
        assertFalse(rig.uidsDelivered("p").isEmpty(), "sabotaged engine delivered despite undecodable metadata");
    }

    /** Dropping held causes of sends is caught. */
    @Test
    void droppingHeldCausesOfSendsIsCaught() {
        Rig rig = TargetedScenarioTest.receiptCausesReexpressed(SabotageMode.SKIP_RECEIPT_MERGE);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Structural 15")),
                () -> "expected a Structural 15 violation, got: " + violations);
    }

    /** Losing held messages across restart is caught. */
    @Test
    void losingHeldMessagesAcrossRestartIsCaught() {
        Rig rig = TargetedScenarioTest.heldSurvivesRestart(SabotageMode.DROP_HELD);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a liveness violation, got: " + violations);
    }

    /**
     * The composite blind spot is closed: a premature delivery whose cause first arrives
     * after a restart. The premature delivery advances the persisted delivered past over its
     * own cause, the restarted clamp then drops the late-arriving cause as a sanctioned
     * duplicate, and the end-of-run Safety 1 check compares delivered pairs only — so with
     * the cause never delivered, the violation is visible solely at delivery time.
     */
    @Test
    void prematureDeliveryWhoseCauseArrivesOnlyAfterRestartIsCaught() {
        Rig rig = TargetedScenarioTest.heldSurvivesRestart(SabotageMode.IGNORE_CAUSES);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1 (delivery-time)")),
                () -> "expected the delivery-time legality check to flag the premature delivery, got: "
                        + violations);
    }

    /**
     * A host that resets a read position past discarded positions — {@code auto.offset.reset=earliest}
     * where D9 pins {@code none} — is caught by the harness's Safety 8 obligation, judged
     * from world truth the host cannot launder: a committed record between where the process
     * first read the channel and where it committed reading to that was never fed to it. The
     * engine no longer checks retention (D115), so the fault is the host's: the simulated
     * host's fetch refusal is disarmed, the process reads on past the gap, and the scenario
     * flags the records it skipped — whether or not a message depending on one of them is
     * later delivered, which is the only shape the delivery-time Safety 1 check sees.
     */
    @Test
    void aHostResettingPastDiscardedPositionsIsCaught() {
        Rig rig = TargetedScenarioTest.truncation(SabotageMode.NONE);
        SimProcess p = rig.proc("p");
        p.hostFault(SimProcess.HostFault.RESET_PAST_LOG_START);
        assertDoesNotThrow(() -> p.feedOne(rig.chans.get("c1")),
                "the host fault must disarm the fetch refusal, or the assertion below tests nothing");

        List<String> violations = Scenario.run(HOST_RESET_SEED, SabotageMode.NONE,
                SimProcess.HostFault.RESET_PAST_LOG_START).violations();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 8")),
                () -> "seed " + HOST_RESET_SEED + " must catch the host sailing past truncation, got: " + violations);
    }

    /** Starting without a removed held channel is caught. */
    @Test
    void startingWithoutARemovedHeldChannelIsCaught() {
        Rig rig = TargetedScenarioTest.removeChannelWithHeld(SabotageMode.IGNORE_REMOVED_CHANNELS);

        assertDoesNotThrow(() -> rig.proc("p").start());
        Scenario.quiesce(List.of(rig.proc("p")));
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a stranded-message liveness violation, got: " + violations);
    }

    /** Silently dropping a fed message is caught. */
    @Test
    void silentlyDroppingAFedMessageIsCaught() {
        Rig rig = TargetedScenarioTest.fourOnOneChannel(SabotageMode.SILENT_DROP);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a fed-but-never-delivered liveness violation, got: " + violations);
    }

    /** Ignoring recreation is caught. */
    @Test
    void ignoringRecreationIsCaught() {
        Rig rig = TargetedScenarioTest.recreatedTopic(SabotageMode.IGNORE_RECREATION);

        assertDoesNotThrow(() -> rig.proc("p").reinitialise(),
                "the sabotage must disarm the refusal, or the oracle assertion below tests nothing");

        List<String> violations = Scenario.run(RECREATION_SEED, SabotageMode.IGNORE_RECREATION).violations();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Assumption 2")),
                () -> "seed " + RECREATION_SEED + " must catch the engine running across a recreation, got: "
                        + violations);
    }

    /**
     * Delivering past dead channel holds disarms the refusal. The oracle evidence for this
     * mode is deliberately not a random seed:
     * {@link #deliveringPastDeadChannelHoldsInvertsCausalOrderAndTheOracleSeesIt} constructs
     * the inversion, because no seed in 1..300 reaches it by chance — which is also why this
     * mode has no random-sweep floor below.
     */
    @Test
    void deliveringPastDeadChannelHoldsIsCaught() {
        Rig rig = TargetedScenarioTest.deadChannelWithHeldMessages(SabotageMode.DELIVER_PAST_DEAD_HOLDS);

        assertDoesNotThrow(() -> rig.proc("p").reinitialise());
    }

    /** Delivering past dead channel holds inverts causal order and the oracle sees it. */
    @Test
    void deliveringPastDeadChannelHoldsInvertsCausalOrderAndTheOracleSeesIt() {
        Rig rig = new Rig(SabotageMode.DELIVER_PAST_DEAD_HOLDS);

        SimWorld.SimChannel chanA = rig.channel("chanA");
        SimWorld.SimChannel chanB = rig.channel("chanB");
        SimWorld.SimChannel cq = chanA.id().compareTo(chanB.id()) < 0 ? chanA : chanB;
        SimWorld.SimChannel cx = cq == chanA ? chanB : chanA;
        SimWorld.SimChannel c9 = rig.channel("c9");
        SimWorld.SimChannel ct = rig.channel("ct");
        SimProcess q = rig.process("q", List.of(cx, ct), List.of(cq),
                d -> d.uid.equals("T") ? List.of(cq) : List.of());
        SimProcess p = rig.process("p", List.of(cx, c9, cq), List.of(), d -> List.of());

        rig.external(c9, "N0");
        Instance n1 = rig.external(c9, "N1");
        rig.externalCausedBy(cx, "X1", n1, n1.position);
        rig.external(ct, "T");

        q.feedOne(cx);
        q.drain();
        q.commitStep();

        p.feedOne(cx);
        p.drain();
        p.commitStep();

        rig.world.killChannel(cx);
        q.reinitialise();
        q.feedOne(ct);
        q.drain();
        q.commitStep();

        p.reinitialise();
        p.feedOne(cq);
        p.feedOne(c9);
        p.feedOne(c9);
        p.drain();
        p.commitStep();

        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1")),
                () -> "expected the lifetime causal-order inversion to be flagged, got: " + violations);
    }

    /** Overexpressing non causes is caught. */
    @Test
    void overexpressingNonCausesIsCaught() {
        Rig rig = TargetedScenarioTest.expressionUpperBound(SabotageMode.OVEREXPRESS);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Over-expression")),
                () -> "expected an over-expression violation, got: " + violations);
    }

    /** Random sweep catches broken engines with margin. */
    @Test
    void randomSweepCatchesBrokenEnginesWithMargin() {
        // Half of the catches measured over these 120 seeds after D115 retired the facts
        // event, re-initialised a lost topic's receivers at the event, and stopped clamping
        // rewinds to the log start (D43's rule; counts in D115): 74, 14, 83, 87, 83, 59, 19,
        // 29 and 93.
        Map<SabotageMode, Integer> floors = new EnumMap<>(SabotageMode.class);
        floors.put(SabotageMode.IGNORE_CAUSES, 37);
        floors.put(SabotageMode.NO_FIFO, 7);
        floors.put(SabotageMode.REDELIVER_REFEEDS, 41);
        floors.put(SabotageMode.UNDECODABLE_AS_ABSENT, 43);
        floors.put(SabotageMode.SKIP_RECEIPT_MERGE, 41);
        floors.put(SabotageMode.DROP_HELD, 29);
        floors.put(SabotageMode.IGNORE_REMOVED_CHANNELS, 9);
        floors.put(SabotageMode.SILENT_DROP, 14);
        floors.put(SabotageMode.OVEREXPRESS, 46);
        // DELIVER_PAST_DEAD_HOLDS has no floor: calibration found 0 catches in 300 seeds. Its
        // oracle evidence is deterministic.
        floors.forEach((mode, floor) -> {
            long caught = LongStream.rangeClosed(1, 120)
                    .filter(seed -> !Scenario.run(seed, mode).clean())
                    .count();
            assertTrue(caught >= floor, "sabotage mode " + mode + " caught by only " + caught
                    + " of 120 seeds (floor " + floor + "): the sweep's margin for this mode has collapsed");
        });

        long recreationCaught = LongStream.rangeClosed(1, 300)
                .filter(seed -> !Scenario.run(seed, SabotageMode.IGNORE_RECREATION).clean())
                .count();
        assertTrue(recreationCaught >= RECREATION_FLOOR, "sabotage mode IGNORE_RECREATION caught by only "
                + recreationCaught + " of 300 seeds (floor " + RECREATION_FLOOR + ", half of the calibrated count):"
                + " the sweep's margin for this mode has collapsed");

        long hostResetCaught = LongStream.rangeClosed(1, 120)
                .filter(seed -> !Scenario.run(seed, SabotageMode.NONE, SimProcess.HostFault.RESET_PAST_LOG_START).clean())
                .count();
        assertTrue(hostResetCaught >= HOST_RESET_FLOOR, "host fault RESET_PAST_LOG_START caught by only "
                + hostResetCaught + " of 120 seeds (floor " + HOST_RESET_FLOOR + "): the sweep's margin for the"
                + " host fault has collapsed");
    }
}
