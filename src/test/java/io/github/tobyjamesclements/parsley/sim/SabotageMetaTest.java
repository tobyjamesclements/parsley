package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.PositionFacts;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;
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

    /** Ignoring truncation is caught. */
    @Test
    void ignoringTruncationIsCaught() {
        Rig rig = TargetedScenarioTest.truncation(SabotageMode.IGNORE_TRUNCATION);

        assertDoesNotThrow(() -> rig.proc("p").ingestFacts(),
                "the sabotage must disarm the refusal, or the oracle assertion below tests nothing");

        List<String> violations = Scenario.run(4, SabotageMode.IGNORE_TRUNCATION).violations();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 8")),
                () -> "seed 4 must catch the engine sailing past truncation, got: " + violations);
    }

    /**
     * Delivering past a hold that retention discarded inverts causal order and the oracle
     * sees it (D104): with the log-start check disarmed, the holder delivers the effect B,
     * whose sender legally pruned the discarded cause, before the held cause X. Both the
     * delivery-time check (a cause this process still holds is never settled by world
     * truth) and the end-of-run pair check flag it.
     */
    @Test
    void deliveringPastARetentionDiscardedHoldInvertsCausalOrderAndTheOracleSeesIt() {
        Rig rig = TargetedScenarioTest.retentionCrossesAHeldMessage(SabotageMode.IGNORE_TRUNCATION);
        SimProcess p = rig.proc("p");
        assertDoesNotThrow(p::ingestFacts, "the sabotage must disarm the refusal, or the assertion below tests nothing");
        p.feedOne(rig.chans.get("b"));
        p.feedOne(rig.chans.get("w"));
        p.drain();
        p.commitStep();
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1 (delivery-time)")),
                () -> "expected the delivery-time check to flag the effect delivered past its held cause, got: "
                        + violations);
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1:")),
                () -> "expected the pair check to flag the inversion once X delivers, got: " + violations);
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

        assertDoesNotThrow(() -> rig.proc("p").ingestFacts(),
                "the sabotage must disarm the refusal, or the oracle assertion below tests nothing");

        // Re-pinned from seed 65 when D104 biased the sweep's truncation events toward held
        // channels; 13 of 300 seeds catch this mode under the current generator, 17 the first.
        List<String> violations = Scenario.run(17, SabotageMode.IGNORE_RECREATION).violations();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Assumption 2")),
                () -> "seed 17 must catch the engine running across a recreation, got: " + violations);
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

        assertDoesNotThrow(() -> rig.proc("p").ingestFacts());
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
        q.ingestFacts();
        q.feedOne(ct);
        q.drain();
        q.commitStep();

        p.ingestFacts();
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
        // Half of the catches measured over these 120 seeds after D104 biased truncation
        // toward held channels and D106 decorrelated timestamps (D43's rule; counts in D112).
        Map<SabotageMode, Integer> floors = new EnumMap<>(SabotageMode.class);
        floors.put(SabotageMode.IGNORE_CAUSES, 29);
        floors.put(SabotageMode.NO_FIFO, 5);
        floors.put(SabotageMode.REDELIVER_REFEEDS, 28);
        floors.put(SabotageMode.UNDECODABLE_AS_ABSENT, 39);
        floors.put(SabotageMode.SKIP_RECEIPT_MERGE, 28);
        floors.put(SabotageMode.DROP_HELD, 37);
        floors.put(SabotageMode.IGNORE_TRUNCATION, 25);
        floors.put(SabotageMode.IGNORE_REMOVED_CHANNELS, 16);
        floors.put(SabotageMode.SILENT_DROP, 15);
        floors.put(SabotageMode.OVEREXPRESS, 41);
        // DELIVER_PAST_DEAD_HOLDS and TREAT_COVERED_FEED_AS_REPLAY have no floor: calibration
        // found 0 catches in 300 seeds for each. Their oracle evidence is deterministic.
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
        assertTrue(recreationCaught >= 6, "sabotage mode IGNORE_RECREATION caught by only " + recreationCaught
                + " of 300 seeds (floor 6, half of the calibrated 13): the sweep's margin for this mode has collapsed");
    }

    /**
     * Silently dropping a feed at a report-covered position is caught. The mode disarms the
     * refusal's silent-drop direction, which the random sweep cannot reach (D91: the harness
     * derives read-position reports from a process's own progress, so a successor-ahead
     * report never arises), so the evidence runs directly over {@link ProcessEngine}, the way
     * {@code SupersessionTest} stages the honest refusal: the same shape that makes the honest
     * engine refuse makes the sabotaged one drop the feed as a replay, which is what turns
     * {@code ProcessEngineTest#feedAtAReportCoveredPositionFailsClosedAsCoveredPositionFed}
     * red. This mode carries no sweep floor.
     */
    @Test
    void treatingACoveredFeedAsAReplayIsCaught() {
        ChannelId c1 = new ChannelId(new java.util.UUID(12, 1), 0);
        Map<ChannelId, String> received = Map.of(c1, "c1");
        for (SabotageMode mode : List.of(SabotageMode.NONE, SabotageMode.TREAT_COVERED_FEED_AS_REPLAY)) {
            ProcessEngine engine = EngineTestFactory.create("p", received, new MemoryOrderingStore(), mode);
            engine.onReceive(EngineTestFactory.plain(c1, 2, "A"));
            engine.markDelivered(c1, 2);
            engine.onFacts(new PositionFacts(Map.of(c1, 10L), Map.of(), java.util.Set.of()));
            if (mode == SabotageMode.NONE) {
                ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                        () -> engine.onReceive(EngineTestFactory.plain(c1, 7, "M")),
                        "the honest engine refuses a feed the report already covered");
                assertEquals(ParsleyFailClosedException.Reason.COVERED_POSITION_FED, e.reason());
            } else {
                assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED,
                        assertDoesNotThrow(() -> engine.onReceive(EngineTestFactory.plain(c1, 7, "M"))),
                        "the sabotage must disarm the refusal into a silent drop, or the pin tests nothing");
            }
        }
    }
}
