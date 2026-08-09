package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.api.Test;


import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.sim.TargetedScenarioTest.Rig;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the tests: each guarantee is disabled in turn (test-only sabotage; the public API cannot do this) and the
 * suite must catch the resulting violation. EVIDENCE.md cites these: a criterion's test is only evidence if it fails
 * when the behaviour breaks.
 */
class SabotageMetaTest {

    @Test
    void deliveringDespiteCausesIsCaught() {
        Rig rig = TargetedScenarioTest.diamond(SabotageMode.IGNORE_CAUSES);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1")),
                () -> "expected a Safety 1 violation, got: " + violations);
    }

    @Test
    void deliveringPastABlockedHeadIsCaught() {
        Rig rig = TargetedScenarioTest.fifoHold(SabotageMode.NO_FIFO);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertFalse(violations.isEmpty(), "delivering past a blocked head must be flagged");
    }

    @Test
    void redeliveringRefedMessagesIsCaught() {
        Rig rig = TargetedScenarioTest.rewindDedupe(SabotageMode.REDELIVER_REFEEDS);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 2")),
                () -> "expected a Safety 2 violation, got: " + violations);
    }

    @Test
    void treatingUndecodableMetadataAsAbsentIsCaught() {
        Rig rig = TargetedScenarioTest.undecodableMetadata(SabotageMode.UNDECODABLE_AS_ABSENT);
        SimProcess p = rig.proc("p");
        SimWorld.SimChannel c1 = rig.chans.get("c1");
        // The honest scenario asserts this feed fails closed; the sabotaged engine delivers the garbage instead,
        // which is exactly what the expect-throw assertion would flag.
        assertDoesNotThrow(() -> p.feedOne(c1));
        p.drain();
        p.commitStep();
        assertFalse(rig.uidsDelivered("p").isEmpty(), "sabotaged engine delivered despite undecodable metadata");
    }

    @Test
    void droppingHeldCausesOfSendsIsCaught() {
        Rig rig = TargetedScenarioTest.receiptCausesReexpressed(SabotageMode.SKIP_RECEIPT_MERGE);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Structural 15")),
                () -> "expected a Structural 15 violation, got: " + violations);
    }

    @Test
    void losingHeldMessagesAcrossRestartIsCaught() {
        Rig rig = TargetedScenarioTest.heldSurvivesRestart(SabotageMode.DROP_HELD);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a liveness violation, got: " + violations);
    }

    @Test
    void ignoringTruncationIsCaught() {
        Rig rig = TargetedScenarioTest.truncation(SabotageMode.IGNORE_TRUNCATION);
        // The honest scenario asserts fact ingestion fails closed here; the sabotaged engine sails past.
        assertDoesNotThrow(() -> rig.proc("p").ingestFacts());
    }

    @Test
    void startingWithoutARemovedHeldChannelIsCaught() {
        Rig rig = TargetedScenarioTest.removeChannelWithHeld(SabotageMode.IGNORE_REMOVED_CHANNELS);
        // The honest scenario asserts this start is refused; the sabotaged engine starts and strands the held message.
        assertDoesNotThrow(() -> rig.proc("p").start());
        Scenario.quiesce(List.of(rig.proc("p")));
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a stranded-message liveness violation, got: " + violations);
    }

    @Test
    void silentlyDroppingAFedMessageIsCaught() {
        Rig rig = TargetedScenarioTest.fourOnOneChannel(SabotageMode.SILENT_DROP);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Liveness")),
                () -> "expected a fed-but-never-delivered liveness violation, got: " + violations);
    }

    @Test
    void ignoringRecreationIsCaught() {
        Rig rig = TargetedScenarioTest.recreatedTopic(SabotageMode.IGNORE_RECREATION);
        // The honest scenario asserts fact ingestion fails closed on the recreation; the sabotaged engine sails on
        // — which is exactly what the expect-throw assertion (and the sweep's world-truth obligation) flags.
        assertDoesNotThrow(() -> rig.proc("p").ingestFacts());
    }

    @Test
    void deliveringPastDeadChannelHoldsIsCaught() {
        Rig rig = TargetedScenarioTest.deadChannelWithHeldMessages(SabotageMode.DELIVER_PAST_DEAD_HOLDS);
        // The honest scenario asserts fact ingestion refuses (SPEC Safety 9); the sabotaged engine settles the dead
        // channel instead — which the expect-throw assertion flags, and the oracle meta-test below proves harmful.
        assertDoesNotThrow(() -> rig.proc("p").ingestFacts());
    }

    /**
     * The full harm of skipping the Safety 9 refusal, caught by the oracle: upstream q delivers X1 from channel cx
     * (X1's cause is on c9, which q does not receive — vacuous there), cx dies, q legally prunes cx from its
     * metadata and emits E2. The sabotaged downstream p — holding X1, blocked on its c9 cause — sails past the
     * dead-channel verdict, delivers E2 (whose true cause X1 is sitting right there in its hold-back buffer), and
     * once c9 settles delivers X1 behind its effect: the oracle's lifetime Safety 1 check flags the inversion the
     * refusal exists to prevent. This is the deterministic form of the shape the property generator found at
     * random (seed 134 of the pre-fix tree); randomly it needs an adversarial alignment of kill, holds and
     * emission traffic, so the catch proof lives here rather than in a sweep floor.
     */
    @Test
    void deliveringPastDeadChannelHoldsInvertsCausalOrderAndTheOracleSeesIt() {
        Rig rig = new Rig(SabotageMode.DELIVER_PAST_DEAD_HOLDS);
        // The inversion needs the effect's channel drained before the held cause's channel; the drain scans
        // channels in ChannelId order, so assign the roles by that order rather than trusting UUID luck.
        SimWorld.SimChannel chanA = rig.channel("chanA");
        SimWorld.SimChannel chanB = rig.channel("chanB");
        SimWorld.SimChannel cq = chanA.id().compareTo(chanB.id()) < 0 ? chanA : chanB; // q's output: drains first
        SimWorld.SimChannel cx = cq == chanA ? chanB : chanA;                          // dies with X1 held at p
        SimWorld.SimChannel c9 = rig.channel("c9");
        SimWorld.SimChannel ct = rig.channel("ct");
        SimProcess q = rig.process("q", List.of(cx, ct), List.of(cq),
                d -> d.uid.equals("T") ? List.of(cq) : List.of());
        SimProcess p = rig.process("p", List.of(cx, c9, cq), List.of(), d -> List.of());

        rig.external(c9, "N0");
        Instance n1 = rig.external(c9, "N1");
        rig.externalCausedBy(cx, "X1", n1, n1.position);  // X1 depends on c9@1
        rig.external(ct, "T");

        q.feedOne(cx);      // q does not receive c9: X1's cause is vacuous there, X1 delivers at q
        q.drain();
        q.commitStep();

        p.feedOne(cx);      // p receives c9: X1 held until c9@1 is delivered here
        p.drain();
        p.commitStep();

        rig.world.killChannel(cx);
        q.ingestFacts();    // q holds nothing from cx: it prunes the dead channel from its metadata, legally
        q.feedOne(ct);
        q.drain();          // delivers T, emits E2 — true causes include X1, metadata no longer names cx
        q.commitStep();

        p.ingestFacts();    // sabotage: p sails past the Safety 9 refusal and settles the dead channel
        p.feedOne(cq);      // E2 arrives; its only live dependency is c9@1
        p.feedOne(c9);
        p.feedOne(c9);      // N0, N1 arrive
        p.drain();          // N0, N1 deliver; then E2 (cq drains before cx) — past held X1 — then X1
        p.commitStep();

        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Safety 1")),
                () -> "expected the lifetime causal-order inversion to be flagged, got: " + violations);
    }

    @Test
    void overexpressingNonCausesIsCaught() {
        Rig rig = TargetedScenarioTest.expressionUpperBound(SabotageMode.OVEREXPRESS);
        List<String> violations = rig.violationsAfterFinalChecks();
        assertTrue(violations.stream().anyMatch(v -> v.startsWith("Over-expression")),
                () -> "expected an over-expression violation, got: " + violations);
    }

    // ---------------------------------------------------------------- the random sweep also catches broken engines

    /**
     * Per-mode catch margins over a fixed seed range. The floors are set from measured margins (see the values in
     * the assertion messages when this fails): a mode whose margin collapses toward zero has lost its random-sweep
     * coverage even if its targeted meta-test still passes — margin is the evidence, {@code caught > 0} is not.
     */
    @Test
    void randomSweepCatchesBrokenEnginesWithMargin() {
        // Floors are half the margins measured when this check was last calibrated (of 120 seeds: IGNORE_CAUSES 33,
        // NO_FIFO 18, REDELIVER_REFEEDS 80, UNDECODABLE_AS_ABSENT 74, SKIP_RECEIPT_MERGE 73, DROP_HELD 57,
        // IGNORE_TRUNCATION 38 — up from 17 once the sim's rejoin high-water bump stopped masking
        // truncation-while-away, review finding S10 — IGNORE_REMOVED_CHANNELS 35, SILENT_DROP 44, OVEREXPRESS 93).
        Map<SabotageMode, Integer> floors = new EnumMap<>(SabotageMode.class);
        floors.put(SabotageMode.IGNORE_CAUSES, 16);
        floors.put(SabotageMode.NO_FIFO, 9);
        floors.put(SabotageMode.REDELIVER_REFEEDS, 40);
        floors.put(SabotageMode.UNDECODABLE_AS_ABSENT, 37);
        floors.put(SabotageMode.SKIP_RECEIPT_MERGE, 36);
        floors.put(SabotageMode.DROP_HELD, 28);
        floors.put(SabotageMode.IGNORE_TRUNCATION, 19);
        floors.put(SabotageMode.IGNORE_REMOVED_CHANNELS, 17);
        floors.put(SabotageMode.SILENT_DROP, 22);
        floors.put(SabotageMode.OVEREXPRESS, 46);
        floors.forEach((mode, floor) -> {
            long caught = LongStream.rangeClosed(1, 120)
                    .filter(seed -> !Scenario.run(seed, mode).clean())
                    .count();
            assertTrue(caught >= floor, "sabotage mode " + mode + " caught by only " + caught
                    + " of 120 seeds (floor " + floor + "): the sweep's margin for this mode has collapsed");
        });
        // IGNORE_RECREATION's violating shape (a recreation its process survives to quiescence) needs a long setup,
        // so it is measured over the full seed range. DELIVER_PAST_DEAD_HOLDS is deliberately absent from the
        // sweep: its harm needs an adversarial alignment of kill, holds and emission traffic that random topologies
        // reach only incidentally — deliveringPastDeadChannelHoldsInvertsCausalOrderAndTheOracleSeesIt is the
        // deterministic catch proof.
        long recreationCaught = LongStream.rangeClosed(1, 300)
                .filter(seed -> !Scenario.run(seed, SabotageMode.IGNORE_RECREATION).clean())
                .count();
        assertTrue(recreationCaught >= 6, "sabotage mode IGNORE_RECREATION caught by only " + recreationCaught
                + " of 300 seeds (floor 6, half of the calibrated 12): the sweep's margin for this mode has collapsed");
    }
}
