package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyEngine}'s dead-letter cascade: a poison record (undecodable on the forward path)
 * is diverted rather than failing the task when a dead-letter sink is configured, and any buffered
 * dependent whose own dependencies now require an unreachable coordinate is dead-lettered too.
 */
class ParsleyEngineDeadLetterTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final TopicPartition T3 = new TopicPartition("t3", 0);
    private static final TopicPartition T4 = new TopicPartition("t4", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    private static final Uuid T3_ID = Uuid.randomUuid();
    private static final Uuid T4_ID = Uuid.randomUuid();

    /**
     * With no dead-letter sink configured ({@code deadLetterEnabled = false}), a poison record on the
     * forward path fails the task exactly as before dead-lettering existed — the regression guard for
     * the mutation-before-throw hazard: the buffer must not be touched before the throw.
     *
     * Asserts {@code onRecord} throws {@link ParsleyBufferDeserializationException} and the poisoned
     * record remains in the buffer (not removed).
     */
    @Test
    void poisonFailsTheTaskWhenDeadLetteringIsDisabled() {
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis); // deadLetterEnabled defaults to false

        ParsleyClock needsT4 = ParsleyClock.empty().observe(T4_ID, 0, 0);
        engine.onRecord(message(T1, 5, T1_ID, needsT4));
        buffer.poison(0L); // the only sequence added so far

        assertThrows(ParsleyBufferDeserializationException.class,
                () -> engine.onRecord(message(T4, 0, T4_ID, ParsleyClock.empty())),
                "a poison record on the forward path must fail the task when no dead-letter sink is configured");
        assertEquals(1, buffer.size(), "the poisoned record must remain in the buffer for recovery, not be removed");
    }

    /**
     * With a dead-letter sink configured ({@code deadLetterEnabled = true}), a chain of three buffered
     * records — B depends on A's own coordinate, C depends on B's own coordinate — all cascade to the
     * dead letter outcome in one pass once A is proven poison: A itself (undecodable), then B and C
     * (each provably unsatisfiable once their premise is gone), none forwarded, none left buffered.
     *
     * Asserts the outcome's {@code deadLettered} contains A (Undecodable/POISON), B and C
     * (Decoded/ORPHAN_CASCADE), nothing is delivered, and the buffer ends empty.
     */
    @Test
    void poisonCascadesToEveryBufferedDependentInOnePass() {
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, true);

        // A (t1@5) depends on the not-yet-observed trigger t4@0 — buffered as sequence 0.
        ParsleyClock needsT4 = ParsleyClock.empty().observe(T4_ID, 0, 0);
        engine.onRecord(message(T1, 5, T1_ID, needsT4));
        buffer.poison(0L);

        // B (t2@0) depends on A's own coordinate t1@5 — buffered as sequence 1.
        ParsleyClock needsA = ParsleyClock.empty().observe(T1_ID, 0, 5);
        engine.onRecord(message(T2, 0, T2_ID, needsA));

        // C (t3@0) depends on B's own coordinate t2@0 — buffered as sequence 2.
        ParsleyClock needsB = ParsleyClock.empty().observe(T2_ID, 0, 0);
        engine.onRecord(message(T3, 0, T3_ID, needsB));

        assertEquals(3, buffer.size(), "all three records must be held before the trigger arrives");

        // The trigger (t4@0, no deps) delivers immediately, advancing the t4 frontier and releasing A
        // via propagate()'s cascade — where fetching A's buffered entry throws (poisoned).
        ParsleyEngine.Outcome<String, String> outcome =
                engine.onRecord(message(T4, 0, T4_ID, ParsleyClock.empty()));

        assertEquals(List.of("t4"), outcome.delivered().stream().map(ParsleyMessage::topic).toList(),
                "only the trigger itself is delivered — A, B, and C are all dead-lettered, never forwarded");
        assertEquals(3, outcome.deadLettered().size(), "A, B, and C must all appear in the dead-letter outcome");
        assertEquals(
                List.of("t1", "t2", "t3"),
                outcome.deadLettered().stream().map(ParsleyEngine.DeadLetter::topic).toList(),
                "A must dead-letter first (the root cause), then B, then C, in cascade order");
        assertEquals(ParsleyEngine.DeadLetter.Reason.POISON, outcome.deadLettered().get(0).reason(),
                "A is the poison root cause");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(1).reason(),
                "B is a cascade victim of A");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(2).reason(),
                "C is a cascade victim of B");
        assertTrue(outcome.deadLettered().get(0) instanceof ParsleyEngine.DeadLetter.Undecodable,
                "A's value could not be decoded — Undecodable");
        assertTrue(outcome.deadLettered().get(1) instanceof ParsleyEngine.DeadLetter.Decoded,
                "B decoded fine — only its premise is gone");
        assertTrue(outcome.deadLettered().get(2) instanceof ParsleyEngine.DeadLetter.Decoded,
                "C decoded fine — only its premise is gone");
        assertEquals(0, buffer.size(), "the buffer must end empty — nothing left waiting on a dead coordinate");
    }

    /**
     * An orphan floor already recorded before a crash (as if an earlier pass dead-lettered the
     * coordinate's owning record but the cascade was interrupted before reaching every dependent) is
     * picked up by the very next full-buffer scan — {@link ParsleyEngine#drainAfterRestore()} — with no
     * separate recovery mechanism needed: a still-buffered dependent naming that coordinate is dead-
     * lettered on this pass.
     *
     * Asserts {@code drainAfterRestore()} dead-letters the buffered dependent and empties the buffer.
     */
    @Test
    void drainAfterRestoreCompletesAnOrphanCascadeInterruptedBeforeTheCrash() {
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        MockCandidateIndex candidateIndex = new MockCandidateIndex();
        MockOrphanIndex orphanIndex = new MockOrphanIndex();
        // Simulate: t2's coordinate at offset 0 was already found orphaned (its owning record —
        // "B" — was dead-lettered) before the crash, but C's own cascade step never ran.
        orphanIndex.markOrphaned(T2_ID, 0, 0);

        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), orphanIndex);
        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(frontier, buffer, candidateIndex,
                ParsleyMetrics.NOOP, CausalAudit.NOOP, System::currentTimeMillis, true);

        // C (t3@0) depends on t2@0 — the coordinate already known orphaned — restored into the buffer
        // exactly as it would be after a restart (indexed the same way onRecord would have indexed it).
        ParsleyMessage<String, String> c = message(T3, 0, T3_ID, ParsleyClock.empty().observe(T2_ID, 0, 0));
        long seq = buffer.add(c, 0L);
        candidateIndex.index(seq, c.dependencies(), ParsleyClock.empty());

        ParsleyEngine.Outcome<String, String> outcome = restarted.drainAfterRestore();

        assertEquals(1, outcome.deadLettered().size(), "C must be dead-lettered on the very next full scan");
        assertEquals("t3", outcome.deadLettered().get(0).topic(), "the dead-lettered record must be C");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(0).reason(),
                "C's own premise (t2@0) was already proven orphaned before the crash");
        assertEquals(0, buffer.size(), "the buffer must end empty — the interrupted cascade is now complete");
    }

    /**
     * Regression test for the BACKLOG.md #1 double-dispatch bug: {@code propagate()} used to collect
     * deliverable candidates into a batch and release them only after scanning every candidate at that
     * level, so a poison candidate found <em>later</em> in the same scan could dead-letter (via its
     * orphan cascade) an entry already collected as deliverable but not yet removed from the buffer —
     * the release loop then forwarded it anyway.
     *
     * <p>Three fan-in channels C, D, E (a cross-channel completeness gate, {@code trackChannels =
     * true}, is required to reach this bug — see the class comment on the constructor used below).
     * R (D@3) depends on {@code {C@5, E@7}}; P (C@5, poison) depends on {@code {E@7}} — both indexed
     * on the same coordinate (E@7), R first (lower buffer sequence). Once cross-channel headers make
     * completeness(C) reach 5 and E@7 itself delivers, {@code propagate(E, 7)} finds R then P in one
     * scan: R passes the deliverability check, P throws on fetch (poisoned) and cascades an orphan of
     * C@5 — which used to catch R still sitting in the buffer.
     *
     * <p>Asserts R (D@3) is delivered exactly once and never also appears as dead-lettered — the
     * {@link ParsleyEngine.Outcome} disjointness contract.
     */
    @Test
    void propagateDoesNotBothForwardAndDeadLetterTheSameRecord() {
        TopicPartition tc = new TopicPartition("tc", 0);
        TopicPartition td = new TopicPartition("td", 0);
        TopicPartition te = new TopicPartition("te", 0);
        Uuid tcId = Uuid.randomUuid();
        Uuid tdId = Uuid.randomUuid();
        Uuid teId = Uuid.randomUuid();

        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), new MockOrphanIndex());
        // Pre-register every input channel (as the processor does at registration), so a channel that
        // has not yet advertised anything still holds the completeness min rather than being absent.
        frontier.channelUpdate(tcId, 0, ParsleyClock.empty());
        frontier.channelUpdate(tdId, 0, ParsleyClock.empty());
        frontier.channelUpdate(teId, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(frontier, buffer, new MockCandidateIndex(),
                ParsleyMetrics.NOOP, CausalAudit.NOOP, System::currentTimeMillis, true);

        // Advance C's own frontier to 4 (no deps) so completeness(C) has somewhere to rise from.
        for (long offset = 0; offset < 5; offset++) {
            engine.onRecord(message(tc, offset, tcId, ParsleyClock.empty()));
        }
        // Advance D's own frontier to 1 (no deps).
        engine.onRecord(message(td, 0, tdId, ParsleyClock.empty()));
        engine.onRecord(message(td, 1, tdId, ParsleyClock.empty()));

        // R = D@3, deps {C@5, E@7} — held; advertises C@5 and E@7 on channel D.
        ParsleyClock rDeps = ParsleyClock.empty().observe(tcId, 0, 5).observe(teId, 0, 7);
        engine.onRecord(message(td, 3, tdId, rDeps));

        // P = C@5, deps {E@7} — held; advertises E@7 on channel C. Poisoned so its later fetch throws.
        ParsleyClock pDeps = ParsleyClock.empty().observe(teId, 0, 7);
        engine.onRecord(message(tc, 5, tcId, pDeps));
        buffer.poison(1L); // R took sequence 0 (the C@0-4 and D@0-1 records never buffer); P is seq 1

        // C@6, deps {C@5} — held (an intra-topic backward reference); its receipt-time channelUpdate
        // makes channel C advertise C@5, which is what lets completeness(C) reach 5 below.
        ParsleyClock c6Deps = ParsleyClock.empty().observe(tcId, 0, 5);
        engine.onRecord(message(tc, 6, tcId, c6Deps));

        assertEquals(3, buffer.size(), "R, P, and C@6 must all be held before E@7 arrives");

        // E@7, deps {C@5} — its own receipt-time channelUpdate is what makes channel E advertise C@5
        // too (channels C and D already do, via C@6 and R respectively), so completeness(C) reaches 5
        // and E@7 itself delivers in the very same step, triggering propagate(E, 7), which finds R
        // (seq 0) then P (seq 2) in the same scan.
        ParsleyClock e7Deps = ParsleyClock.empty().observe(tcId, 0, 5);
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(te, 7, teId, e7Deps));

        assertEquals(5L, engine.completeness().offsetFor(tcId, 0),
                "completeness(C) must have reached 5 for R to have been judged deliverable at all");
        List<String> delivered = outcome.delivered().stream()
                .map(m -> m.topic() + "@" + m.offset()).toList();
        List<String> deadLettered = outcome.deadLettered().stream()
                .map(d -> d.topic() + "@" + d.offset()).toList();

        assertTrue(delivered.contains("td@3"), "R (D@3) must be delivered — it was causally valid the "
                + "moment completeness(C) reached 5, before C's local poison was ever discovered");
        assertTrue(deadLettered.stream().noneMatch(delivered::contains),
                "no record may appear in both delivered() and deadLettered() — Outcome's disjointness "
                        + "contract");
        assertEquals(1, delivered.stream().filter("td@3"::equals).count(),
                "R (D@3) must be delivered exactly once, not double-counted");
    }

    /**
     * Regression test for the related gap named in the same BACKLOG.md item: {@code propagate()} used
     * to never check {@code isProvenImpossible}, unlike {@code onRecord} and {@code drainSatisfied}, so
     * a candidate that is simultaneously "deliverable" (per the completeness frontier) and "proven
     * impossible" (per the orphan index) would be forwarded if {@code propagate()}'s own candidate-index
     * scan reached it — because completeness is driven by cross-channel header advertisement, not
     * genuine local delivery, a coordinate can look satisfied even though the buffered record that would
     * have supplied it is independently proven poison and orphaned.
     *
     * <p>Three channels G, W, V. W (the poison record) and V (the record under test) are both held on
     * {@code G@2} (so a single {@code propagate(G, 2)} scan finds both, W first by buffer sequence). W
     * depends on {@code {G@2, W@0}} — the second an exact self-reference, which advertises {@code W@0}
     * on channel W without gating W's own deliverability (self-cycles are stripped from the gate but
     * not from the raw channel advertisement). Another held record on G (never released in this test)
     * advertises {@code W@0} on channel G. Combined with V's own admission advertising {@code W@0} on
     * channel V, all three channels confirm {@code W@0} by the time V is buffered — so {@code W@0} is
     * already satisfied and never indexed against V at all; only {@code G@2} is.
     *
     * <p>When {@code G@2} finally delivers, {@code propagate(G, 2)} finds W first: it is poisoned,
     * dead-lettered, and orphans coordinate W at floor 0. It then finds V: {@code isDeliverable(V)}
     * holds (completeness dominates both {@code G@2} and {@code W@0}, the latter via cross-channel
     * headers, independent of W ever truly delivering) — without the fix this would forward V; with the
     * fix, {@code isProvenImpossible(V)} catches it first, since V's own {@code W@0} dependency is now
     * at/above W's orphan floor.
     *
     * <p>Asserts V is dead-lettered ({@link ParsleyEngine.DeadLetter.Reason#ORPHAN_CASCADE}) and never
     * delivered.
     */
    @Test
    void propagateDeadLettersACandidateWhoseDirectDependencyWasOrphanedByAnUnrelatedCascade() {
        TopicPartition tg = new TopicPartition("tg", 0);
        TopicPartition tw = new TopicPartition("tw", 0);
        TopicPartition tv = new TopicPartition("tv", 0);
        Uuid tgId = Uuid.randomUuid();
        Uuid twId = Uuid.randomUuid();
        Uuid tvId = Uuid.randomUuid();

        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), new MockOrphanIndex());
        frontier.channelUpdate(tgId, 0, ParsleyClock.empty());
        frontier.channelUpdate(twId, 0, ParsleyClock.empty());
        frontier.channelUpdate(tvId, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(frontier, buffer, new MockCandidateIndex(),
                ParsleyMetrics.NOOP, CausalAudit.NOOP, System::currentTimeMillis, true);

        // Advance G's own frontier to 1 (no deps), so G@2 below is a clean, contiguous next advance.
        engine.onRecord(message(tg, 0, tgId, ParsleyClock.empty()));
        engine.onRecord(message(tg, 1, tgId, ParsleyClock.empty()));

        // A held filler on G (never released in this test) advertises W@0 on channel G, without
        // disturbing G's contiguous frontier (it sits at an offset past where G actually advances).
        engine.onRecord(message(tg, 5, tgId, ParsleyClock.empty().observe(twId, 0, 0)));

        // W = W@0, deps {G@2, W@0} — the W@0 is an exact self-reference (stripped from W's own
        // deliverability gate, so W is held on G@2 alone) but still advertises W@0 on channel W via the
        // raw, unstripped receipt-time channelUpdate.
        ParsleyClock wDeps = ParsleyClock.empty().observe(tgId, 0, 2).observe(twId, 0, 0);
        engine.onRecord(message(tw, 0, twId, wDeps));
        buffer.poison(1L); // seq 0 is the G@5 filler, W@0 is seq 1

        // V (the record under test): v@2 depends on {G@2, W@0} — buffered while G@2 is not yet
        // observed. By now channels G (via the filler) and W (via W@0's self-reference) already
        // advertise W@0, and V's own admission advertises it on channel V too, so completeness(W)
        // already dominates 0 by the time this record is indexed — W@0 is satisfied and never indexed
        // against V; only G@2 is.
        ParsleyClock vDeps = ParsleyClock.empty().observe(tgId, 0, 2).observe(twId, 0, 0);
        engine.onRecord(message(tv, 2, tvId, vDeps));

        // The G@5 filler's only purpose was to advertise W@0 on channel G; the moment V's own admission
        // makes completeness(W) reach 0 (the last of the three channels to confirm it), the filler's
        // own pending dependency on W@0 is satisfied too, and the resulting cross-channel drain
        // (triggered by V's channel-clock advance) releases it in this same call — it does not
        // interfere with G's contiguous frontier (delivered out of order, at offset 5 with a gap before
        // it) or with what follows.
        assertEquals(2, buffer.size(), "W@0 and V (v@2) must still be held before G@2 arrives");
        assertEquals(0L, engine.completeness().offsetFor(twId, 0),
                "completeness(W) must already dominate W@0 via cross-channel advertisement, before W@0 "
                        + "is ever proven poison below");

        // G@2, deps {} — delivers immediately, advancing G's frontier to 2 and triggering
        // propagate(G, 2), which finds W@0 first (poisoned, orphaning W at floor 0) and then V (v@2) —
        // deliverable per completeness, but its direct dependency on W@0 is now provably impossible.
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(tg, 2, tgId, ParsleyClock.empty()));

        List<String> delivered = outcome.delivered().stream()
                .map(m -> m.topic() + "@" + m.offset()).toList();
        assertTrue(delivered.stream().noneMatch("tv@2"::equals),
                "V (v@2) must never be delivered — its direct dependency W@0 is proven impossible");
        ParsleyEngine.DeadLetter<String, String> vLetter = outcome.deadLettered().stream()
                .filter(d -> d.topic().equals("tv") && d.offset() == 2)
                .findFirst()
                .orElse(null);
        assertTrue(vLetter != null, "V (v@2) must be dead-lettered");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, vLetter.reason(),
                "V is a cascade victim of W's poisoning, not itself poison");
    }

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset,
                                                           Uuid topicId, ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }
}
