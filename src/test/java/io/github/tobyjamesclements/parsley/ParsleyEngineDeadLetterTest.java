package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyEngine}'s dead-letter cascade: a poison record (undecodable on the forward path)
 * is diverted rather than failing the task when a dead-letter sink is configured, and any buffered
 * dependent whose own dependencies now require an unreachable coordinate is dead-lettered too. Also
 * covers {@link ParsleyEngine#deadLetterAtIngest}, the counterpart for a record dead-lettered before it
 * ever became engine state at all (an undecodable causal-dependencies header).
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

    // Coordinates for the cross-branch orphan-cascade repro below: root Z fans out to D and E, each of
    // which fans out to a distinct record on C — so C is discovered twice, at two different floors.
    private static final TopicPartition Z = new TopicPartition("z", 0);
    private static final TopicPartition D = new TopicPartition("d", 0);
    private static final TopicPartition E = new TopicPartition("e", 0);
    private static final TopicPartition C = new TopicPartition("c", 0);
    private static final TopicPartition REQ = new TopicPartition("req", 0);
    private static final Uuid Z_ID = Uuid.randomUuid();
    private static final Uuid D_ID = Uuid.randomUuid();
    private static final Uuid E_ID = Uuid.randomUuid();
    private static final Uuid C_ID = Uuid.randomUuid();
    private static final Uuid REQ_ID = Uuid.randomUuid();

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
     * Regression test for BACKLOG.md's dead-letter torn-changelog-write finding: every dead-letter path
     * must durably orphan a victim's own coordinate <em>before</em> removing it from the buffer, so a
     * crash between the two writes always tears toward "orphan floor recorded, victim still buffered" (a
     * harmless duplicate the next {@code drainSatisfied}/{@code drainAfterRestore} pass resolves) — never
     * the reverse, which would leave the record gone with no floor recorded, permanently stranding
     * anything depending on that exact coordinate.
     *
     * <p>Same three-record chain as {@link #poisonCascadesToEveryBufferedDependentInOnePass} — A (t1@5,
     * poison) triggers {@code propagate()}'s poison-on-fetch branch directly; B (t2@0, depends on A) and
     * C (t3@0, depends on B) are each caught by {@code orphan()}'s cascade via {@link
     * ParsleyEngine#fetchForDeadLetter} — but here the buffer and orphan index are recording wrappers
     * sharing one call-order log, so the test asserts the actual write order rather than just the
     * outcome (which {@link #poisonCascadesToEveryBufferedDependentInOnePass} already covers).
     *
     * Asserts, for each of A, B, and C, that its own coordinate's {@code mark:} log entry precedes its
     * {@code remove:} log entry.
     */
    @Test
    void markOrphanedAlwaysPrecedesTheVictimsOwnBufferRemoval() {
        List<String> log = new ArrayList<>();
        Map<Uuid, String> labels = Map.of(T1_ID, "t1", T2_ID, "t2", T3_ID, "t3", T4_ID, "t4");
        RecordingBufferStore<String, String> buffer = new RecordingBufferStore<>(log);
        RecordingOrphanIndex orphanIndex = new RecordingOrphanIndex(log, labels);
        // trackChannels = false, matching poisonCascadesToEveryBufferedDependentInOnePass's convenience
        // constructor: single-layer frontier-only mode, so A's own advertisement of its t4@0 dependency
        // does not trivially self-satisfy it (which the multi-channel completeness mode would allow with
        // only one channel registered).
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), orphanIndex, false);
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(frontier, buffer, new MockCandidateIndex(),
                ParsleyMetrics.NOOP, CausalAudit.NOOP, System::currentTimeMillis, true);

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

        // The trigger (t4@0, no deps) delivers immediately, cascading through A (propagate's poison
        // branch), then B and C (orphan()'s cascade via fetchForDeadLetter).
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(T4, 0, T4_ID, ParsleyClock.empty()));
        assertEquals(3, outcome.deadLettered().size(), "A, B, and C must all be dead-lettered in this one pass");

        assertMarkedBeforeRemoved(log, "t1@5", "remove:seq0", "A (propagate's poison-on-fetch branch)");
        assertMarkedBeforeRemoved(log, "t2@0", "remove:seq1", "B (orphan()'s cascade, via fetchForDeadLetter)");
        assertMarkedBeforeRemoved(log, "t3@0", "remove:seq2", "C (orphan()'s cascade, via fetchForDeadLetter)");
    }

    /** Asserts {@code log} contains {@code "mark:" + coord} strictly before {@code removeEntry}. */
    private static void assertMarkedBeforeRemoved(List<String> log, String coord, String removeEntry, String who) {
        int markIndex = log.indexOf("mark:" + coord);
        int removeIndex = log.indexOf(removeEntry);
        assertTrue(markIndex >= 0, "expected a mark: entry for " + coord + " in " + log);
        assertTrue(removeIndex >= 0, "expected a " + removeEntry + " entry in " + log);
        assertTrue(markIndex < removeIndex, who + ": markOrphaned(" + coord + ") must precede " + removeEntry
                + " — log was " + log);
    }

    /**
     * The ingest-time counterpart to {@link #poisonCascadesToEveryBufferedDependentInOnePass}: an
     * {@code UNRESOLVABLE_CLOCK} record — its causal-dependencies header undecodable at ingest — never
     * goes through {@link ParsleyEngine#onRecord} at all (see {@code ParsleyProcessor#onUnresolvableClock}),
     * so {@link ParsleyEngine#deadLetterAtIngest} is the only place its coordinate is ever durably
     * orphaned. Regression test for the BACKLOG.md finding: without this call, the coordinate's frontier
     * freezes forever with nothing recording why, and any buffered dependent is held indefinitely.
     *
     * <p>B (t2@0) is buffered depending on t1@5 — the coordinate about to be dead-lettered at ingest,
     * which (unlike the poison case above) never itself occupies a buffer slot. A second record
     * depending on a <em>later</em> offset of the same coordinate ({@code t1@7}, not merely the literal
     * next offset) arriving afterwards is then shown to be caught too, proving the fix is not limited to
     * the narrow case where the dead-lettered record happened to be the very first offset ever observed
     * for its coordinate (the {@code seedIfFirstSeen} accident the finding calls out, which only rescues
     * the immediate next record).
     *
     * <p>Asserts the ingest-time outcome delivers nothing and dead-letters B ({@code ORPHAN_CASCADE}),
     * emptying the buffer, and that the later record is dead-lettered immediately on admission too.
     */
    @Test
    void deadLetterAtIngestOrphansItsCoordinateAndCascadesToBufferedDependents() {
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, true);

        // B (t2@0) depends on t1@5 — the coordinate about to be dead-lettered at ingest, before it is
        // ever buffered itself.
        ParsleyClock needsT1at5 = ParsleyClock.empty().observe(T1_ID, 0, 5);
        engine.onRecord(message(T2, 0, T2_ID, needsT1at5));
        assertEquals(1, buffer.size(), "B must be held before t1@5 is ever dead-lettered");

        // Simulates ParsleyProcessor#onUnresolvableClock: t1@5's causal-dependencies header was
        // undecodable at ingest, so it never went through onRecord and has no buffer/candidate-index
        // footprint of its own at all.
        ParsleyEngine.Outcome<String, String> outcome = engine.deadLetterAtIngest(T1_ID, 0, 5);

        assertTrue(outcome.delivered().isEmpty(), "orphaning never releases anything itself");
        assertEquals(1, outcome.deadLettered().size(), "B must be dead-lettered as a cascade victim");
        assertEquals("t2", outcome.deadLettered().get(0).topic());
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(0).reason());
        assertEquals(0, buffer.size(), "B must not be left buffered forever");

        // C (t3@0) depends on t1@7 — a later offset of the same now-orphaned coordinate, not merely the
        // literal next one — arriving afterwards must be caught immediately too.
        ParsleyClock needsT1at7 = ParsleyClock.empty().observe(T1_ID, 0, 7);
        ParsleyEngine.Outcome<String, String> later = engine.onRecord(message(T3, 0, T3_ID, needsT1at7));
        assertTrue(later.delivered().isEmpty(), "C must never be delivered on a provably impossible premise");
        assertEquals(1, later.deadLettered().size(), "C must be dead-lettered immediately, on admission");
        assertEquals("t3", later.deadLettered().get(0).topic());
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, later.deadLettered().get(0).reason());
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

    /**
     * Regression test for the BACKLOG.md liveness gap: {@code onRecord}'s receipt-time channel-clock
     * update (before the gate) runs for <em>every</em> record, including one that turns out
     * proven-impossible — so a record's own dead-lettering must not skip the channel-advance drain its
     * arrival may have unlocked for other, unrelated buffered records, or they would be stuck with no
     * further trigger to re-check them.
     *
     * <p>Two fan-in channels T1, T2 ({@code trackChannels = true}, required to reach this bug — a
     * single-channel processor's completeness is its own frontier, fully covered by {@code propagate}).
     * R (T1@10) depends on {@code T1@5}: delivered locally on channel T1, but channel T2 has not yet
     * advertised it, so completeness(T1) is entirely absent and R is held. X (T2@1) then arrives
     * advertising {@code T1@5} in its own header — the last channel needed to confirm it — but X itself
     * depends on {@code T3@0}, a coordinate already orphaned, so it is dead-lettered instead of
     * forwarded.
     *
     * <p>Asserts R is still released in the very same {@code onRecord(X)} call, and X is dead-lettered —
     * the channel advance recorded before X's disposition was decided must not be lost just because X
     * itself never delivers.
     */
    @Test
    void aProvenImpossibleRecordsChannelAdvanceStillReleasesOtherHeldRecords() {
        MockOrphanIndex orphanIndex = new MockOrphanIndex();
        orphanIndex.markOrphaned(T3_ID, 0, 0); // T3 offset 0 and above is already proven impossible

        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyFrontier frontier = new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), orphanIndex);
        // Pre-register both input channels, as the processor does at registration.
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(frontier, buffer, new MockCandidateIndex(),
                ParsleyMetrics.NOOP, CausalAudit.NOOP, System::currentTimeMillis, true);

        // T1@5, deps {} — delivers immediately, advancing T1's own frontier to 5. Channel T1's
        // advertised clock stays empty (this record's own deps are empty).
        engine.onRecord(message(T1, 5, T1_ID, ParsleyClock.empty()));

        // R = T1@10, deps {T1@5} — held: completeness(T1) requires every channel to confirm T1@5, and
        // channel T2 has said nothing about T1 at all yet, so T1 is entirely absent from completeness.
        ParsleyEngine.Outcome<String, String> beforeX =
                engine.onRecord(message(T1, 10, T1_ID, ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(beforeX.delivered().isEmpty(), "R (T1@10) must be held: channel T2 has not yet confirmed T1@5");
        assertEquals(1, buffer.size(), "R must be sitting in the buffer before X arrives");

        // X = T2@1, deps {T1@5, T3@0} — its receipt-time channelUpdate advertises T1@5 on channel T2
        // (the confirmation R was waiting on), but X itself depends on the already-orphaned T3@0.
        ParsleyClock xDeps = ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T3_ID, 0, 0);
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(T2, 1, T2_ID, xDeps));

        List<String> delivered = outcome.delivered().stream().map(m -> m.topic() + "@" + m.offset()).toList();
        assertTrue(delivered.contains("t1@10"),
                "R (T1@10) must still be released — X's channel advance must not be skipped just "
                        + "because X itself was dead-lettered");
        assertEquals(1, outcome.deadLettered().size(), "X (T2@1) itself must be dead-lettered");
        assertEquals("t2", outcome.deadLettered().get(0).topic());
        assertEquals(1L, outcome.deadLettered().get(0).offset());
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(0).reason(),
                "X depends on an already-orphaned coordinate, not itself poison");
    }

    /**
     * Regression test for BACKLOG.md's orphan-floor-monotonicity finding (the "raise" direction): once a
     * coordinate's floor is established, dead-lettering a <em>later</em> offset of the same coordinate
     * must not weaken it — the coordinate's contiguous frontier is frozen at the earliest offset ever
     * proven unreachable, regardless of what is proven unreachable afterward.
     *
     * <p>T1@5 is dead-lettered at ingest, establishing floor(T1) = 5. T1@6 (the exact producer pattern of
     * a record depending on T1@5) is then dead-lettered at ingest too — under the inverted-monotonicity
     * bug this used to overwrite the floor to 6. A record depending on {@code T1@5} arriving afterwards
     * must still be caught: under the bug, {@code isProvenImpossible} would see {@code 5 >= 6} as false
     * and buffer it — and since T1's frontier can never reach 5, it would then be held forever, never
     * released, proven impossible, or cascaded.
     *
     * <p>Asserts the orphan floor stays at 5 after the second dead-letter, and a dependent on
     * {@code T1@5} is dead-lettered immediately on admission rather than buffered.
     */
    @Test
    void orphaningALaterOffsetOfAnAlreadyOrphanedCoordinateDoesNotWeakenItsFloor() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        MockOrphanIndex orphanIndex = new MockOrphanIndex();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), orphanIndex, ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, true);

        engine.deadLetterAtIngest(T1_ID, 0, 5);
        assertEquals(5L, orphanIndex.orphanFloor(T1_ID, 0), "the floor must be established at 5");

        engine.deadLetterAtIngest(T1_ID, 0, 6);
        assertEquals(5L, orphanIndex.orphanFloor(T1_ID, 0),
                "a later dead-letter at a higher offset must not weaken the floor");

        // X (t2@0) depends on t1@5 — must be caught as proven impossible, not buffered forever.
        ParsleyClock needsT1at5 = ParsleyClock.empty().observe(T1_ID, 0, 5);
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(T2, 0, T2_ID, needsT1at5));

        assertTrue(outcome.delivered().isEmpty(), "X must never be delivered on a provably impossible premise");
        assertEquals(1, outcome.deadLettered().size(), "X must be dead-lettered immediately, on admission");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(0).reason(),
                "X depends on an already-orphaned coordinate, not itself poison");
        assertEquals(0, buffer.size(), "X must never be left buffered");
    }

    /**
     * Regression test for BACKLOG.md's orphan-floor-monotonicity finding (the "ignore" direction): once a
     * coordinate's floor is established, a cascade that later dead-letters an <em>earlier</em> offset of
     * the same coordinate must durably lower the floor — the earlier offset is the stronger, truer claim
     * about where the coordinate's contiguous frontier actually freezes.
     *
     * <p>T1@10 is dead-lettered at ingest first, establishing floor(T1) = 10 — simulating an earlier,
     * independent proof reached before V (T1@5) is even known about. V (T1@5) is then buffered depending
     * on an unrelated, not-yet-observed coordinate (T4@0); when T4@0 is later dead-lettered as a root
     * cause, the cascade catches V and durably orphans its own coordinate, T1@5 — under the
     * inverted-monotonicity bug this used to be silently dropped as a no-op ({@code 10 >= 5}), leaving
     * the floor stuck at 10. A record depending on {@code T1@7} — between the true floor (5) and the
     * stale one (10) — arriving afterwards must be caught immediately: under the bug it would be buffered
     * forever, since T1's frontier can never reach 5, let alone 7.
     *
     * <p>Asserts the orphan floor lowers to 5 once V is cascaded, and a dependent on {@code T1@7} is
     * dead-lettered immediately on admission rather than buffered.
     */
    @Test
    void orphaningAnEarlierOffsetOfAnAlreadyOrphanedCoordinateStrengthensItsFloor() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        MockOrphanIndex orphanIndex = new MockOrphanIndex();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), orphanIndex, ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, true);

        engine.deadLetterAtIngest(T1_ID, 0, 10);
        assertEquals(10L, orphanIndex.orphanFloor(T1_ID, 0), "the stale floor must be established at 10 first");

        // V (t1@5) depends on the not-yet-observed t4@0 — buffered, its own coordinate independent of
        // the floor already recorded for T1.
        ParsleyClock needsT4 = ParsleyClock.empty().observe(T4_ID, 0, 0);
        engine.onRecord(message(T1, 5, T1_ID, needsT4));
        assertEquals(1, buffer.size(), "V must be held before T4@0 is ever dead-lettered");

        // T4@0 dead-lettered as a root cause cascades to V, durably orphaning its own coordinate T1@5 —
        // the stronger, truer floor.
        engine.deadLetterAtIngest(T4_ID, 0, 0);
        assertEquals(5L, orphanIndex.orphanFloor(T1_ID, 0),
                "an earlier, truer floor discovered later must lower the stale one, not be ignored");
        assertEquals(0, buffer.size(), "V must not be left buffered forever");

        // W (t2@0) depends on t1@7 — between the true floor (5) and the stale one (10) — must be caught
        // immediately.
        ParsleyClock needsT1at7 = ParsleyClock.empty().observe(T1_ID, 0, 7);
        ParsleyEngine.Outcome<String, String> outcome = engine.onRecord(message(T2, 0, T2_ID, needsT1at7));

        assertTrue(outcome.delivered().isEmpty(), "W must never be delivered on a provably impossible premise");
        assertEquals(1, outcome.deadLettered().size(), "W must be dead-lettered immediately, on admission");
        assertEquals(ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE, outcome.deadLettered().get(0).reason(),
                "W depends on an already-orphaned coordinate, not itself poison");
        assertEquals(0, buffer.size(), "W must never be left buffered");
    }

    /**
     * Regression test for BACKLOG.md's floor-blind scan-gating finding: a coordinate discovered twice in
     * one {@code orphan()} cascade, via two different parent branches, at two different floors, must be
     * scanned at the <em>lower</em> of the two floors — otherwise the range between them is never
     * scanned in this pass and a genuine dependent in that range is stranded.
     *
     * <p>Root Z is orphaned. Scanning Z's dependents finds V1 (D@0, indexed requiring Z@0) and V2 (E@0,
     * indexed requiring Z@1) — Z's required-offset ordering (ascending) guarantees V1 is scanned first,
     * so its cascade reaches the worklist before V2's. Scanning V1's own coordinate (D@0) finds C10
     * (C@10, indexed requiring D@0); scanning V2's own coordinate (E@0) finds C5 (C@5, indexed requiring
     * E@0). The worklist is now {@code [(C,10), (C,5)]} in that order — cross-branch discovery order is
     * unconstrained, so the higher floor for C is reached first.
     *
     * <p>REQ (indexed requiring C@7) sits strictly between the two floors: {@code 5 <= 7 < 10}. Popping
     * {@code (C,10)} first and scanning at floor 10 does not find it ({@code 7 < 10}); the fix must then
     * still rescan at the lower floor 5 when {@code (C,5)} is popped next, finding REQ then. A single
     * "scanned once" set would instead skip the {@code (C,5)} scan entirely (C already seen), leaving
     * REQ buffered forever.
     *
     * <p>Every record is seeded directly into the buffer and candidate index — mirroring {@link
     * #drainAfterRestoreCompletesAnOrphanCascadeInterruptedBeforeTheCrash}'s pattern — rather than via
     * {@code onRecord}, so the scenario exercises {@code orphan()}'s worklist alone, without the
     * completeness/channel machinery (irrelevant here) also shaping which records get buffered.
     *
     * Asserts REQ is dead-lettered as an orphan-cascade victim and the buffer ends empty.
     */
    @Test
    void orphanRescansACoordinateAtALowerFloorDiscoveredLaterInTheSameCascade() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        MockCandidateIndex candidateIndex = new MockCandidateIndex();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                candidateIndex, new MockForwardedIndex(), new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, true);

        seed(buffer, candidateIndex, message(D, 0, D_ID, ParsleyClock.empty().observe(Z_ID, 0, 0)));
        seed(buffer, candidateIndex, message(E, 0, E_ID, ParsleyClock.empty().observe(Z_ID, 0, 1)));
        seed(buffer, candidateIndex, message(C, 10, C_ID, ParsleyClock.empty().observe(D_ID, 0, 0)));
        seed(buffer, candidateIndex, message(C, 5, C_ID, ParsleyClock.empty().observe(E_ID, 0, 0)));
        seed(buffer, candidateIndex, message(REQ, 0, REQ_ID, ParsleyClock.empty().observe(C_ID, 0, 7)));
        assertEquals(5, buffer.size(), "all five records must be held before Z is ever orphaned");

        ParsleyEngine.Outcome<String, String> outcome = engine.deadLetterAtIngest(Z_ID, 0, 0);

        assertEquals(
                List.of("d", "e", "c", "c", "req"),
                outcome.deadLettered().stream().map(ParsleyEngine.DeadLetter::topic).toList(),
                "the cascade must reach every dependent, including REQ at the lower of C's two discovered floors");
        assertTrue(outcome.deadLettered().stream()
                        .allMatch(letter -> letter.reason() == ParsleyEngine.DeadLetter.Reason.ORPHAN_CASCADE),
                "every victim here is a cascade dependent, none itself poison");
        assertEquals(0, buffer.size(), "REQ must not be left stranded in the buffer forever");
    }

    /** Adds {@code message} to {@code buffer} and indexes it against an empty frontier, exactly as a
     * restored task's one-time construction-time pass would (see {@link ParsleyEngine}'s constructor). */
    private static void seed(MockBufferStore<String, String> buffer, MockCandidateIndex candidateIndex,
                             ParsleyMessage<String, String> message) {
        long seq = buffer.add(message, 0L);
        candidateIndex.index(seq, message.dependencies(), ParsleyClock.empty());
    }

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset,
                                                           Uuid topicId, ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }
}
