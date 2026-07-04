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

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset,
                                                           Uuid topicId, ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }
}
