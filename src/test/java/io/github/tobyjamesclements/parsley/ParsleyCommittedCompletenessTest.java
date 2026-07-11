package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyCommittedCompleteness}'s two-slot commit-cycle snapshot: a value taken at
 * flush {@code N} becomes publishable only at flush {@code N+1} — the proof that transaction {@code N}
 * committed — so a crash between a flush and its commit can never leak an optimistic (rolled-back)
 * completeness onto the non-transactional epoch-events log.
 */
class ParsleyCommittedCompletenessTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    private static ParsleyClock at(long offset) {
        return ParsleyClock.empty().observe(T1_ID, 0, offset);
    }

    /**
     * Before any flush, the publishable value is the restored completeness seeded at {@code bind} —
     * rebuilt from the committed changelog at init, so durable by definition.
     */
    @Test
    void committedIsTheRestoredCompletenessBeforeAnyFlush() {
        ParsleyCommittedCompleteness hook = new ParsleyCommittedCompleteness("h");
        hook.bind(() -> at(9), at(3));

        assertEquals(at(3), hook.committed(),
                "before any commit cycle, only the restored (durable) completeness is publishable — "
                        + "never the live clock, which may reflect the current uncommitted transaction");
    }

    /**
     * A live value snapshotted at flush {@code N} becomes publishable only at flush {@code N+1}: the
     * first flush promotes the restored seed and stages the live value; the second flush — which can
     * only run once the first cycle's transaction committed — promotes it.
     */
    @Test
    void liveValueBecomesPublishableOnlyOneFlushLater() {
        AtomicReference<ParsleyClock> live = new AtomicReference<>(at(5));
        ParsleyCommittedCompleteness hook = new ParsleyCommittedCompleteness("h");
        hook.bind(live::get, at(3));

        hook.flush();                        // cycle N: stages live=5, promotes the seed
        assertEquals(at(3), hook.committed(),
                "at flush N the live value's transaction has not committed yet — the seed stays publishable");

        live.set(at(8));
        hook.flush();                        // cycle N+1: proves cycle N committed, promotes 5, stages 8
        assertEquals(at(5), hook.committed(),
                "flush N+1 proves cycle N's transaction committed, so its snapshot becomes publishable");

        hook.flush();                        // cycle N+2 promotes 8
        assertEquals(at(8), hook.committed(),
                "each flush promotes exactly the previous cycle's snapshot");
    }

    /**
     * The staged (pending) value is never observable through {@code committed()} — a crash between a
     * flush and its transaction commit discards the optimistic pending value with the task instance,
     * and nothing was ever published from it.
     */
    @Test
    void pendingValueIsNeverPublishableUntilProvenCommitted() {
        AtomicReference<ParsleyClock> live = new AtomicReference<>(at(100));
        ParsleyCommittedCompleteness hook = new ParsleyCommittedCompleteness("h");
        hook.bind(live::get, ParsleyClock.empty());

        hook.flush();                        // stages the optimistic 100; its transaction may yet abort

        assertEquals(ParsleyClock.empty(), hook.committed(),
                "the optimistic snapshot staged at flush must not be publishable before a later flush "
                        + "proves its transaction committed — a crash here must leak nothing");
    }
}
