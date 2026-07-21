package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CausalStreams#awaitDrain}: the graceful-shutdown drain wait is unbounded only
 * while draining can actually progress and there is something to drain. An instance whose streams died
 * in {@code ERROR} with a still-held record (its task cannot deliver again) must not hang forever, and
 * neither must an instance that sits {@code RUNNING} owning zero tasks — held records are
 * changelog-backed and survive to the next start regardless.
 */
class CausalStreamsCloseTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);

    /**
     * A dead streams instance (ERROR) with a registered task that never drained ends the drain wait via
     * the dead-instance escape rather than hanging forever: the task can never deliver again, so
     * {@link ParsleyQuiesce#isSafeToClose} can never become true, but the ERROR state ends the wait.
     */
    @Test
    void drainWaitEndsWhenTheStreamsInstanceIsDead() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        // A registered, still-undrained task: isSafeToClose() is permanently false, so only the
        // dead-instance (ERROR) escape can end the wait.
        quiesce.register("0_0");

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.ERROR),
                "awaitDrain must return promptly when the streams instance is in ERROR — the registered "
                        + "task can never drain again, so waiting on isSafeToClose() would hang forever");
    }

    /**
     * B2 regression: an instance that owns no tasks (more instances than partitions, or every task
     * migrated away — each unregisters on its own close) sits {@code RUNNING}, so the dead-instance
     * escape never fires, yet has no registered task that could ever report drained. It must still close
     * at once: there is nothing buffered to strand.
     */
    @Test
    void drainWaitReturnsImmediatelyOnARunningInstanceWithZeroRegisteredTasks() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        // No task ever registered, and the instance is healthy (RUNNING) — the pre-fix hang.

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.RUNNING),
                "awaitDrain must return at once on a RUNNING instance with zero registered tasks — it "
                        + "holds nothing to strand, so it must not spin forever waiting for a task that "
                        + "will never exist");
    }

    /**
     * A healthy, drained instance ends the wait through the ordinary safe-to-close path — the dead-
     * instance escape never fires while the instance is RUNNING.
     */
    @Test
    void drainWaitEndsThroughSafeToCloseOnAHealthyInstance() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");
        quiesce.setDrained("0_0", true);

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.RUNNING),
                "awaitDrain must return once every registered task reports drained");
    }

    /**
     * The bounded wait {@link CausalStreams#close(Duration)} uses gives up once the deadline
     * passes while a task is still undrained — returning {@code false} rather than hanging a
     * shutdown hook forever on a buffer that can never drain (a dependency that will never
     * arrive). Giving up delivers nothing early; the held records replay on the next start.
     */
    @Test
    void boundedDrainWaitGivesUpAtTheDeadlineAndReportsFalse() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");
        // Fake clock: first poll sees 600 (before the 1000 deadline), the next sees 1200 (past it).
        java.util.concurrent.atomic.AtomicLong nanos = new java.util.concurrent.atomic.AtomicLong();

        boolean drained = assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.RUNNING,
                        1_000L, () -> nanos.addAndGet(600L)),
                "the bounded drain wait must end at its deadline even while the instance is RUNNING "
                        + "with an undrained task");
        assertFalse(drained, "a wait ended by the deadline must report the drain as incomplete");
    }

    /**
     * The bounded wait reports {@code true} when every registered task is already drained: the
     * deadline is a bound, not a mandatory delay, so a drained instance closes immediately.
     */
    @Test
    void boundedDrainWaitReportsTrueOnceDrained() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");
        quiesce.setDrained("0_0", true);

        boolean drained = assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.RUNNING,
                        // A deadline already in the past: it must not matter, the drain is complete.
                        Long.MIN_VALUE, () -> 0L),
                "the bounded drain wait must return at once when every registered task is drained");
        assertTrue(drained, "a completed drain must be reported as complete regardless of the deadline");
    }

    /**
     * The bounded wait keeps the dead-instance escape: an instance in {@code ERROR} can never
     * drain, so the wait ends (reporting {@code false}) without consuming the deadline budget.
     */
    @Test
    void boundedDrainWaitKeepsTheDeadInstanceEscape() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");

        boolean drained = assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.ERROR,
                        Long.MAX_VALUE, () -> 0L),
                "the bounded drain wait must end promptly on a dead (ERROR) instance rather than "
                        + "spending its whole deadline budget on a task that can never drain");
        assertFalse(drained, "a wait ended by instance death must report the drain as incomplete");
    }

    /**
     * A running instance whose task drains while the wait is in progress ends the wait via
     * safe-to-close: the wait genuinely polls rather than deciding once up front.
     */
    @Test
    void drainWaitPollsUntilARunningInstanceDrains() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");

        AtomicReference<KafkaStreams.State> state = new AtomicReference<>(KafkaStreams.State.RUNNING);
        Thread drainer = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            quiesce.setDrained("0_0", true);
        });
        drainer.start();

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, state::get),
                "awaitDrain must keep polling a RUNNING instance and return once its task drains");
    }
}
