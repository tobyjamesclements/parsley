package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link ParsleyQuiesce}'s registration/readiness bookkeeping directly, independent of any
 * Kafka Streams topology.
 */
class ParsleyQuiesceTest {

    private static final String TASK_0 = "0_0";
    private static final String TASK_1 = "0_1";

    /**
     * With no task ever registered, {@code isSafeToClose} tracks the quiesce request alone: false until
     * requested, true once requested. An instance that owns no tasks (more instances than partitions, or
     * every task migrated away) holds nothing that could be stranded, so it is trivially safe to close —
     * requiring a registered task here would hang {@link CausalStreams#close()} forever on such an
     * instance.
     *
     * Asserts {@code isSafeToClose} is false before requesting quiesce and true after, with no task
     * registered.
     */
    @Test
    void isSafeToCloseWithNoRegisteredTasksTracksOnlyTheQuiesceRequest() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        assertFalse(quiesce.isSafeToClose(), "quiesce not requested yet, so not safe to close");

        quiesce.requestQuiesce();
        assertTrue(quiesce.isSafeToClose(),
                "no registered task can strand nothing — a zero-task instance is safe once quiesce is "
                        + "requested, rather than hanging close() forever");
    }

    /**
     * A registered, empty-buffer (drained) task is not "safe to close" until quiesce has actually
     * been requested — an idle task should not report the whole application shutdown-ready on its
     * own.
     *
     * Asserts {@code isSafeToClose} is false before {@code requestQuiesce}, true after.
     */
    @Test
    void isSafeToCloseRequiresQuiesceToBeRequested() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.register(TASK_0);
        quiesce.setDrained(TASK_0, true);

        assertFalse(quiesce.isSafeToClose(), "drained but quiesce was never requested");

        quiesce.requestQuiesce();
        assertTrue(quiesce.isSafeToClose(), "requested and every registered task is drained");
    }

    /**
     * With two registered tasks, {@code isSafeToClose} must wait for BOTH to drain — one drained
     * task must not make the whole application appear shutdown-ready while another still holds
     * buffered records.
     *
     * Asserts {@code isSafeToClose} is false while task 1 is undrained, true once it drains too.
     */
    @Test
    void isSafeToCloseWaitsForEveryRegisteredTask() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.register(TASK_0);
        quiesce.register(TASK_1);
        quiesce.requestQuiesce();
        quiesce.setDrained(TASK_0, true);

        assertFalse(quiesce.isSafeToClose(), "task 1 has not drained yet");

        quiesce.setDrained(TASK_1, true);
        assertTrue(quiesce.isSafeToClose(), "both registered tasks are now drained");
    }

    /**
     * A task's drained state can toggle back to false — e.g. a new record arrives and is held again
     * after previously draining to zero — and {@code isSafeToClose} must reflect that immediately.
     *
     * Asserts {@code isSafeToClose} flips back to false when a previously-drained task un-drains.
     */
    @Test
    void isSafeToCloseFlipsBackWhenATaskUnDrains() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.register(TASK_0);
        quiesce.requestQuiesce();
        quiesce.setDrained(TASK_0, true);
        assertTrue(quiesce.isSafeToClose(), "the only registered task is drained");

        quiesce.setDrained(TASK_0, false);
        assertFalse(quiesce.isSafeToClose(), "the task is holding a record again");
    }

    /**
     * Unregistering a task (its own {@code close()}) removes it from consideration — a task that has
     * left the topology (e.g. a rebalance) must not permanently block {@code isSafeToClose} for the
     * tasks that remain.
     *
     * Asserts {@code isSafeToClose} becomes true for the remaining drained task once the undrained
     * one unregisters.
     */
    @Test
    void unregisteringATaskRemovesItFromConsideration() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.register(TASK_0);
        quiesce.register(TASK_1);
        quiesce.requestQuiesce();
        quiesce.setDrained(TASK_0, true);

        assertFalse(quiesce.isSafeToClose(), "task 1 is still registered and undrained");

        quiesce.unregister(TASK_1);
        assertTrue(quiesce.isSafeToClose(), "only task 0 remains registered, and it is drained");
    }
}
