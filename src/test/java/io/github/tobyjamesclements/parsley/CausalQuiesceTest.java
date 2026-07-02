package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CausalQuiesce}'s registration/readiness bookkeeping directly, independent of any
 * Kafka Streams topology.
 */
class CausalQuiesceTest {

    private static final TaskId TASK_0 = new TaskId(0, 0);
    private static final TaskId TASK_1 = new TaskId(0, 1);

    /**
     * With no task ever registered, {@code isSafeToClose} must never report {@code true} — an empty
     * registered set would otherwise vacuously satisfy "every registered task is drained".
     *
     * Asserts {@code isSafeToClose} is false both before and after requesting quiesce.
     */
    @Test
    void isSafeToCloseIsFalseWithNoRegisteredTasks() {
        CausalQuiesce quiesce = CausalQuiesce.create();
        assertFalse(quiesce.isSafeToClose(), "no task has registered yet");

        quiesce.requestQuiesce();
        assertFalse(quiesce.isSafeToClose(), "still no registered task, even after requesting quiesce");
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
        CausalQuiesce quiesce = CausalQuiesce.create();
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
        CausalQuiesce quiesce = CausalQuiesce.create();
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
        CausalQuiesce quiesce = CausalQuiesce.create();
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
        CausalQuiesce quiesce = CausalQuiesce.create();
        quiesce.register(TASK_0);
        quiesce.register(TASK_1);
        quiesce.requestQuiesce();
        quiesce.setDrained(TASK_0, true);

        assertFalse(quiesce.isSafeToClose(), "task 1 is still registered and undrained");

        quiesce.unregister(TASK_1);
        assertTrue(quiesce.isSafeToClose(), "only task 0 remains registered, and it is drained");
    }
}
