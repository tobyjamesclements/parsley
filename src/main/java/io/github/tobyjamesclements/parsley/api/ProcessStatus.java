package io.github.tobyjamesclements.parsley.api;

import java.util.List;
import java.util.Optional;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * The state of one process at a moment in time.
 *
 * @param process        the process name, as declared
 * @param state          whether the process is running, rebalancing or stopped
 * @param refusalReason  present when the process stopped to preserve the guarantee, absent
 *                       when it is running or stopped for another cause
 * @param failureDetail  the diagnosis accompanying a stop, when one is available
 * @param tasks          the delivery state of each task this instance currently runs, in
 *                       partition order: what is held, what it waits for, and how much
 *                       causal metadata it carries; empty before any task has initialised
 *                       or after every task has closed
 * @see Parsley#status()
 * @see TaskStatus
 */
public record ProcessStatus(
        String process,
        State state,
        Optional<ParsleyFailClosedException.Reason> refusalReason,
        Optional<String> failureDetail,
        List<TaskStatus> tasks) {

    /**
     * Copies the tasks.
     *
     * @throws IllegalArgumentException if any component is null; absence is expressed
     *         through the empty {@code Optional}s and the empty task list
     */
    public ProcessStatus {
        if (process == null || state == null || refusalReason == null || failureDetail == null || tasks == null) {
            throw new IllegalArgumentException("every component of a status must be non-null");
        }
        if (tasks.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("tasks must not contain null");
        }
        tasks = List.copyOf(tasks);
    }

    /**
     * A status with no task detail, as reported before any task has initialised.
     *
     * @param process       the process name, as declared
     * @param state         whether the process is running, rebalancing or stopped
     * @param refusalReason present when the process stopped to preserve the guarantee
     * @param failureDetail the diagnosis accompanying a stop, when one is available
     */
    public ProcessStatus(String process, State state, Optional<ParsleyFailClosedException.Reason> refusalReason,
                         Optional<String> failureDetail) {
        this(process, state, refusalReason, failureDetail, List.of());
    }

    /** Lifecycle state of a process. */
    public enum State {
        /** Delivering, or waiting for causes to arrive. */
        RUNNING,
        /** Partition assignment is in flux; delivery resumes when it settles. */
        REBALANCING,
        /** No longer delivering. */
        STOPPED
    }

    /**
     * Distinguishes a stop taken to preserve the guarantee from any other stop.
     *
     * @return {@code true} when a {@link #refusalReason()} is present
     */
    public boolean stoppedDeliberately() {
        return refusalReason.isPresent();
    }
}
