package io.github.tobyjamesclements.parsley;

import java.util.Optional;


/**
 * The state of one process at a moment in time.
 *
 * @param name          the process name, as declared
 * @param lifecycle     whether the process is running, rebalancing or stopped
 * @param refusalReason present when the process stopped to preserve the guarantee, absent
 *                      when it is running or stopped for another cause
 * @param failureDetail the diagnosis accompanying a stop, when one is available
 * @see Parsley#status()
 */
public record ProcessStatus(
        String name,
        Lifecycle lifecycle,
        Optional<FailClosedException.Reason> refusalReason,
        Optional<String> failureDetail) {

    /**
     * Refuses null components.
     *
     * @throws IllegalArgumentException if any component is null; absence is expressed
     *         through the empty {@code Optional}s
     */
    public ProcessStatus {
        if (name == null || lifecycle == null || refusalReason == null || failureDetail == null) {
            throw new IllegalArgumentException("every component of a status must be non-null");
        }
    }

    /** Where a process is in its lifecycle. */
    public enum Lifecycle {
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
    public boolean refused() {
        return refusalReason.isPresent();
    }
}
