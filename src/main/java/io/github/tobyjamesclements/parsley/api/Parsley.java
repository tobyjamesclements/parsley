package io.github.tobyjamesclements.parsley.api;

import java.util.List;

import io.github.tobyjamesclements.parsley.kafka.ParsleyRuntime;

/**
 * A running set of processes, each executing under causal delivery order.
 *
 * <p>Every {@link ProcessDefinition} passed to {@link #start} runs as its own Kafka Streams
 * application under {@code exactly_once_v2} and {@code read_committed}. A process that cannot
 * uphold the delivery guarantee stops rather than weakening it, and stays stopped until an
 * operator intervenes.
 *
 * @see ProcessDefinition
 * @see ParsleyConfig
 */
public final class Parsley implements AutoCloseable {
    private final ParsleyRuntime runtime;

    private Parsley(ParsleyRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Validates every definition, resolves every received and sent topic, establishes each
     * process's initial read positions, then starts every process and returns.
     *
     * <p>Every declared topic must already exist; nothing is created. The start is
     * all-or-nothing: a refusal for any process leaves nothing running and is thrown. The
     * call returns once each process's Kafka Streams application has been started, which is
     * the beginning of its first rebalance, not the end of it — poll {@link #status()} or
     * wait with {@link #awaitStopped()}. A refusal raised inside task initialisation on the
     * host's threads surfaces through {@link #status()}.
     *
     * @param config    broker connection, application identity and metadata budget
     * @param processes the processes to run, at least one
     * @return a handle owning the running processes
     * @throws io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException
     *         if a process cannot start without breaching the guarantee, for example when
     *         messages remain held on a channel the definition no longer receives
     * @throws IllegalArgumentException if {@code config}, {@code processes} or an element
     *         is null, or the definitions conflict or name no process
     * @throws IllegalStateException if the cluster could not be queried, or a declared topic
     *         does not exist
     */
    public static Parsley start(ParsleyConfig config, ProcessDefinition... processes) {
        if (config == null) {
            throw new IllegalArgumentException("config must be non-null");
        }
        if (processes == null) {
            throw new IllegalArgumentException("processes must be non-null");
        }
        for (ProcessDefinition process : processes) {
            if (process == null) {
                throw new IllegalArgumentException("processes must not contain a null element");
            }
        }
        return new Parsley(ParsleyRuntime.start(config, List.of(processes)));
    }

    /**
     * Reports whether every process is still running.
     *
     * @return {@code false} once any process has stopped, deliberately or otherwise
     * @see #status()
     */
    public boolean healthy() {
        return runtime.healthy();
    }

    /**
     * Reports the current state of each process, keyed by process name.
     *
     * <p>This is the diagnosis surface when {@link #healthy()} turns false:
     * {@link ProcessStatus#refusalReason()} distinguishes a deliberate stop from a failure.
     *
     * @return a snapshot of every process, never empty
     */
    public java.util.Map<String, ProcessStatus> status() {
        return runtime.status();
    }

    /**
     * Waits until a process stops, or this handle is closed.
     *
     * <p>{@link #start} returns as soon as every process has been started, so an application
     * whose work is its processes has nothing else to do but wait here. The wait ends when
     * any process stops — deliberately, to preserve the guarantee, or otherwise — which is
     * the moment to read {@link #status()} and act, or when another thread calls
     * {@link #close()}. The wait ends as soon as the stop is known; the host's own shutdown
     * may still be completing, so {@link #status()} can report the process as running for a
     * moment longer before it settles on the stopped state and its reason.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void awaitStopped() throws InterruptedException {
        runtime.awaitStopped();
    }

    /**
     * Waits, for at most {@code timeout}, until a process stops or this handle is closed.
     *
     * @param timeout how long to wait
     * @return {@code true} if a process stopped or the handle was closed within the timeout,
     *         {@code false} if every process was still running when it elapsed
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws IllegalArgumentException if {@code timeout} is null or negative
     */
    public boolean awaitStopped(java.time.Duration timeout) throws InterruptedException {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must be non-null");
        }
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        return runtime.awaitStopped(timeout);
    }

    /**
     * Stops every process and releases its resources.
     *
     * <p>Each resource is released independently, so a failure to release one cannot strand
     * the others. Closing is bounded in time.
     */
    @Override
    public void close() {
        runtime.close();
    }
}
