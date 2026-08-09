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
     * Starts every given process and returns once each is running or has been refused.
     *
     * @param config    broker connection, application identity and metadata budget
     * @param processes the processes to run, at least one
     * @return a handle owning the running processes
     * @throws io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException
     *         if a process cannot start without breaching the guarantee, for example when
     *         messages remain held on a channel the definition no longer receives
     * @throws IllegalArgumentException if the definitions conflict or name no process
     */
    public static Parsley start(ParsleyConfig config, ProcessDefinition... processes) {
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
