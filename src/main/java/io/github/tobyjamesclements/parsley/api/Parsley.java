package io.github.tobyjamesclements.parsley.api;

import java.util.List;

import io.github.tobyjamesclements.parsley.kafka.ParsleyRuntime;

/**
 * Entry point: start a declared application (SPEC Structural 17). Parsley owns the Kafka Streams lifecycle end to
 * end; there is no way to obtain the topology or run it under weaker settings (SPEC Structural 9, Substrate 3). The
 * API offers no timers and no way to cause a delivery other than a channel's messages (SPEC Structural 10) — periodic
 * work belongs to applications sending messages from outside.
 */
public final class Parsley implements AutoCloseable {

    private final ParsleyRuntime runtime;

    private Parsley(ParsleyRuntime runtime) {
        this.runtime = runtime;
    }

    public static Parsley start(ParsleyConfig config, ProcessDefinition... processes) {
        return new Parsley(ParsleyRuntime.start(config, List.of(processes)));
    }

    /** True while every declared process is running. A process that failed closed stays down until an operator acts. */
    public boolean healthy() {
        return runtime.healthy();
    }

    /** Per-process state and stop reason (SPEC Operational 1): a deliberate refusal — which recurs identically on
     * restart — carries its reason; a transient failure carries only its detail. */
    public java.util.Map<String, ProcessStatus> status() {
        return runtime.status();
    }

    @Override
    public void close() {
        runtime.close();
    }
}
