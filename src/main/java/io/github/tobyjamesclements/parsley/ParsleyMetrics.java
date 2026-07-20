package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Callback interface through which {@link ParsleyCausalBroadcast} reports observable events. Every causal
 * processor node wires a {@link #wire} backed implementation; callers that do not need metrics
 * receive {@link #NOOP}.
 */
interface ParsleyMetrics {

    /** A record was added to the causal buffer. */
    void recordBuffered();

    /**
     * One or more records were released from the buffer (their dependencies are now satisfied).
     *
     * @param count the number of records released in this drain pass
     */
    void recordReleased(int count);

    /**
     * A held record could not be deserialised from the buffer store on the forward path (e.g. an
     * incompatible Schema Registry change after buffering). This counts the failed attempt.
     */
    void recordDeserializationError();

    /**
     * An inbound record's {@code parsley-causal-dependencies} header could not be decoded into a clock
     * at ingest (a corrupt or truncated header). This counts the occurrence.
     */
    void recordClockResolutionError();

    /**
     * A record's dependencies named a coordinate this node has no input channel for — the task fails
     * fast. This counts the occurrence.
     */
    void recordUnreachableDependencyError();

    /**
     * A received record's offset was already delivered here (at or below the contiguous frontier, or
     * still marked in the forwarded index) and was skipped instead of being forwarded to the delegate
     * again. Routine while an added input's re-fetched prefix is replayed past the carried-ancestry
     * seed ({@code ParsleyChannels#rescope}); otherwise a redelivery {@code exactly_once_v2} should
     * make impossible — sustained counts outside a scope-change replay warrant investigation.
     */
    void recordReplaySkipped();

    /**
     * An inbound clock claimed one of this node's own sink coordinates above the {@code ownOutputs}
     * clock, with pending producer acks already folded (I8). A truthful reflected claim can only name
     * a position this node itself produced, so a claim above the own-output view means that view is
     * stale beyond what the init-time end-offset seed healed, or a peer's stamp is not truthful.
     * Diagnostic only — never a failure (O3 dissolved): the gate treats the claim like any other,
     * and a stale view only ever delays delivery, never reorders it.
     */
    void recordReflectedClaimAboveOwnOutputs();

    /**
     * Reports how many held records have waited beyond the stall threshold on a consumed-channel
     * dependency above that channel's highest physically received offset (T3.0 A9) — the signature
     * of a claim nothing received so far can satisfy, whose delay is unbounded if the claimed
     * channel stays silent. Fail-safe, never unsafe; reported as a gauge so a persistent stall is
     * visible without log-scraping.
     *
     * @param count the number of held records currently stalled past the threshold
     */
    void reportHeldAboveHighestReceived(int count);

    /**
     * Reports the buffer's current observable state. Called after every depth-changing event and,
     * independently, on a periodic refresh tick — so the oldest-record gauge stays current even on a
     * buffer that is idle (no admits or releases) between ticks.
     *
     * @param depth             the current number of records held in the buffer
     * @param oldestBufferedAtMs the oldest held record's buffer-admission time (epoch millis), or
     *                          empty if the buffer is empty
     */
    void reportState(int depth, OptionalLong oldestBufferedAtMs);

    ParsleyMetrics NOOP = new ParsleyMetrics() {
        @Override public void recordBuffered() {}
        @Override public void recordReleased(int c) {}
        @Override public void recordDeserializationError() {}
        @Override public void recordClockResolutionError() {}
        @Override public void recordUnreachableDependencyError() {}
        @Override public void recordReplaySkipped() {}
        @Override public void recordReflectedClaimAboveOwnOutputs() {}
        @Override public void reportHeldAboveHighestReceived(int count) {}
        @Override public void reportState(int depth, OptionalLong oldestBufferedAtMs) {}
    };

    /**
     * A {@link ParsleyMetrics} implementation bundled with the {@link Sensor}s it registered, so the
     * owning processor can remove them again on {@code close()}.
     */
    record Wired(ParsleyMetrics metrics, List<Sensor> sensors) {
        void close(StreamsMetrics streamsMetrics) {
            for (Sensor sensor : sensors) {
                streamsMetrics.removeSensor(sensor);
            }
        }
    }

    /**
     * Registers the Kafka Streams {@link Sensor}s backing a {@link ParsleyMetrics} for the task
     * owning {@code context}, namespaced under {@code "parsley"} and the task ID.
     */
    static Wired wire(ProcessorContext<?, ?> context) {
        StreamsMetrics sm = context.metrics();
        String taskId = context.taskId().toString();
        String parsleyId = context.applicationId() + "-" + taskId;

        Sensor buffered  = sm.addRateTotalSensor("parsley", taskId, "records-buffered",  Sensor.RecordingLevel.INFO);
        Sensor released  = sm.addRateTotalSensor("parsley", taskId, "records-released",  Sensor.RecordingLevel.INFO);
        Sensor deserErr  = sm.addRateTotalSensor("parsley", taskId, "deserialization-errors", Sensor.RecordingLevel.INFO);
        Sensor clockResErr = sm.addRateTotalSensor("parsley", taskId, "clock-resolution-errors", Sensor.RecordingLevel.INFO);
        Sensor unreachableDepErr = sm.addRateTotalSensor("parsley", taskId, "unreachable-dependency-errors",
                Sensor.RecordingLevel.INFO);
        Sensor replaySkipped = sm.addRateTotalSensor("parsley", taskId, "replays-skipped",
                Sensor.RecordingLevel.INFO);
        Sensor reflectedAboveOwn = sm.addRateTotalSensor("parsley", taskId,
                "reflected-claims-above-own-outputs", Sensor.RecordingLevel.INFO);

        Sensor depth = gauge(sm, parsleyId, "buffer-depth",
                "Current number of records held in the causal buffer");
        Sensor oldestBufferedAt = gauge(sm, parsleyId, "buffer-oldest-buffered-at-ms",
                "Buffer-admission time (epoch millis) of the oldest held record, or 0 if the buffer is empty");
        Sensor heldAboveReceived = gauge(sm, parsleyId, "records-held-above-highest-received",
                "Held records waiting past the stall threshold on a dependency above its channel's "
                        + "highest received offset — nothing received so far can satisfy the claim");

        List<Sensor> sensors = new ArrayList<>(List.of(buffered, released, deserErr,
                clockResErr, unreachableDepErr, replaySkipped, reflectedAboveOwn, depth,
                oldestBufferedAt, heldAboveReceived));

        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void recordBuffered()             { buffered.record(); }
            @Override public void recordReleased(int c)        { released.record(c); }
            @Override public void recordDeserializationError() { deserErr.record(); }
            @Override public void recordClockResolutionError() { clockResErr.record(); }
            @Override public void recordUnreachableDependencyError() { unreachableDepErr.record(); }
            @Override public void recordReplaySkipped()        { replaySkipped.record(); }
            @Override public void recordReflectedClaimAboveOwnOutputs() { reflectedAboveOwn.record(); }
            @Override public void reportHeldAboveHighestReceived(int count) { heldAboveReceived.record(count); }
            @Override public void reportState(int d, OptionalLong oldest) {
                depth.record(d);
                oldestBufferedAt.record(oldest.isPresent() ? oldest.getAsLong() : 0L);
            }
        };

        return new Wired(metrics, sensors);
    }

    private static Sensor gauge(StreamsMetrics sm, String taskId, String name, String description) {
        Sensor sensor = sm.addSensor("parsley-" + name + "-" + taskId, Sensor.RecordingLevel.INFO);
        sensor.add(new MetricName(name, "stream-parsley-metrics", description,
                Map.of("parsley-id", taskId)), new Value());
        return sensor;
    }
}
