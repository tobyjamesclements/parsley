package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Callback interface through which {@link ParsleyEngine} reports observable events. Every causal
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
     * One or more records were evicted because a {@link CausalBufferLimit} fired.
     *
     * @param count the number of records evicted
     */
    void recordEvicted(int count);

    /** A record was evicted from the causal buffer before its dependencies were satisfied. */
    void recordViolation();

    /**
     * A held record could not be deserialised from the buffer store on the forward path (e.g. an
     * incompatible Schema Registry change after buffering). The record stays buffered; this counts
     * the failed attempt.
     */
    void recordDeserializationError();

    /**
     * An inbound record's {@code parsley-causal-dependencies} header could not be decoded into a clock
     * at ingest (a corrupt or truncated header). This counts the occurrence regardless of whether
     * {@code parsley.clock.resolution.failure.policy} then failed the task or forwarded empty.
     */
    void recordClockResolutionError();

    /**
     * A {@link CausalBufferLimit} fired on a held record and {@code parsley.buffer.eviction.failure.policy
     * = fail} (the default) failed the task fast instead of evicting it. The record stays buffered;
     * this counts the failed attempt.
     */
    void recordEvictionLimitExceeded();

    /**
     * Reports the buffer's current observable state. Called after every depth-changing event and,
     * independently, on a periodic refresh tick — so the oldest-record gauge stays current even on a
     * buffer that is idle (no admits, releases, or evictions) between ticks.
     *
     * @param depth             the current number of records held in the buffer
     * @param oldestBufferedAtMs the oldest held record's buffer-admission time (epoch millis), or
     *                          empty if the buffer is empty
     */
    void reportState(int depth, OptionalLong oldestBufferedAtMs);

    ParsleyMetrics NOOP = new ParsleyMetrics() {
        @Override public void recordBuffered() {}
        @Override public void recordReleased(int c) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() {}
        @Override public void recordDeserializationError() {}
        @Override public void recordClockResolutionError() {}
        @Override public void recordEvictionLimitExceeded() {}
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
     *
     * <p>{@code sizeLimit} and {@code durationLimit} are the buffer's configured {@link
     * CausalBufferLimit}, resolved by the caller before the {@link ParsleyEngine} exists (so they are
     * known to {@link ParsleyEngine#sizeLimitOf} / {@link ParsleyEngine#durationLimitOf}). Each is
     * registered as a constant gauge only when that limit kind is actually configured, so a gap to a
     * limit can be computed downstream (e.g. {@code buffer-size-limit - buffer-depth}) without pushing
     * a precomputed value that goes stale the instant after it is recorded.
     */
    static Wired wire(ProcessorContext<?, ?> context, Optional<Integer> sizeLimit, Optional<Duration> durationLimit) {
        StreamsMetrics sm = context.metrics();
        String taskId = context.taskId().toString();

        Sensor buffered  = sm.addRateTotalSensor("parsley", taskId, "records-buffered",  Sensor.RecordingLevel.INFO);
        Sensor released  = sm.addRateTotalSensor("parsley", taskId, "records-released",  Sensor.RecordingLevel.INFO);
        Sensor evicted   = sm.addRateTotalSensor("parsley", taskId, "records-evicted",   Sensor.RecordingLevel.INFO);
        Sensor violation = sm.addRateTotalSensor("parsley", taskId, "violations",         Sensor.RecordingLevel.INFO);
        Sensor deserErr  = sm.addRateTotalSensor("parsley", taskId, "deserialization-errors", Sensor.RecordingLevel.INFO);
        Sensor clockResErr = sm.addRateTotalSensor("parsley", taskId, "clock-resolution-errors", Sensor.RecordingLevel.INFO);
        Sensor evictionLimitExceeded = sm.addRateTotalSensor("parsley", taskId, "eviction-limit-exceeded", Sensor.RecordingLevel.INFO);

        Sensor depth = gauge(sm, taskId, "buffer-depth",
                "Current number of records held in the causal buffer");
        Sensor oldestBufferedAt = gauge(sm, taskId, "buffer-oldest-buffered-at-ms",
                "Buffer-admission time (epoch millis) of the oldest held record, or 0 if the buffer is empty");

        List<Sensor> sensors = new ArrayList<>(List.of(buffered, released, evicted, violation, deserErr,
                clockResErr, evictionLimitExceeded, depth, oldestBufferedAt));

        sizeLimit.ifPresent(limit -> {
            Sensor sizeLimitGauge = gauge(sm, taskId, "buffer-size-limit",
                    "Configured maximum buffer size (message count) before the size limit evicts");
            sizeLimitGauge.record(limit);
            sensors.add(sizeLimitGauge);
        });
        durationLimit.ifPresent(duration -> {
            Sensor durationLimitGauge = gauge(sm, taskId, "buffer-duration-limit-ms",
                    "Configured maximum buffer duration (millis) before the duration limit evicts");
            durationLimitGauge.record(duration.toMillis());
            sensors.add(durationLimitGauge);
        });

        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void recordBuffered()             { buffered.record(); }
            @Override public void recordReleased(int c)        { released.record(c); }
            @Override public void recordEvicted(int c)         { evicted.record(c); }
            @Override public void recordViolation()            { violation.record(); }
            @Override public void recordDeserializationError() { deserErr.record(); }
            @Override public void recordClockResolutionError() { clockResErr.record(); }
            @Override public void recordEvictionLimitExceeded() { evictionLimitExceeded.record(); }
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
