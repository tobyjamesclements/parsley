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
     * A record was removed from the causal execution path onto the dead-letter sink (poison,
     * unresolvable-clock, or an orphan-cascade victim of either). This counts the occurrence.
     */
    void recordDeadLetter();

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
        @Override public void recordDeadLetter() {}
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
        Sensor deadLettered = sm.addRateTotalSensor("parsley", taskId, "dead-lettered", Sensor.RecordingLevel.INFO);

        Sensor depth = gauge(sm, parsleyId, "buffer-depth",
                "Current number of records held in the causal buffer");
        Sensor oldestBufferedAt = gauge(sm, parsleyId, "buffer-oldest-buffered-at-ms",
                "Buffer-admission time (epoch millis) of the oldest held record, or 0 if the buffer is empty");

        List<Sensor> sensors = new ArrayList<>(List.of(buffered, released, deserErr,
                clockResErr, deadLettered, depth, oldestBufferedAt));

        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void recordBuffered()             { buffered.record(); }
            @Override public void recordReleased(int c)        { released.record(c); }
            @Override public void recordDeserializationError() { deserErr.record(); }
            @Override public void recordClockResolutionError() { clockResErr.record(); }
            @Override public void recordDeadLetter()           { deadLettered.record(); }
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
