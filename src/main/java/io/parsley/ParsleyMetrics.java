package io.parsley;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.util.List;
import java.util.Map;

/**
 * Callback interface through which {@link ParsleyEngine} reports observable events. Every causal
 * processor node wires a {@link #wire} backed implementation; callers that do not need metrics
 * receive {@link #NOOP}.
 */
interface ParsleyMetrics {

    /**
     * A record was added to the causal buffer.
     *
     * @param newBufferDepth the buffer depth after the add
     */
    void recordBuffered(int newBufferDepth);

    /**
     * One or more records were released from the buffer (their dependencies are now satisfied).
     *
     * @param count          the number of records released in this drain pass
     * @param newBufferDepth the buffer depth after the release
     */
    void recordReleased(int count, int newBufferDepth);

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

    ParsleyMetrics NOOP = new ParsleyMetrics() {
        @Override public void recordBuffered(int d) {}
        @Override public void recordReleased(int c, int d) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() {}
        @Override public void recordDeserializationError() {}
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

        Sensor buffered  = sm.addRateTotalSensor("parsley", taskId, "records-buffered",  Sensor.RecordingLevel.INFO);
        Sensor released  = sm.addRateTotalSensor("parsley", taskId, "records-released",  Sensor.RecordingLevel.INFO);
        Sensor evicted   = sm.addRateTotalSensor("parsley", taskId, "records-evicted",   Sensor.RecordingLevel.INFO);
        Sensor violation = sm.addRateTotalSensor("parsley", taskId, "violations",         Sensor.RecordingLevel.INFO);
        Sensor deserErr  = sm.addRateTotalSensor("parsley", taskId, "deserialization-errors", Sensor.RecordingLevel.INFO);

        Sensor depth = sm.addSensor("parsley-buffer-depth-" + taskId, Sensor.RecordingLevel.INFO);
        depth.add(new MetricName("buffer-depth", "stream-parsley-metrics",
                "Current number of records held in the causal buffer",
                Map.of("parsley-id", taskId)), new Value());

        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void recordBuffered(int d)       { buffered.record();   depth.record(d); }
            @Override public void recordReleased(int c, int d){ released.record(c);  depth.record(d); }
            @Override public void recordEvicted(int c)        { evicted.record(c);   depth.record(0); }
            @Override public void recordViolation()           { violation.record(); }
            @Override public void recordDeserializationError(){ deserErr.record(); }
        };

        return new Wired(metrics, List.of(buffered, released, evicted, violation, deserErr, depth));
    }
}
