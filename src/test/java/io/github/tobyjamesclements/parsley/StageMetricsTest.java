package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Directed tests for the metric surface, driven against a real {@link CausalNode} and a real
 * client-metrics registry behind a hand-rolled {@link StreamsMetrics} seam. Time is passed
 * explicitly to {@link StageMetrics#sample}, so every age assertion is deterministic. The
 * adapter's registration and punctuated sampling under the driver are covered in
 * {@link StageTest}.
 */
class StageMetricsTest {

    private static final UUID TOPIC_1 = UUID.nameUUIDFromBytes("mt1".getBytes());
    private static final UUID TOPIC_2 = UUID.nameUUIDFromBytes("mt2".getBytes());
    private static final UUID TOPIC_3 = UUID.nameUUIDFromBytes("mt3".getBytes());
    private static final UUID SINK = UUID.nameUUIDFromBytes("msink".getBytes());
    private static final Channel C1 = new Channel(TOPIC_1, 0);
    private static final Channel C2 = new Channel(TOPIC_2, 0);
    private static final Channel C3 = new Channel(TOPIC_3, 0);
    private static final UUID UPSTREAM = UUID.nameUUIDFromBytes("upstream".getBytes());

    private static final BrokerOffsets NO_OFFSETS = new BrokerOffsets() {
        @Override
        public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
            return Map.of();
        }

        @Override
        public EarliestOffsets earliestOffsets(Set<Channel> channels) {
            return new EarliestOffsets(Map.of(), Set.of());
        }
    };

    /**
     * {@link StreamsMetrics} over a plain client-metrics registry: the three members the
     * metric surface uses, delegated; the rate-total conveniences are unused by it.
     */
    private static final class RegistryStreamsMetrics implements StreamsMetrics {
        final Metrics registry;

        RegistryStreamsMetrics(Sensor.RecordingLevel level) {
            registry = new Metrics(new MetricConfig().recordLevel(level));
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return registry.metrics();
        }

        @Override
        public Sensor addSensor(String name, Sensor.RecordingLevel recordingLevel) {
            return registry.sensor(name, recordingLevel);
        }

        @Override
        public Sensor addSensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
            return registry.sensor(name, recordingLevel, parents);
        }

        @Override
        public void removeSensor(Sensor sensor) {
            registry.removeSensor(sensor.name());
        }

        @Override
        public Sensor addLatencyRateTotalSensor(String scopeName, String entityName,
                                                String operationName, Sensor.RecordingLevel recordingLevel,
                                                String... tags) {
            throw new UnsupportedOperationException("unused by StageMetrics");
        }

        @Override
        public Sensor addRateTotalSensor(String scopeName, String entityName, String operationName,
                                         Sensor.RecordingLevel recordingLevel, String... tags) {
            throw new UnsupportedOperationException("unused by StageMetrics");
        }
    }

    private static CausalNode node() {
        return new CausalNode(
                new NodeConfig("metrics-test", UUID.nameUUIDFromBytes("self".getBytes()),
                        Set.of(C1, C2), Set.of(SINK), 0),
                new SimStateStore(), NO_OFFSETS);
    }

    private static StageMetrics metricsFor(RegistryStreamsMetrics registry, CausalNode node) {
        return new StageMetrics(registry, "p1", "0_0", Map.of(C1, "c1", C2, "c2"), node);
    }

    private static InboundRecord record(Channel c, long offset, VectorClock clock) {
        return new InboundRecord(c, offset, clock, null, -1, null, null, 1000);
    }

    /** The value of the group's metric {@code name}; task-level when {@code topic} is null. */
    private static double value(RegistryStreamsMetrics registry, String name, String topic) {
        List<Double> matches = registry.metrics().entrySet().stream()
                .filter(e -> e.getKey().group().equals(StageMetrics.GROUP))
                .filter(e -> e.getKey().name().equals(name))
                .filter(e -> topic == null ? !e.getKey().tags().containsKey("topic")
                        : topic.equals(e.getKey().tags().get("topic")))
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .toList();
        assertEquals(1, matches.size(),
                "expected exactly one metric named " + name + " for topic " + topic);
        return matches.get(0);
    }

    private static boolean exists(RegistryStreamsMetrics registry, String name, String topic) {
        return registry.metrics().keySet().stream()
                .filter(m -> m.group().equals(StageMetrics.GROUP) && m.name().equals(name))
                .anyMatch(m -> topic == null ? !m.tags().containsKey("topic")
                        : topic.equals(m.tags().get("topic")));
    }

    /** Registration puts the whole documented metric set on the registry, initially zero. */
    @Test
    void registersTheDocumentedMetricSet() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.DEBUG);
        metricsFor(registry, node());

        for (String gauge : List.of("records-held", "records-held-age-max-ms",
                "stamp-offset-entries", "stamp-sequence-entries")) {
            assertEquals(0.0, value(registry, gauge, null), gauge + " must register at zero");
        }
        for (String total : List.of("records-delivered-total", "replays-skipped-total",
                "truncation-sweeps-skipped-total")) {
            assertEquals(0.0, value(registry, total, null), total + " must register at zero");
        }
        assertTrue(exists(registry, "records-delivered-rate", null),
                "the delivered rate must register");
        for (String topic : List.of("c1", "c2")) {
            assertEquals(0.0, value(registry, "records-held", topic),
                    "the per-topic depth gauge must register for " + topic);
            assertEquals(0.0, value(registry, "records-held-age-max-ms", topic),
                    "the per-topic age gauge must register for " + topic);
        }
    }

    /** Hold depth and age gauge a held record, and both return to zero on release. */
    @Test
    void gaugesTrackHeldDepthAndAge() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.INFO);
        CausalNode node = node();
        StageMetrics metrics = metricsFor(registry, node);

        assertTrue(node.onRecord(record(C2, 0, VectorClock.of(C1, 5))).isEmpty(),
                "the record claims c1@5 ahead of the frontier, so it must hold");
        metrics.sample(1_000);
        assertEquals(1.0, value(registry, "records-held", null),
                "one held record must gauge as depth one");
        assertEquals(0.0, value(registry, "records-held-age-max-ms", null),
                "age counts from the head's first observation");

        metrics.sample(3_000);
        assertEquals(2_000.0, value(registry, "records-held-age-max-ms", null),
                "the held head must age with the sampling clock");

        for (long o = 0; o <= 5; o++) {
            node.onRecord(record(C1, o, null));
        }
        metrics.sample(4_000);
        assertEquals(0.0, value(registry, "records-held", null),
                "the released record must leave the depth gauge");
        assertEquals(0.0, value(registry, "records-held-age-max-ms", null),
                "a drained queue has no age");
    }

    /** A delivered head hands the age clock to the next head, which ages from zero. */
    @Test
    void headChangeRestartsTheAgeClock() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.DEBUG);
        CausalNode node = node();
        StageMetrics metrics = metricsFor(registry, node);

        node.onRecord(record(C2, 0, VectorClock.of(C1, 0)));
        node.onRecord(record(C2, 1, VectorClock.of(C1, 1)));
        metrics.sample(1_000);
        metrics.sample(2_000);
        assertEquals(2.0, value(registry, "records-held", "c2"),
                "both records hold on the c2 channel");
        assertEquals(0.0, value(registry, "records-held", "c1"),
                "the c1 channel holds nothing");
        assertEquals(1_000.0, value(registry, "records-held-age-max-ms", null),
                "the first head ages from its first observation");

        assertEquals(2, node.onRecord(record(C1, 0, null)).size(),
                "c1@0 delivers and releases the first held record");
        metrics.sample(2_500);
        assertEquals(1.0, value(registry, "records-held", "c2"),
                "one record must remain held");
        assertEquals(0.0, value(registry, "records-held-age-max-ms", null),
                "the new head's age must restart at its first observation");

        metrics.sample(4_000);
        assertEquals(1_500.0, value(registry, "records-held-age-max-ms", "c2"),
                "the new head must age from its own start");
    }

    /** Delivered batches accumulate into the total. */
    @Test
    void deliveredBatchesAccumulate() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.INFO);
        StageMetrics metrics = metricsFor(registry, node());

        metrics.recordDelivered(3);
        metrics.recordDelivered(2);
        assertEquals(5.0, value(registry, "records-delivered-total", null),
                "batch sizes must accumulate into the delivered total");
    }

    /** The replay counter drains as deltas: totals accumulate and never double-count. */
    @Test
    void replaysDrainAsDeltas() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.INFO);
        CausalNode node = node();
        StageMetrics metrics = metricsFor(registry, node);

        node.onRecord(record(C1, 0, null));
        node.onRecord(record(C1, 1, null));
        assertTrue(node.onRecord(record(C1, 0, null)).isEmpty(), "a covered offset is a replay");
        assertTrue(node.onRecord(record(C1, 1, null)).isEmpty(), "a covered offset is a replay");
        assertEquals(2, node.replaysSkipped(), "the node must count both replays");

        metrics.sample(1_000);
        assertEquals(2.0, value(registry, "replays-skipped-total", null),
                "the sample must drain the replay count");
        metrics.sample(2_000);
        assertEquals(2.0, value(registry, "replays-skipped-total", null),
                "a sample without new replays must not double-count");

        node.onRecord(record(C1, 1, null));
        metrics.sample(3_000);
        assertEquals(3.0, value(registry, "replays-skipped-total", null),
                "a later replay must add its delta");
    }

    /** Failed truncation sweeps count. */
    @Test
    void sweepSkipsCount() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.INFO);
        StageMetrics metrics = metricsFor(registry, node());

        metrics.recordSweepSkipped();
        metrics.recordSweepSkipped();
        assertEquals(2.0, value(registry, "truncation-sweeps-skipped-total", null),
                "each failed sweep must count once");
    }

    /** The width gauges count the stamp-side clock's two claim kinds separately. */
    @Test
    void stampWidthGaugesCountBothClaimKinds() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.INFO);
        CausalNode node = node();
        StageMetrics metrics = metricsFor(registry, node);

        VectorClock claims = new VectorClock();
        claims.advanceSeq(C3, UPSTREAM, 3);
        assertEquals(1, node.onRecord(record(C1, 0, claims)).size(),
                "an unconsumed channel's sequence claim must not gate delivery");
        metrics.sample(1_000);
        assertEquals(1.0, value(registry, "stamp-offset-entries", null),
                "the delivered frontier entry must count as one offset entry");
        assertEquals(1.0, value(registry, "stamp-sequence-entries", null),
                "the carried unresolved sequence claim must count as one sequence entry");
    }

    /** Close removes every sensor and metric of the group, per-topic ones included. */
    @Test
    void closeRemovesEverySensorAndItsMetrics() {
        var registry = new RegistryStreamsMetrics(Sensor.RecordingLevel.DEBUG);
        StageMetrics metrics = metricsFor(registry, node());
        assertTrue(exists(registry, "records-held", null), "the gauges register before close");

        metrics.close();
        long remaining = registry.metrics().keySet().stream()
                .filter(m -> m.group().equals(StageMetrics.GROUP)).count();
        assertEquals(0, remaining, "close must remove every metric of the parsley group");
    }
}
