package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.StreamsMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registers the stage adapter's metrics on the host's Kafka Streams registry.
 *
 * <p>Sensors go in group {@value #GROUP}, so they reach JMX, configured reporters, and
 * {@link CausalStreams#metrics()} alongside the built-ins. The committed consumer position
 * advances past held records, because holding is protocol state rather than consumer lag, so
 * hold depth and hold age are observable only through these metrics.
 *
 * <p>Gauges are sampled by the host's wall-clock punctuator from the node's observation
 * surface. Event counters are recorded at their event sites.
 *
 * <p>Per-topic breakdowns record at the {@code DEBUG} recording level, everything else at
 * {@code INFO}, mirroring the log-level split.
 *
 * <p>Time enters only through {@link #sample}, so the class holds no clock and samples
 * deterministically under a mocked driver.
 */
final class StageMetrics {

    /** The metrics group every sensor registers in. */
    static final String GROUP = "vc-metrics";

    /**
     * The head a channel's hold age is measured against.
     *
     * @param offset the head record's offset
     * @param sinceMs when it became the head, on the host's wall clock
     */
    private record HeadMark(long offset, long sinceMs) {}

    private final StreamsMetrics registry;
    private final CausalNode node;
    private final List<Sensor> sensors = new ArrayList<>();
    private final Sensor recordsHeld;
    private final Sensor recordsHeldAge;
    private final Sensor recordsDelivered;
    private final Sensor replaysSkipped;
    private final Sensor sweepsSkipped;
    private final Sensor ticksEmitted;
    private final Sensor stampOffsetEntries;
    private final Sensor stampSequenceEntries;
    private final Map<Channel, Sensor> heldByChannel = new HashMap<>();
    private final Map<Channel, Sensor> heldAgeByChannel = new HashMap<>();
    private final Map<Channel, HeadMark> headMarks = new HashMap<>();
    /** The replay count already drained into the sensor. {@link #sample} records the delta. */
    private long replaysRecorded;
    /** The wall clock of the most recent {@link #sample}, which {@link #heldMs} ages against. */
    private long lastSampleMs;

    /**
     * Registers every sensor for one stage task.
     *
     * @param registry the host's Kafka Streams metrics registry
     * @param stage the stage name, used as a tag and in each sensor's name
     * @param taskId the task, as Kafka Streams names it
     * @param topicByChannel the source topic of each consumed channel, for per-topic breakdowns
     * @param node the node the gauges sample from
     */
    StageMetrics(StreamsMetrics registry, String stage, String taskId,
                 Map<Channel, String> topicByChannel, CausalNode node) {
        this.registry = registry;
        this.node = node;
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("thread-id", Thread.currentThread().getName());
        tags.put("task-id", taskId);
        tags.put("stage", stage);
        String prefix = "vc-" + stage + "-" + taskId + "-";

        recordsHeld = gauge(prefix, "records-held",
                "The number of records currently held across the task's hold queues.",
                tags, Sensor.RecordingLevel.INFO);
        recordsHeldAge = gauge(prefix, "records-held-age-max-ms",
                "The wall-clock milliseconds the longest-gating head record has been held,"
                        + " measured from when it became its queue's head.",
                tags, Sensor.RecordingLevel.INFO);
        stampOffsetEntries = gauge(prefix, "stamp-offset-entries",
                "The number of offset entries in the stamp-side clock.",
                tags, Sensor.RecordingLevel.INFO);
        stampSequenceEntries = gauge(prefix, "stamp-sequence-entries",
                "The number of sequence entries in the stamp-side clock, claims not yet"
                        + " resolved to offsets.",
                tags, Sensor.RecordingLevel.INFO);

        recordsDelivered = registry.addSensor(prefix + "records-delivered", Sensor.RecordingLevel.INFO);
        recordsDelivered.add(new MetricName("records-delivered-rate", GROUP,
                "The per-second rate of records released through the causal gate.", tags), new Rate());
        recordsDelivered.add(new MetricName("records-delivered-total", GROUP,
                "The total number of records released through the causal gate.", tags),
                new CumulativeSum());
        sensors.add(recordsDelivered);

        replaysSkipped = total(prefix, "replays-skipped-total",
                "The total number of records fed at or below the frontier and discarded."
                        + " Nonzero outside a rescope-growth refetch means the host is refeeding"
                        + " covered offsets.", tags);
        sweepsSkipped = total(prefix, "truncation-sweeps-skipped-total",
                "The total number of truncation sweeps that failed and skipped their cycle.", tags);
        ticksEmitted = total(prefix, "ticks-emitted-total",
                "The total number of tick records this task has emitted to its tick channel.", tags);

        topicByChannel.forEach((c, topic) -> {
            Map<String, String> topicTags = new LinkedHashMap<>(tags);
            topicTags.put("topic", topic);
            String topicPrefix = prefix + topic + "-";
            heldByChannel.put(c, gauge(topicPrefix, "records-held",
                    "The number of records currently held on this source topic's channel.",
                    topicTags, Sensor.RecordingLevel.DEBUG));
            heldAgeByChannel.put(c, gauge(topicPrefix, "records-held-age-max-ms",
                    "The wall-clock milliseconds this channel's head record has been held.",
                    topicTags, Sensor.RecordingLevel.DEBUG));
        });
    }

    private Sensor gauge(String sensorPrefix, String name, String description,
                         Map<String, String> tags, Sensor.RecordingLevel level) {
        Sensor s = registry.addSensor(sensorPrefix + name, level);
        s.add(new MetricName(name, GROUP, description, tags), new Value());
        sensors.add(s);
        return s;
    }

    private Sensor total(String sensorPrefix, String name, String description,
                         Map<String, String> tags) {
        Sensor s = registry.addSensor(sensorPrefix + name, Sensor.RecordingLevel.INFO);
        s.add(new MetricName(name, GROUP, description, tags), new CumulativeSum());
        sensors.add(s);
        return s;
    }

    /**
     * Records one batch of records released through the gate.
     *
     * @param count how many records the batch released
     */
    void recordDelivered(int count) {
        recordsDelivered.record(count);
    }

    /** Records one failed truncation sweep. */
    void recordSweepSkipped() {
        sweepsSkipped.record(1);
    }

    /** Records one emitted tick. */
    void recordTickEmitted() {
        ticksEmitted.record(1);
    }

    /**
     * Samples every gauge from the node's observation surface and drains the replay counter.
     *
     * <p>A head first observed held is aged from that observation, so restored holds age from
     * the first sample after init.
     *
     * @param nowMs the host's wall clock
     */
    void sample(long nowMs) {
        lastSampleMs = nowMs;
        Map<Channel, Integer> held = node.heldCounts();
        Map<Channel, Long> heads = node.headOffsets();
        headMarks.keySet().retainAll(heads.keySet());
        int total = 0;
        long maxAge = 0;
        for (Map.Entry<Channel, Sensor> e : heldByChannel.entrySet()) {
            Channel c = e.getKey();
            int count = held.getOrDefault(c, 0);
            total += count;
            long age = 0;
            Long head = heads.get(c);
            if (head != null) {
                HeadMark mark = headMarks.get(c);
                if (mark == null || mark.offset() != head) {
                    mark = new HeadMark(head, nowMs);
                    headMarks.put(c, mark);
                }
                age = nowMs - mark.sinceMs();
            }
            maxAge = Math.max(maxAge, age);
            e.getValue().record(count);
            heldAgeByChannel.get(c).record(age);
        }
        recordsHeld.record(total);
        recordsHeldAge.record(maxAge);

        CausalNode.StampWidth width = node.stampWidth();
        stampOffsetEntries.record(width.offsetEntries());
        stampSequenceEntries.record(width.sequenceEntries());

        long replays = node.replaysSkipped();
        if (replays > replaysRecorded) {
            replaysSkipped.record(replays - replaysRecorded);
            replaysRecorded = replays;
        }
    }

    /**
     * Returns how long this channel's current head has been held, as of the last
     * {@link #sample}.
     *
     * <p>The same measurement the {@code records-held-age-max-ms} gauge reports, reused so the
     * gauge, the diagnosis surface, and the hold warning cannot disagree.
     *
     * @param c the channel to age
     * @return the age in milliseconds, or {@code 0} when the channel has no head marked
     */
    long heldMs(Channel c) {
        HeadMark mark = headMarks.get(c);
        return mark == null ? 0 : lastSampleMs - mark.sinceMs();
    }

    /** Removes every registered sensor. Called when the task's processor closes. */
    void close() {
        sensors.forEach(registry::removeSensor);
    }
}
