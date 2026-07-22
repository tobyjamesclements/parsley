package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Contract test for the observability surface: the exact metric names, group, tags, and gauge
 * descriptions that {@link ParsleyMetrics#wire} registers, and that each recording method moves
 * the metric users would chart for it. Users build dashboards and alerts on these names (they are
 * documented in {@code docs/configuration.md#metrics}), so a renamed sensor, a dropped tag, or an
 * added/removed metric must fail here rather than break dashboards silently.
 *
 * <p>The registry underneath is a real Kafka {@code Metrics} instance — {@link MockProcessorContext}
 * (Kafka's official test-utils context, an accepted double in this repo) wires a genuine
 * {@code StreamsMetricsImpl} over it, so the names and stats asserted are the ones a live task
 * exposes over JMX. Rate/total descriptions are composed by Kafka Streams from the operation name
 * ("The total number of X operations"), so their wording is Kafka's contract, not ours: this test
 * pins them non-blank but pins exact description text only for the three gauges, whose strings
 * this codebase owns.
 */
class ParsleyMetricsTest {

    private static final String GROUP = "stream-parsley-metrics";
    private static final String APPLICATION_ID = "metrics-contract-test";
    /** {@link MockProcessorContext}'s default task ID. */
    private static final String TASK_ID = "0_0";

    /** The seven sensors registered as rate/total pairs, by base name (docs/configuration.md#metrics). */
    private static final Set<String> RATE_TOTAL_SENSORS = Set.of(
            "records-buffered",
            "records-released",
            "deserialization-errors",
            "clock-resolution-errors",
            "deps-out-of-scope-ignored",
            "replays-skipped",
            "reflected-claims-above-own-outputs");

    /** The three gauges and their exact registered descriptions (docs/configuration.md#metrics). */
    private static final Map<String, String> GAUGES = Map.of(
            "buffer-depth",
            "Current number of records held in the causal buffer",
            "buffer-oldest-buffered-at-ms",
            "Buffer-admission time (epoch millis) of the oldest held record, or 0 if the buffer is empty",
            "records-held-above-highest-received",
            "Held records waiting past the stall threshold on a dependency above its channel's "
                    + "highest received offset — nothing received so far can satisfy the claim");

    /**
     * Wiring registers exactly the documented metric set — seven rate/total pairs plus three
     * gauges, all under the {@code stream-parsley-metrics} group — and every metric carries a
     * non-blank description. An added, removed, or renamed sensor changes the set and fails here.
     */
    @Test
    void wireRegistersExactlyTheDocumentedMetricSet() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics.wire(context);

        Set<String> expected = new HashSet<>(GAUGES.keySet());
        for (String base : RATE_TOTAL_SENSORS) {
            expected.add(base + "-rate");
            expected.add(base + "-total");
        }

        Map<MetricName, ? extends Metric> registered = parsleyMetrics(context);
        Set<String> actual = registered.keySet().stream().map(MetricName::name).collect(Collectors.toSet());
        assertEquals(expected, actual,
                "the registered stream-parsley-metrics set must match docs/configuration.md#metrics "
                        + "exactly; a rename or an added/removed sensor breaks users' dashboards");
        for (MetricName name : registered.keySet()) {
            assertFalse(name.description().isBlank(),
                    "every registered metric needs a description for JMX/tooling: " + name.name());
        }
    }

    /**
     * Every metric in the group — rate/total and gauge alike — carries the same tag scheme:
     * {@code parsley-id} = the bare task ID and {@code thread-id} = the registering thread, the
     * tags dashboards group by. One scheme for the whole group is itself part of the contract: a
     * dashboard grouping by {@code parsley-id} must see a single id format per task.
     */
    @Test
    void everyMetricIsTaggedWithTheTaskIdAndThreadId() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics.wire(context);

        for (MetricName name : parsleyMetrics(context).keySet()) {
            assertEquals(TASK_ID, name.tags().get("parsley-id"),
                    "metric must be tagged with the bare task ID: " + name.name());
            assertEquals(Thread.currentThread().getName(), name.tags().get("thread-id"),
                    "metric must be tagged with the registering thread: " + name.name());
            assertEquals(Set.of("parsley-id", "thread-id"), name.tags().keySet(),
                    "metric must carry exactly the two contract tags: " + name.name());
        }
    }

    /**
     * Each gauge carries exactly its documented description — gauge description strings are owned
     * by this codebase (unlike the Kafka-composed rate/total descriptions), so their wording is
     * part of the contract.
     */
    @Test
    void gaugesKeepTheirDocumentedDescriptions() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics.wire(context);

        Map<String, MetricName> gaugesByName = parsleyMetrics(context).keySet().stream()
                .filter(name -> GAUGES.containsKey(name.name()))
                .collect(Collectors.toMap(MetricName::name, name -> name));
        assertEquals(GAUGES.keySet(), gaugesByName.keySet(),
                "all three documented gauges must be registered");
        for (MetricName gauge : gaugesByName.values()) {
            assertEquals(GAUGES.get(gauge.name()), gauge.description(),
                    "gauge description text is part of the observability contract: " + gauge.name());
        }
    }

    /**
     * Each single-occurrence recording method increments its own {@code -total} and no other —
     * distinct call counts per method so cross-wired sensors (method A recording to sensor B)
     * cannot cancel out and pass.
     */
    @Test
    void countingMethodsIncrementTheirOwnTotals() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics metrics = ParsleyMetrics.wire(context).metrics();

        repeat(1, metrics::recordBuffered);
        repeat(2, metrics::recordDeserializationError);
        repeat(3, metrics::recordClockResolutionError);
        repeat(4, metrics::recordReplaySkipped);
        repeat(5, metrics::recordReflectedClaimAboveOwnOutputs);

        assertEquals(1.0, totalOf(context, "records-buffered"),
                "recordBuffered must count one buffered record per call");
        assertEquals(2.0, totalOf(context, "deserialization-errors"),
                "recordDeserializationError must count one failed deserialisation per call");
        assertEquals(3.0, totalOf(context, "clock-resolution-errors"),
                "recordClockResolutionError must count one undecodable header per call");
        assertEquals(4.0, totalOf(context, "replays-skipped"),
                "recordReplaySkipped must count one skipped redelivery per call");
        assertEquals(5.0, totalOf(context, "reflected-claims-above-own-outputs"),
                "recordReflectedClaimAboveOwnOutputs must count one suspect claim per call");
    }

    /**
     * {@code recordReleased(count)} advances {@code records-released-total} by the number of
     * released records, not by one per drain pass. Regression pin: the sensor's "-total" stat
     * counts observations, so the original single {@code record(count)} call counted drain passes
     * — under-reporting releases and making the total incomparable with {@code records-buffered}.
     */
    @Test
    void recordReleasedCountsReleasedRecordsNotDrainPasses() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics metrics = ParsleyMetrics.wire(context).metrics();

        metrics.recordReleased(3);
        metrics.recordReleased(2);

        assertEquals(5.0, totalOf(context, "records-released"),
                "two drain passes releasing 3 and 2 records must total 5 released records");
    }

    /**
     * {@code recordOutOfScopeIgnored(coordinates)} counts once per ignored coordinate (the
     * documented unit), so a zero-coordinate call must leave the total untouched.
     */
    @Test
    void outOfScopeIgnoredCountsEachIgnoredCoordinate() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics metrics = ParsleyMetrics.wire(context).metrics();

        metrics.recordOutOfScopeIgnored(3);
        metrics.recordOutOfScopeIgnored(0);

        assertEquals(3.0, totalOf(context, "deps-out-of-scope-ignored"),
                "one count per ignored coordinate: 3 + 0 coordinates must total 3");
    }

    /**
     * {@code reportState} drives both buffer gauges to the latest reported values, and an empty
     * oldest-record timestamp reads as the documented 0 sentinel — gauges show current state, not
     * an accumulation.
     */
    @Test
    void stateGaugesTrackTheLatestReport() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics metrics = ParsleyMetrics.wire(context).metrics();

        metrics.reportState(5, OptionalLong.of(123L));
        assertEquals(5.0, gaugeOf(context, "buffer-depth"),
                "buffer-depth must show the reported depth");
        assertEquals(123.0, gaugeOf(context, "buffer-oldest-buffered-at-ms"),
                "buffer-oldest-buffered-at-ms must show the reported admission time");

        metrics.reportState(0, OptionalLong.empty());
        assertEquals(0.0, gaugeOf(context, "buffer-depth"),
                "buffer-depth must follow the buffer back down to empty");
        assertEquals(0.0, gaugeOf(context, "buffer-oldest-buffered-at-ms"),
                "an empty buffer must read as the documented 0 sentinel, not the stale timestamp");
    }

    /**
     * {@code reportHeldAboveHighestReceived} drives its gauge to the latest reported count,
     * including back to zero when a stall clears — the gauge is a current-state signal for
     * alerting, so a stale non-zero reading would be a false alarm.
     */
    @Test
    void heldAboveHighestReceivedGaugeTracksTheLatestReport() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics metrics = ParsleyMetrics.wire(context).metrics();

        metrics.reportHeldAboveHighestReceived(4);
        assertEquals(4.0, gaugeOf(context, "records-held-above-highest-received"),
                "the gauge must show the reported stalled-record count");

        metrics.reportHeldAboveHighestReceived(0);
        assertEquals(0.0, gaugeOf(context, "records-held-above-highest-received"),
                "the gauge must return to 0 when the stall clears");
    }

    /**
     * {@code Wired.close} removes every registered sensor from the registry — a task that closed
     * must not leave metrics behind to report stale values for a task this instance no longer owns.
     */
    @Test
    void closeRemovesEverySensorFromTheRegistry() {
        MockProcessorContext<Void, Void> context = newContext();
        ParsleyMetrics.Wired wired = ParsleyMetrics.wire(context);
        assertFalse(parsleyMetrics(context).isEmpty(), "wiring must have registered metrics to remove");

        wired.close(context.metrics());

        assertEquals(Set.of(), parsleyMetrics(context).keySet(),
                "close must remove every parsley sensor; leftovers would report stale values "
                        + "for a task this instance no longer owns");
    }

    // --- helpers -------------------------------------------------------------------------------

    /** A context whose {@code metrics()} is a real Kafka metrics registry, with a fixed app ID. */
    private static MockProcessorContext<Void, Void> newContext() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        return new MockProcessorContext<>(props);
    }

    /** All metrics currently registered under the {@code stream-parsley-metrics} group. */
    private static Map<MetricName, Metric> parsleyMetrics(MockProcessorContext<?, ?> context) {
        Map<MetricName, Metric> found = new LinkedHashMap<>();
        for (Map.Entry<MetricName, ? extends Metric> e : context.metrics().metrics().entrySet()) {
            if (GROUP.equals(e.getKey().group())) {
                found.put(e.getKey(), e.getValue());
            }
        }
        return found;
    }

    private static double valueOf(MockProcessorContext<?, ?> context, String metricName) {
        for (Map.Entry<MetricName, Metric> e : parsleyMetrics(context).entrySet()) {
            if (e.getKey().name().equals(metricName)) {
                return ((Number) e.getValue().metricValue()).doubleValue();
            }
        }
        throw new AssertionError("expected metric is not registered: " + metricName);
    }

    /** The cumulative {@code -total} value of a rate/total sensor, by base name. */
    private static double totalOf(MockProcessorContext<?, ?> context, String sensorBaseName) {
        return valueOf(context, sensorBaseName + "-total");
    }

    /** The current value of a gauge metric. */
    private static double gaugeOf(MockProcessorContext<?, ?> context, String gaugeName) {
        return valueOf(context, gaugeName);
    }

    private static void repeat(int times, Runnable recording) {
        for (int i = 0; i < times; i++) {
            recording.run();
        }
    }
}
