package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Executable checks for the application contract (docs/guide/expectations.md): every clause
 * that is mechanically checkable runs in the application's own test suite, before deploy,
 * instead of being remembered. The docs describe; this class verifies. Runtime fail-closed
 * checks are unaffected — probes move discovery earlier, they replace nothing.
 *
 * <p>{@link #probe(Parsley, Samples, Path)} drives the application's own
 * {@link Parsley#testTopology()} under {@code TopologyTestDriver} (broker-less; requires
 * {@code kafka-streams-test-utils} on the test classpath, an optional dependency of this
 * library) with caller-supplied sample records, and checks what the driver can see: key
 * codecs canonical on the samples, every observed output stamped with parseable causal
 * headers and decodable by its topic's declared codecs, handlers total on their samples, and
 * emissions confined to declared sinks. {@link #probeCluster(Parsley, Admin)} checks what
 * only a live cluster can answer: declared topics exist, a stage's sources agree on
 * partition count (the mechanical half of co-partitioning), and tick topics carry exactly
 * the partition count of the stage's widest source.
 *
 * <p>A {@link Report} separates failures — observed contract violations, each naming the
 * violated clause — from notes, which record coverage gaps (an unsampled source, a sink no
 * sample reached) and the names the deployment must keep stable. A probe run with notes and
 * no failures passed, with stated blind spots.
 */
public final class ContractProbes {

    private static final String CLAUSE_STAMPS =
            "docs/guide/expectations.md#what-parsley-expects-of-you (Stamps)";
    private static final String CLAUSE_TOPICS =
            "docs/guide/expectations.md#what-parsley-expects-of-you (Topics)";
    private static final String CLAUSE_KEYS = "docs/guide/codecs.md#key-codecs-are-identity";
    private static final String CLAUSE_COPARTITION =
            "docs/guide/expectations.md#what-parsley-expects-of-you (Keys and partitioning)";
    private static final String CLAUSE_LOGIC =
            "docs/guide/expectations.md#what-parsley-expects-of-you (Logic)";
    private static final String CLAUSE_NAMES =
            "docs/guide/expectations.md#what-parsley-expects-of-you (Names)";
    private static final String CLAUSE_CODECS = "docs/guide/codecs.md";
    private static final String CLAUSE_TICKS = "docs/guide/ticks.md";

    private ContractProbes() {}

    /** One probe result: which probe, what it observed, and the contract clause it checks. */
    public record Finding(String probe, String detail, String clause) {

        @Override
        public String toString() {
            return probe + ": " + detail + " — see " + clause;
        }
    }

    /**
     * Sample records for {@link #probe}: at least one per source topic, so every handler path
     * is exercised. Keys and values are typed against the topic at the call site and encoded
     * with the topic's own codecs when piped.
     */
    public static final class Samples {

        private record Entry(Topic<Object, Object> topic, Object key, Object value) {}

        private final List<Entry> entries = new ArrayList<>();

        private Samples() {}

        public static Samples of() {
            return new Samples();
        }

        /** Adds one sample record for {@code topic}. Null keys and values are allowed. */
        @SuppressWarnings("unchecked")
        public <K, V> Samples on(Topic<K, V> topic, K key, V value) {
            entries.add(new Entry((Topic<Object, Object>) topic, key, value));
            return this;
        }
    }

    /** The outcome of a probe run: failures to act on, notes to read. */
    public static final class Report {

        private final List<Finding> failures;
        private final List<Finding> notes;

        private Report(List<Finding> failures, List<Finding> notes) {
            this.failures = Collections.unmodifiableList(failures);
            this.notes = Collections.unmodifiableList(notes);
        }

        /** Observed contract violations; empty when the probes passed. */
        public List<Finding> failures() {
            return failures;
        }

        /** Coverage gaps and stability manifests; never failures. */
        public List<Finding> notes() {
            return notes;
        }

        public boolean ok() {
            return failures.isEmpty();
        }

        /** Throws an {@link AssertionError} listing every failure; passes silently when ok. */
        public void assertOk() {
            if (!failures.isEmpty()) {
                throw new AssertionError(this);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("contract probes: ")
                    .append(failures.size()).append(" failure(s), ")
                    .append(notes.size()).append(" note(s)");
            for (Finding f : failures) sb.append("\nFAIL ").append(f);
            for (Finding f : notes) sb.append("\nnote ").append(f);
            return sb.toString();
        }
    }

    /**
     * Runs the test-time probes against the application's own topology. {@code stateDir}
     * backs the driver's state stores; pass a per-test temporary directory (a shared
     * directory contends on one RocksDB lock).
     */
    public static Report probe(Parsley app, Samples samples, Path stateDir) {
        List<Finding> failures = new ArrayList<>();
        List<Finding> notes = new ArrayList<>();

        Map<String, Topic<?, ?>> sources = new TreeMap<>();
        Map<String, Topic<?, ?>> sinks = new TreeMap<>();
        for (Stage stage : app.stageSet()) {
            sources.putAll(stage.declaredSources());
            sinks.putAll(stage.declaredSinks());
        }

        for (Samples.Entry sample : samples.entries) {
            if (!sources.containsKey(sample.topic().name())) {
                failures.add(new Finding("samples", "sample on topic '" + sample.topic()
                        + "', which is not a source of any stage; probe the topics the"
                        + " application consumes", CLAUSE_TOPICS));
            }
            probeCodecs(sample, failures);
        }
        for (String source : sources.keySet()) {
            if (samples.entries.stream().noneMatch(e -> e.topic().name().equals(source))) {
                notes.add(new Finding("coverage", "source topic '" + source + "' has no"
                        + " sample; its handler path was not exercised", CLAUSE_CODECS));
            }
        }

        runTopology(app, samples, stateDir, sources, sinks, failures, notes);

        for (Stage stage : app.stageSet()) {
            if (stage.hasTicks()) {
                notes.add(new Finding("tick-topic", "stage '" + stage.name() + "' needs topic '"
                        + stage.tickTopicName(app.applicationId()) + "' created with the"
                        + " partition count of its widest source before start", CLAUSE_TICKS));
            }
            notes.add(new Finding("names", "stage '" + stage.name() + "' pins store '"
                    + stage.protocolStoreName() + "'"
                    + (stage.stateful() ? " and '" + stage.foldStoreName() + "'" : "")
                    + "; the stage name and application id must stay stable across"
                    + " deployments", CLAUSE_NAMES));
        }
        return new Report(failures, notes);
    }

    /**
     * Runs the cluster probes: everything the contract requires of topics that only a live
     * cluster can answer. Read-only; the application is not started. The {@code Admin} is
     * borrowed, not closed.
     */
    public static Report probeCluster(Parsley app, Admin admin) {
        return probeCluster(app, TopicIds.fromAdmin(admin));
    }

    static Report probeCluster(Parsley app, TopicIds ids) {
        List<Finding> failures = new ArrayList<>();
        for (Stage stage : app.stageSet()) {
            Map<String, Integer> sourcePartitions = new TreeMap<>();
            boolean allSourcesResolved = true;
            for (String topic : new TreeMap<>(stage.declaredSources()).keySet()) {
                Integer partitions = resolveOrReport(ids, topic, "source", stage, failures);
                if (partitions == null) {
                    allSourcesResolved = false;
                } else {
                    sourcePartitions.put(topic, partitions);
                }
            }
            for (String topic : new TreeMap<>(stage.declaredSinks()).keySet()) {
                resolveOrReport(ids, topic, "sink", stage, failures);
            }
            if (allSourcesResolved && sourcePartitions.values().stream().distinct().count() > 1) {
                failures.add(new Finding("co-partition", "the sources of stage '" + stage.name()
                        + "' disagree on partition count: " + sourcePartitions + "; co-partitioned"
                        + " topics need the same count", CLAUSE_COPARTITION));
            }
            if (stage.hasTicks()) {
                String tickTopic = stage.tickTopicName(app.applicationId());
                Integer tickPartitions =
                        resolveOrReport(ids, tickTopic, "tick topic", stage, failures);
                if (tickPartitions != null && allSourcesResolved) {
                    int widest = sourcePartitions.values().stream()
                            .mapToInt(Integer::intValue).max().orElse(0);
                    if (tickPartitions != widest) {
                        failures.add(new Finding("tick-topic", "topic '" + tickTopic
                                + "' has " + tickPartitions + " partition(s) but the widest source"
                                + " of stage '" + stage.name() + "' has " + widest
                                + "; create it with exactly that count", CLAUSE_TICKS));
                    }
                }
            }
        }
        return new Report(failures, new ArrayList<>());
    }

    private static Integer resolveOrReport(TopicIds ids, String topic, String role, Stage stage,
                                           List<Finding> failures) {
        try {
            return ids.resolve(topic).partitions();
        } catch (RuntimeException e) {
            failures.add(new Finding("topic-exists", role + " topic '" + topic + "' of stage '"
                    + stage.name() + "' cannot be resolved: every declared topic exists before"
                    + " the application starts", CLAUSE_TOPICS));
            return null;
        }
    }

    /** Key codecs must be canonical and deterministic; value codecs total on the sample. */
    private static void probeCodecs(Samples.Entry sample, List<Finding> failures) {
        Topic<Object, Object> topic = sample.topic();
        try {
            if (sample.key() != null) {
                byte[] first = topic.keyCodec().encode(sample.key());
                byte[] again = topic.keyCodec().encode(sample.key());
                if (!Arrays.equals(first, again)) {
                    failures.add(new Finding("key-codec", "key codec of topic '" + topic
                            + "' is nondeterministic on the sample key: the encoded bytes choose"
                            + " the partition and key the fold state", CLAUSE_KEYS));
                }
                byte[] reencoded = topic.keyCodec().encode(topic.keyCodec().decode(first));
                if (!Arrays.equals(first, reencoded)) {
                    failures.add(new Finding("key-codec", "key codec of topic '" + topic
                            + "' is not canonical on the sample key: decode then encode changes"
                            + " the bytes", CLAUSE_KEYS));
                }
            }
            if (sample.value() != null) {
                topic.valueCodec().decode(topic.valueCodec().encode(sample.value()));
            }
        } catch (RuntimeException e) {
            failures.add(new Finding("key-codec", "codec of topic '" + topic
                    + "' threw on the sample: " + e, CLAUSE_CODECS));
        }
    }

    /** Pipes the samples through the driver and checks everything observable on the way out. */
    private static void runTopology(Parsley app, Samples samples, Path stateDir,
                                    Map<String, Topic<?, ?>> sources, Map<String, Topic<?, ?>> sinks,
                                    List<Finding> failures, List<Finding> notes) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, app.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "probe:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        try (TopologyTestDriver driver = new TopologyTestDriver(app.testTopology(), props)) {
            Map<String, TestInputTopic<byte[], byte[]>> inputs = new HashMap<>();
            for (Samples.Entry sample : samples.entries) {
                String name = sample.topic().name();
                if (!sources.containsKey(name)) continue;
                TestInputTopic<byte[], byte[]> input = inputs.computeIfAbsent(name,
                        t -> driver.createInputTopic(t,
                                new ByteArraySerializer(), new ByteArraySerializer()));
                pipe(sample, input, failures);
            }

            Duration widestTick = app.stageSet().stream()
                    .filter(Stage::hasTicks).map(Stage::tickInterval)
                    .max(Duration::compareTo).orElse(null);
            if (widestTick != null) {
                driver.advanceWallClockTime(widestTick);
            }

            for (Map.Entry<String, Topic<?, ?>> sink : sinks.entrySet()) {
                readSink(driver, sink.getKey(), sink.getValue(), failures, notes);
            }
            for (Stage stage : app.stageSet()) {
                if (stage.hasTicks()) {
                    readSink(driver, stage.tickTopicName(app.applicationId()), null, failures,
                            new ArrayList<>());
                }
            }
        }
    }

    private static void pipe(Samples.Entry sample, TestInputTopic<byte[], byte[]> input,
                             List<Finding> failures) {
        byte[] key = sample.key() == null
                ? null : sample.topic().keyCodec().encode(sample.key());
        byte[] value = sample.value() == null
                ? null : sample.topic().valueCodec().encode(sample.value());
        try {
            input.pipeInput(key, value);
        } catch (RuntimeException e) {
            String undeclared = messageContaining(e, "undeclared sink");
            if (undeclared != null) {
                failures.add(new Finding("declared-sinks", "the sample on topic '"
                        + sample.topic() + "' led to an " + undeclared, CLAUSE_LOGIC));
            } else {
                failures.add(new Finding("logic", "the sample on topic '" + sample.topic()
                        + "' failed its stage: " + rootCause(e), CLAUSE_LOGIC));
            }
        }
    }

    /**
     * Drains one output topic: every observed record must carry parseable causal headers, and
     * — for user sinks, where {@code topic} is non-null — decode with the sink's declared
     * codecs, since a hop's consumer will decode exactly these bytes.
     */
    private static void readSink(TopologyTestDriver driver, String name, Topic<?, ?> topic,
                                 List<Finding> failures, List<Finding> notes) {
        List<TestRecord<byte[], byte[]>> records = driver.createOutputTopic(name,
                new ByteArrayDeserializer(), new ByteArrayDeserializer()).readRecordsToList();
        if (records.isEmpty()) {
            notes.add(new Finding("coverage", "sink topic '" + name + "' saw no emission"
                    + " during the probe; extend the samples to reach it, or stop declaring"
                    + " it", CLAUSE_LOGIC));
            return;
        }
        for (TestRecord<byte[], byte[]> record : records) {
            try {
                if (CausalHeaders.read(record.headers()) == null
                        || CausalHeaders.readSender(record.headers()) == null
                        || CausalHeaders.readSeq(record.headers()) < 0) {
                    failures.add(new Finding("stamps", "a record on '" + name + "' is missing"
                            + " a causal header: every producer on a causal path stamps its"
                            + " outputs", CLAUSE_STAMPS));
                    break;
                }
            } catch (RuntimeException e) {
                failures.add(new Finding("stamps", "a record on '" + name + "' carries an"
                        + " undecodable causal header: " + e, CLAUSE_STAMPS));
                break;
            }
            if (topic != null && !decodes(topic, record)) {
                failures.add(new Finding("sink-decode", "a record emitted to '" + name + "' does"
                        + " not decode with the topic's own codecs; the consumer side of this"
                        + " hop will fail on it", CLAUSE_CODECS));
                break;
            }
        }
    }

    private static boolean decodes(Topic<?, ?> topic, TestRecord<byte[], byte[]> record) {
        try {
            if (record.key() != null) topic.keyCodec().decode(record.key());
            if (record.value() != null) topic.valueCodec().decode(record.value());
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** The first message in the cause chain containing {@code fragment}, or null. */
    private static String messageContaining(Throwable t, String fragment) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(fragment)) {
                return c.getMessage();
            }
        }
        return null;
    }

    private static String rootCause(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) root = root.getCause();
        return root.toString();
    }
}
