package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CausalStreams} — the topology-owning high-level causal API — through a real
 * Kafka Streams topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalStreamsTopologyTest {

    // t1/t2 = single- and dual-source test topics; out = the sink.
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID, "t2", T2_ID));

    private final List<String> processed = new ArrayList<>();

    // --- helpers -------------------------------------------------------------------------------

    private static Properties config() {
        return config(null);
    }

    private static Properties config(File stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        if (stateDir != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        }
        return props;
    }

    private static Headers depsHeader(CausalDependencies deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-dependencies", deps.toBytes());
        return headers;
    }

    /** A user processor that records every value it sees and forwards it upper-cased. */
    private ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                processed.add(record.value());
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    // --- tests ---------------------------------------------------------------------------------

    /**
     * A single-source, single-sink causal stage: the built {@link Topology} wires a source for the
     * one registered {@link CausalBuffer}, a causal-decorated processor, and the one registered sink,
     * and an admitted record flows delegate-transformed through to the sink.
     *
     * Asserts the delegate runs and the sink receives the transformed, causally-stamped record.
     */
    @Test
    void singleSourceStageDeliversAdmittedRecordToItsSink() {
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
        }
    }

    /**
     * A two-source causal stage fans both source topics into the same processor node: a record
     * consumed from either source reaches the delegate and is forwarded to the shared sink.
     *
     * Asserts records from both t1 and t2 reach the delegate and are forwarded.
     */
    @Test
    void twoSourceStageFansBothSourcesIntoTheSameProcessor() {
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSource(CausalBuffer.of("t2", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "fromT1", depsHeader(CausalDependencies.empty())));
            t2.pipeInput(new TestRecord<>("k", "fromT2", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("fromT1", "fromT2"), processed,
                    "both sources must feed the same processor node");
            List<TestRecord<String, String>> emitted = out.readRecordsToList();
            assertEquals(2, emitted.size(), "both source records must be forwarded to the shared sink");
        }
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage missing its buffer store.
     *
     * Asserts an {@link IllegalStateException} naming the missing buffer store.
     */
    @Test
    void buildFailsWithoutABufferStore() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without a buffer store");
        assertEquals(true, e.getMessage().contains("buffer store"), "message must name the missing buffer store");
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage with no registered source.
     *
     * Asserts an {@link IllegalStateException} naming the missing source.
     */
    @Test
    void buildFailsWithoutASource() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one source");
        assertEquals(true, e.getMessage().contains("source"), "message must name the missing source");
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage with no registered sink.
     *
     * Asserts an {@link IllegalStateException} naming the missing sink.
     */
    @Test
    void buildFailsWithoutASink() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one sink");
        assertEquals(true, e.getMessage().contains("sink"), "message must name the missing sink");
    }

    /**
     * {@link CausalStreams.Builder#withPartitioner} applies the same {@link StreamPartitioner} to
     * every sink the stage declares — a delegate that branches to two named sinks must see both
     * sink topics invoke the custom partitioner, proving there is no per-sink drift back onto the
     * Kafka default.
     *
     * Asserts the recording partitioner was invoked once for each of the two sink topics, each with
     * the key of the record routed to it.
     */
    @Test
    void withPartitionerAppliesTheSamePartitionerToEverySink() {
        RecordingPartitioner<String, String> recorder = new RecordingPartitioner<>();
        ProcessorSupplier<String, String, String, String> brancher = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String sink = record.value().equals("toA") ? "sink-a" : "sink-b";
                ctx.forward(record, sink);
            }
        };

        Topology topology = CausalStreams.builder(brancher)
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("sink-a", "out-a", Serdes.String(), Serdes.String())
                .addSink("sink-b", "out-b", Serdes.String(), Serdes.String())
                .withPartitioner(recorder)
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());

            t1.pipeInput(new TestRecord<>("k", "toA", depsHeader(CausalDependencies.empty())));
            t1.pipeInput(new TestRecord<>("k", "toB", depsHeader(CausalDependencies.empty())));

            assertTrue(recorder.invocations.contains("out-a:k"),
                    "the custom partitioner must be invoked for sink-a's topic");
            assertTrue(recorder.invocations.contains("out-b:k"),
                    "the custom partitioner must be invoked for sink-b's topic, not just sink-a — proving "
                            + "no per-sink drift back onto the Kafka default");
        }
    }

    /**
     * Under {@code parsley.topology.validation=strict}, a sink topic with a partition count that
     * mismatches the stage's source fails startup fast — {@link CausalStreams} folds sink partition
     * counts into the same co-partitioning check the decorator runs on its inputs.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * mismatch and the strict mode.
     */
    @Test
    void mismatchedSinkPartitionCountFailsStartupUnderStrictValidation() throws IOException {
        ParsleyTopicAdmin mismatched = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of("t1", 2, "out", 3));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(mismatched)
                .build();

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "strict validation must fail startup on a source/sink partition-count mismatch");
        assertEquals(IllegalStateException.class, thrown.getCause().getClass(),
                "the wrapped cause must be the strict-validation failure");
        assertTrue(thrown.getCause().getMessage().contains("mismatched partition counts"),
                "the message must name the mismatch: " + thrown.getCause().getMessage());
    }

    /**
     * Under the default {@code parsley.topology.validation=warn}, a sink topic with a mismatched
     * partition count is logged but does not fail startup.
     *
     * Asserts the topology constructs and processes normally despite the mismatch.
     */
    @Test
    void mismatchedSinkPartitionCountWarnsButStartsUnderDefaultValidation() {
        ParsleyTopicAdmin mismatched = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of("t1", 2, "out", 3));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(mismatched)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("live"), processed,
                    "warn-mode validation must not fail startup: the task starts and processes normally");
        }
    }

    /**
     * A sink topic whose partition count cannot be resolved (e.g. not yet created) is skipped for
     * the co-partitioning check rather than failing the task — unlike a registered input buffer, a
     * sink is not required to exist before the stage starts, even under strict validation.
     *
     * Asserts the topology constructs and processes normally under strict validation despite the
     * sink's partition count being unresolvable.
     */
    @Test
    void unresolvableSinkPartitionCountIsSkippedEvenUnderStrictValidation() {
        ParsleyTopicAdmin admin = FlakySinkAdmin.withUnresolvable(Map.of("t1", T1_ID), Set.of("out"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(admin)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("live"), processed,
                    "an unresolvable sink partition count must be skipped, not fail even strict validation");
        }
    }

    /**
     * Under {@code parsley.topology.validation=strict}, a sink topic whose {@code cleanup.policy}
     * includes {@code compact} fails startup fast — a protocol watermark is a null-value record
     * wire-indistinguishable from a compaction tombstone and could otherwise be compacted away
     * before a slow consumer reads it.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * sink and its cleanup.policy.
     */
    @Test
    void compactedSinkCleanupPolicyFailsStartupUnderStrictValidation() throws IOException {
        ParsleyTopicAdmin compacted = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of(), Map.of("out", "compact"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(compacted)
                .build();

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "strict validation must fail startup when a sink's cleanup.policy includes compact");
        assertEquals(IllegalStateException.class, thrown.getCause().getClass(),
                "the wrapped cause must be the strict-validation failure");
        assertTrue(thrown.getCause().getMessage().contains("cleanup.policy=compact"),
                "the message must name the sink's cleanup.policy: " + thrown.getCause().getMessage());
    }

    /**
     * A sink topic's {@code cleanup.policy=compact,delete} is equally unsafe as plain {@code compact}
     * — compaction still runs and can remove a watermark before it is read — so it must also fail
     * strict validation.
     *
     * Asserts driver construction throws for a {@code compact,delete} sink under strict validation.
     */
    @Test
    void compactAndDeleteSinkCleanupPolicyFailsStartupUnderStrictValidation() throws IOException {
        ParsleyTopicAdmin compacted = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of(), Map.of("out", "compact,delete"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(compacted)
                .build();

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "strict validation must fail startup for compact,delete too — compaction still runs");
        assertEquals(IllegalStateException.class, thrown.getCause().getClass(),
                "the wrapped cause must be the strict-validation failure");
    }

    /**
     * Under the default {@code parsley.topology.validation=warn}, a compacted sink is logged but does
     * not fail startup.
     *
     * Asserts the topology constructs and processes normally despite the compacted sink.
     */
    @Test
    void compactedSinkCleanupPolicyWarnsButStartsUnderDefaultValidation() {
        ParsleyTopicAdmin compacted = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of(), Map.of("out", "compact"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(compacted)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("live"), processed,
                    "warn-mode validation must not fail startup: the task starts and processes normally");
        }
    }

    /**
     * A {@code delete}-policy sink (the default, and the safe choice) passes strict validation — a
     * guard against the cleanup-policy check false-positiving on a correctly configured sink.
     *
     * Asserts the topology constructs and processes normally under strict validation.
     */
    @Test
    void deleteSinkCleanupPolicyPassesStrictValidation() {
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("live"), processed,
                    "strict validation must pass for a delete-policy sink: the task processes normally");
        }
    }

    /**
     * A sink whose partition count cannot be resolved must not mask a genuine partition-count
     * mismatch on a DIFFERENT sink in the same stage: each sink is checked independently, so one
     * not-yet-created sink cannot hide another sink's real misconfiguration, even under strict
     * validation.
     *
     * Asserts driver construction still throws for {@code out-a}'s mismatch despite {@code out-b}
     * being unresolvable.
     */
    @Test
    void unresolvableSinkDoesNotMaskAPartitionMismatchOnAnotherSink() throws IOException {
        ParsleyTopicAdmin admin = new FlakySinkAdmin(
                Map.of("t1", T1_ID), Map.of("t1", 2, "out-a", 3), Map.of(), Set.of("out-b"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("sink-a", "out-a", Serdes.String(), Serdes.String())
                .addSink("sink-b", "out-b", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(admin)
                .build();

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "strict validation must still catch out-a's mismatch despite out-b being unresolvable");
        assertTrue(thrown.getCause().getMessage().contains("mismatched partition counts"),
                "the message must name the mismatch: " + thrown.getCause().getMessage());
    }

    /**
     * A sink whose cleanup.policy cannot be resolved must not mask a genuine {@code compact} policy
     * on a DIFFERENT sink in the same stage — the same independent-per-sink guarantee as partition
     * counts, for the cleanup-policy check.
     *
     * Asserts driver construction still throws for {@code out-a}'s compact policy despite
     * {@code out-b} being unresolvable.
     */
    @Test
    void unresolvableSinkDoesNotMaskACompactPolicyOnAnotherSink() throws IOException {
        ParsleyTopicAdmin admin = new FlakySinkAdmin(
                Map.of("t1", T1_ID), Map.of(), Map.of("out-a", "compact"), Set.of("out-b"));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("sink-a", "out-a", Serdes.String(), Serdes.String())
                .addSink("sink-b", "out-b", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "strict")
                .topicAdmin(admin)
                .build();

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "strict validation must still catch out-a's compact policy despite out-b being unresolvable");
        assertTrue(thrown.getCause().getMessage().contains("cleanup.policy=compact"),
                "the message must name out-a's policy: " + thrown.getCause().getMessage());
    }

    /**
     * Under {@code parsley.topology.validation=off}, sink validation must not even attempt the admin
     * round-trips for partition counts or cleanup policies — not merely discard their results.
     *
     * Asserts a sink admin that fails every call is never actually invoked when validation is off.
     */
    @Test
    void validationOffNeverCallsTheSinkAdminAtAll() {
        CountingSinkAdmin admin = new CountingSinkAdmin(Map.of("t1", T1_ID));
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .withConfig(ParsleyConfig.TOPOLOGY_VALIDATION, "off")
                .topicAdmin(admin)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(0, admin.sinkPartitionCountCalls, "off must skip the sink partition-count admin call entirely");
            assertEquals(0, admin.sinkCleanupPolicyCalls, "off must skip the sink cleanup-policy admin call entirely");
        }
    }

    /** A fresh, unique state directory — required when a test expects driver construction itself to
     * fail, since a failed construction cannot be closed to release its RocksDB locks. */
    private static File tempStateDir() throws IOException {
        return Files.createTempDirectory("causal-streams-test-").toFile();
    }

    /** Records every {@code (topic, key)} pair it is asked to partition, for asserting uniform wiring. */
    private static final class RecordingPartitioner<K, V> implements StreamPartitioner<K, V> {

        private final Set<String> invocations = new java.util.HashSet<>();

        @Override
        public Optional<Set<Integer>> partitions(String topic, K key, V value, int numPartitions) {
            invocations.add(topic + ":" + key);
            return Optional.empty();
        }
    }

    /**
     * A {@link ParsleyTopicAdmin} test double that resolves the given topic UUIDs and reports the
     * given per-topic partition counts (default 1) and cleanup policies (default {@code "delete"}),
     * but throws when {@code partitionCounts}/{@code cleanupPolicies} is asked about any topic in
     * {@code unresolvableTopics} — simulating a sink that does not exist yet. Unlike
     * {@link TestTopicAdmin}, this throws <strong>per call</strong> if ANY requested topic is
     * unresolvable (matching the real {@code Admin}'s all-or-nothing batch behaviour), which is what
     * exercises {@link ParsleyProcessor}'s per-topic resolution of sink checks.
     */
    private static final class FlakySinkAdmin implements ParsleyTopicAdmin {

        private final Map<String, Uuid> topicIds;
        private final Map<String, Integer> partitionCounts;
        private final Map<String, String> cleanupPolicies;
        private final Set<String> unresolvableTopics;

        FlakySinkAdmin(Map<String, Uuid> topicIds, Map<String, Integer> partitionCounts,
                Map<String, String> cleanupPolicies, Set<String> unresolvableTopics) {
            this.topicIds = topicIds;
            this.partitionCounts = partitionCounts;
            this.cleanupPolicies = cleanupPolicies;
            this.unresolvableTopics = unresolvableTopics;
        }

        static FlakySinkAdmin withUnresolvable(Map<String, Uuid> topicIds, Set<String> unresolvableTopics) {
            return new FlakySinkAdmin(topicIds, Map.of(), Map.of(), unresolvableTopics);
        }

        @Override
        public Map<String, Uuid> topicIds(List<String> topics) {
            Map<String, Uuid> resolved = new HashMap<>();
            topics.forEach(t -> resolved.put(t, topicIds.get(t)));
            return resolved;
        }

        @Override
        public Map<String, Integer> partitionCounts(List<String> topics) throws Exception {
            failIfAnyUnresolvable(topics);
            Map<String, Integer> counts = new HashMap<>();
            topics.forEach(t -> counts.put(t, partitionCounts.getOrDefault(t, 1)));
            return counts;
        }

        @Override
        public Map<String, String> cleanupPolicies(List<String> topics) throws Exception {
            failIfAnyUnresolvable(topics);
            Map<String, String> policies = new HashMap<>();
            topics.forEach(t -> policies.put(t, cleanupPolicies.getOrDefault(t, "delete")));
            return policies;
        }

        private void failIfAnyUnresolvable(List<String> topics) throws Exception {
            for (String topic : topics) {
                if (unresolvableTopics.contains(topic)) {
                    throw new Exception("topic '" + topic + "' does not exist");
                }
            }
        }

        @Override
        public void createTopic(String name, int partitions) {
            // no broker in tests
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    /**
     * A {@link ParsleyTopicAdmin} test double that resolves the given (input) topic UUIDs and counts
     * how many times it is asked for a SINK's partition count or cleanup.policy — i.e. any
     * {@code partitionCounts}/{@code cleanupPolicies} call whose topics are not all known input
     * topics. Used to prove {@code parsley.topology.validation=off} skips the sink admin round-trips
     * entirely, not merely discards their results.
     */
    private static final class CountingSinkAdmin implements ParsleyTopicAdmin {

        private final Map<String, Uuid> topicIds;
        int sinkPartitionCountCalls = 0;
        int sinkCleanupPolicyCalls = 0;

        CountingSinkAdmin(Map<String, Uuid> topicIds) {
            this.topicIds = topicIds;
        }

        @Override
        public Map<String, Uuid> topicIds(List<String> topics) {
            Map<String, Uuid> resolved = new HashMap<>();
            topics.forEach(t -> resolved.put(t, topicIds.get(t)));
            return resolved;
        }

        @Override
        public Map<String, Integer> partitionCounts(List<String> topics) {
            if (!topicIds.keySet().containsAll(topics)) {
                sinkPartitionCountCalls++;
            }
            Map<String, Integer> counts = new HashMap<>();
            topics.forEach(t -> counts.put(t, 1));
            return counts;
        }

        @Override
        public Map<String, String> cleanupPolicies(List<String> topics) {
            // cleanupPolicies is only ever called for sink topics — never for registered inputs.
            sinkCleanupPolicyCalls++;
            Map<String, String> policies = new HashMap<>();
            topics.forEach(t -> policies.put(t, "delete"));
            return policies;
        }

        @Override
        public void createTopic(String name, int partitions) {
            // no broker in tests
        }

        @Override
        public void close() {
            // nothing to close
        }
    }
}
