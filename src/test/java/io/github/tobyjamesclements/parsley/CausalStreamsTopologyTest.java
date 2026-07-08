package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import java.time.Duration;
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
 * Exercises {@link CausalStreamsBuilder}/{@link CausalTopology} — the topology-owning causal API —
 * through a real Kafka Streams topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalStreamsTopologyTest {

    // t1/t2 = single- and dual-source test topics; out = the sink.
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID, "t2", T2_ID));
    private static final ParsleyTopics TOPICS = ParsleyTopics.of(Map.of("t1", T1_ID, "t2", T2_ID));

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
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        if (stateDir != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        }
        return props;
    }

    /** Assembles {@code builder} against {@code admin} and default (warn) topology validation. */
    private static Topology assemble(CausalStreamsBuilder builder, ParsleyTopicAdmin admin) {
        return assemble(builder, admin, config(), ParsleyQuiesce.create());
    }

    private static Topology assemble(CausalStreamsBuilder builder, ParsleyTopicAdmin admin, Properties props) {
        return assemble(builder, admin, props, ParsleyQuiesce.create());
    }

    private static Topology assemble(
            CausalStreamsBuilder builder, ParsleyTopicAdmin admin, Properties props, ParsleyQuiesce quiesce) {
        return builder.topicAdmin(admin).build().assemble(props, quiesce, null);
    }

    private static Properties strictValidation() {
        Properties props = config();
        props.put(ParsleyConfig.TOPOLOGY_VALIDATION, "strict");
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
     * A single-source, single-sink causal stage: the assembled {@link Topology} wires a source for the
     * one registered topic, a causal-decorated processor, and the one registered sink, and an admitted
     * record flows delegate-transformed through to the sink.
     *
     * Asserts the delegate runs and the sink receives the transformed, causally-stamped record.
     */
    @Test
    void singleSourceStageDeliversAdmittedRecordToItsSink() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(List.of("t1", "t2"), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

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
     * {@link CausalStreamsBuilder#build()} rejects a stage missing its source topic(s).
     *
     * Asserts an {@link IllegalStateException} naming the missing source.
     */
    @Test
    void buildFailsWithoutASource() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.<String, String>stream(List.<String>of(), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one source");
        assertEquals(true, e.getMessage().contains("source"), "message must name the missing source");
    }

    /**
     * {@link CausalStreamsBuilder#build()} rejects a stage with no registered sink.
     *
     * Asserts an {@link IllegalStateException} naming the missing sink.
     */
    @Test
    void buildFailsWithoutASink() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String()).process(upperCaser());

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one sink");
        assertEquals(true, e.getMessage().contains("sink"), "message must name the missing sink");
    }

    /**
     * Assembling a stage whose {@code userSupplier} is already a package-private
     * {@code ParsleyProcessorSupplier} rejects it — the double-wrap guard lives in
     * {@code ParsleyProcessors#builder}, which {@link CausalTopology#assemble} composes internally, so
     * this proves the protection is inherited rather than needing a second, separate check in the
     * public builder.
     *
     * Asserts an {@link IllegalArgumentException} naming the double-decoration.
     */
    @Test
    void assembleRejectsAnAlreadyDecoratedSupplier() {
        ParsleyProcessorSupplier<String, String, String, String> alreadyDecorated =
                ParsleyProcessors.builder(upperCaser())
                        .addBufferStore("parsley")
                        .addBuffer(ParsleyBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .build();

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(alreadyDecorated)
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        CausalTopology topology = builder.topicAdmin(ADMIN).build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> topology.assemble(config(), ParsleyQuiesce.create(), null),
                "assemble() must reject an already-decorated supplier");
        assertTrue(e.getMessage().contains("ParsleyProcessorSupplier"),
                "the message must name the double-decoration: " + e.getMessage());
    }

    /**
     * {@link CausalProcessedStream#withPartitioner} applies the same {@link StreamPartitioner} to
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

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(brancher)
                .to("sink-a", "out-a", Serdes.String(), Serdes.String())
                .to("sink-b", "out-b", Serdes.String(), Serdes.String())
                .withPartitioner(recorder);
        Topology topology = assemble(builder, ADMIN);

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
     * Regression test for BACKLOG.md's marker-relay finding: {@link CausalTopology} installs a {@link
     * ParsleyMarkerPartitioner} wrapping a stage's own {@code withPartitioner(...)} partitioner, so a
     * Parsley protocol marker's routing is decided by {@link ParsleyMarkerPartition} — the forwarding
     * task's own owned partition — and never reaches the stage's own partitioner at all.
     *
     * <p>A single "drop" input is never forwarded by the delegate, so Parsley emits exactly one stand-in
     * watermark to reach "out". Proving the recording partitioner is genuinely wired into this sink but
     * never invoked for the watermark is the whole point of this test: {@code TopologyTestDriver}'s
     * single-partition topic can't otherwise distinguish "routed by the override" from "routed by the
     * delegate" by outcome alone, since both land on the same (only) partition either way.
     *
     * Asserts the watermark still reaches the output topic, and the recording partitioner was never
     * invoked for it.
     */
    @Test
    void markerForwardsNeverReachTheStagesOwnPartitioner() {
        RecordingPartitioner<String, String> recorder = new RecordingPartitioner<>();
        ProcessorSupplier<String, String, String, String> neverEmits = () -> new Processor<>() {
            @Override
            public void init(ProcessorContext<String, String> context) {
            }

            @Override
            public void process(Record<String, String> record) {
                // forwards nothing -- Parsley emits a stand-in watermark for this input
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(neverEmits)
                .to("out-sink", "out", Serdes.String(), Serdes.String())
                .withPartitioner(recorder);
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "drop", depsHeader(CausalDependencies.empty())));

            assertTrue(isWatermark(out.readRecord()), "the stand-in watermark must still reach the sink");
            assertTrue(recorder.invocations.isEmpty(),
                    "the marker forward must never reach the stage's own partitioner — "
                            + "ParsleyMarkerPartition must have intercepted it first");
        }
    }

    /**
     * A delivered record the delegate forwards to only ONE named sink still causes Parsley's
     * stand-in watermark (emitted because the delegate's forward count for this input was zero — see
     * {@link ParsleyProcessor#deliver}) to reach EVERY sink connected to the processor node, not just
     * the one the delegate targeted. This is Kafka Streams' own broadcast behaviour for an unqualified
     * {@code context.forward(record)} (used for watermark emission, unlike the delegate's own
     * possibly-named business forward) — exercised here through a real multi-sink
     * {@link CausalStreamsBuilder} topology, not just a raw {@code context.forward} call.
     *
     * Asserts: the record that IS forwarded (business output) reaches only its named sink, and the
     * record that is NOT forwarded triggers a watermark reaching BOTH sinks.
     */
    @Test
    void watermarkForANonEmittingRecordReachesEverySinkNotJustTheNamedOne() {
        ProcessorSupplier<String, String, String, String> brancher = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if (record.value().equals("emit")) {
                    ctx.forward(record, "sink-a");
                }
                // "drop": forward nothing — Parsley must emit a stand-in watermark for this input.
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(brancher)
                .to("sink-a", "out-a", Serdes.String(), Serdes.String())
                .to("sink-b", "out-b", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> outA =
                    driver.createOutputTopic("out-a", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> outB =
                    driver.createOutputTopic("out-b", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "emit", depsHeader(CausalDependencies.empty())));
            t1.pipeInput(new TestRecord<>("k", "drop", depsHeader(CausalDependencies.empty())));

            TestRecord<String, String> businessRecord = outA.readRecord();
            assertEquals("emit", businessRecord.value(), "the named-sink forward must reach sink-a");

            TestRecord<String, String> watermarkOnA = outA.readRecord();
            assertTrue(isWatermark(watermarkOnA), "the non-emitting record's watermark must reach sink-a");
            TestRecord<String, String> watermarkOnB = outB.readRecord();
            assertTrue(isWatermark(watermarkOnB),
                    "the non-emitting record's watermark must ALSO reach sink-b, not just the named sink-a — "
                            + "Parsley's watermark forward is unqualified and Kafka Streams broadcasts it to "
                            + "every sink connected to the processor node");
        }
    }

    /** Whether {@code record} carries Parsley's protocol-watermark header. */
    private static boolean isWatermark(TestRecord<String, String> record) {
        return record.headers().lastHeader(ParsleyHeader.WATERMARK) != null;
    }

    /**
     * A stage assembled with a {@link ParsleyQuiesce} keeps that quiesce's readiness signal in sync
     * with its actual buffer depth: a held record (unsatisfied dependency) must keep
     * {@code isSafeToClose} false even after quiesce is requested, and delivering that record via the
     * ordinary satisfying-message path (never a synthetic watermark) must flip it back to true.
     *
     * Asserts {@code isSafeToClose} is false while a record is held, then true once it drains.
     */
    @Test
    void quiesceTracksTheBufferThroughTheOrdinaryDeliveryPath() {
        ParsleyQuiesce quiesce = ParsleyQuiesce.create();
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(List.of("t1", "t2"), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, config(), quiesce);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());

            // Depends on t2@0, a channel this stage consumes but has not yet seen any traffic on —
            // held until a real t2 record arrives.
            t1.pipeInput(new TestRecord<>("k", "held",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 0).build())));

            quiesce.requestQuiesce();
            assertEquals(false, quiesce.isSafeToClose(), "a held record must keep the stage unsafe to close");

            // Satisfies the held record's dependency (t2@0) — draining the buffer to empty through
            // the ordinary delivery path, never a synthetic watermark.
            t2.pipeInput(new TestRecord<>("k", "trigger", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("trigger", "held"), processed,
                    "the held record must drain once its dependency is satisfied");
            assertTrue(quiesce.isSafeToClose(),
                    "the buffer is now empty and quiesce was requested: safe to close");
        }
    }

    /**
     * A task's buffer can empty through the ordinary delivery path <em>before</em>
     * {@code requestQuiesce()} is ever called — {@code updateQuiesceState()} only re-evaluates
     * {@code isQuiesceRequested() && empty} on a buffer-depth-changing event, so without a periodic
     * re-check, a task that was already drained at that point would be recorded {@code drained=false}
     * (quiesce wasn't requested yet) and never get another chance to report otherwise once it is, hanging
     * {@code CausalStreams#close()}'s wait forever despite the buffer genuinely being empty. The periodic
     * metrics-refresh punctuator (every 5s) closes this gap by re-pushing the drained state on every tick,
     * not just on buffer changes.
     *
     * Asserts {@code isSafeToClose()} is false immediately after requesting quiesce (no re-check has run
     * yet) and becomes true once the punctuator ticks, with no further record ever delivered.
     */
    @Test
    void quiesceRecoversViaThePeriodicTickWhenTheBufferDrainedBeforeQuiesceWasRequested() {
        ParsleyQuiesce quiesce = ParsleyQuiesce.create();
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, config(), quiesce);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());

            // Delivered immediately (no dependencies) — the buffer is already empty long before quiesce
            // is ever requested, with nothing further ever delivered to trigger a fresh depth-changing
            // event afterward.
            t1.pipeInput(new TestRecord<>("k", "already-drained", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("already-drained"), processed);

            quiesce.requestQuiesce();
            assertEquals(false, quiesce.isSafeToClose(),
                    "no depth-changing event has re-evaluated isQuiesceRequested() since the request yet");

            driver.advanceWallClockTime(Duration.ofSeconds(6)); // fires the 5s metrics-refresh punctuator

            assertTrue(quiesce.isSafeToClose(),
                    "the periodic tick must re-push the drained state so close() does not hang forever");
        }
    }

    /**
     * Under {@code parsley.topology.validation=strict}, a sink topic with a partition count that
     * mismatches the stage's source fails startup fast — {@link CausalTopology#assemble} folds sink
     * partition counts into the same co-partitioning check the decorator runs on its inputs.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * mismatch and the strict mode.
     */
    @Test
    void mismatchedSinkPartitionCountFailsStartupUnderStrictValidation() throws IOException {
        ParsleyTopicAdmin mismatched = TestTopicAdmin.of(
                Map.of("t1", T1_ID), Map.of("t1", 2, "out", 3));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, mismatched, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, mismatched);

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compacted, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compacted, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compacted);

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("sink-a", "out-a", Serdes.String(), Serdes.String())
                .to("sink-b", "out-b", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, strictValidation());

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
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("sink-a", "out-a", Serdes.String(), Serdes.String())
                .to("sink-b", "out-b", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, strictValidation());

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
        Properties props = config();
        props.put(ParsleyConfig.TOPOLOGY_VALIDATION, "off");
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, props);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("t1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalDependencies.empty())));
            assertEquals(0, admin.sinkPartitionCountCalls, "off must skip the sink partition-count admin call entirely");
            assertEquals(0, admin.sinkCleanupPolicyCalls, "off must skip the sink cleanup-policy admin call entirely");
        }
    }

    /**
     * Regression test for BACKLOG.md's write-ordering-overclaim finding: {@link CausalTopology#assemble}
     * requires {@code processing.guarantee=exactly_once_v2} unconditionally — never gated by {@code
     * parsley.topology.validation}, unlike the partition-count/cleanup-policy checks above — since the
     * crash-safety reasoning throughout {@code ParsleyEngine}/{@code ParsleyFrontier} (a torn write always
     * lands on the benign side) only holds without exception under exactly-once's transactional
     * multi-store commit.
     *
     * Asserts {@code assemble()} throws {@link IllegalStateException} immediately — before any {@code
     * Topology} node is even added — when {@code processing.guarantee} is left at its at-least-once
     * default, naming the required setting in the message.
     */
    @Test
    void assembleFailsFastWithoutExactlyOnceProcessingGuarantee() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());

        Properties atLeastOnce = config();
        atLeastOnce.remove(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> assemble(builder, ADMIN, atLeastOnce),
                "assemble() must fail fast without exactly_once_v2");
        assertTrue(thrown.getMessage().contains("exactly_once_v2"),
                "the message must name the required setting: " + thrown.getMessage());
    }

    /**
     * Each stage's dead-letter sink is its own topology node, parented only on that stage's processor —
     * never one sink node shared across stages. Sharing one node would union every stage's Kafka Streams
     * node group (sub-topology/task assignment is computed per node group), improperly coupling stages
     * that are otherwise fully independent sub-topologies chained only through real topics.
     *
     * <p>Confirms the fix for a design risk found during planning: {@code Topology.addSink} does accept
     * multiple parent names, so one shared dead-letter sink across every stage would compile and run, but
     * would silently merge node groups. Per-stage sinks avoid that risk entirely.
     *
     * Asserts a two-stage topology — each stage with a different (unrelated) source partition count and
     * its own dead-letter sink — still describes as two separate sub-topologies.
     */
    @Test
    void eachStageKeepsItsOwnSubtopologyDespiteBothHavingADeadLetterSink() {
        ParsleyTopicAdmin admin = TestTopicAdmin.of(
                Map.of("t1", T1_ID, "t2", T2_ID), Map.of("t1", 4, "t2", 12));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink-1", "out1", Serdes.String(), Serdes.String());
        builder.stream("t2", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink-2", "out2", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin);

        assertEquals(2, topology.describe().subtopologies().size(),
                "two independent stages, each with its own dead-letter sink, must remain two separate "
                        + "sub-topologies — a single dead-letter sink shared across both would union them");
    }

    /**
     * Finding B (discovered during planning): before targeted forwarding was added, Parsley's business
     * and watermark forwards both used Kafka Streams' zero-arg broadcast, which sends to every child of
     * the processor node. The moment the dead-letter sink became a second child of every stage (this
     * phase's whole point), a plain business or watermark forward would have also broadcast to it and
     * thrown {@code ClassCastException} on its {@code Serdes.ByteArray()} serializer — not a corner
     * case, the very next record. This is the regression test that would have caught it.
     *
     * Asserts a normal business record reaches only its real sink, a stand-in watermark (for a
     * non-emitting record) also reaches only the real sink, and the dead-letter topic receives neither.
     */
    @Test
    void deadLetterSinkNeverReceivesTheOrdinaryBusinessOrWatermarkBroadcast() {
        ProcessorSupplier<String, String, String, String> maybeEmit = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if (record.value().equals("emit")) {
                    ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
                }
                // "drop": forward nothing — Parsley emits a stand-in watermark for this input.
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(maybeEmit)
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<byte[], byte[]> deadLetter = driver.createOutputTopic(
                    "causal-streams-test-deadletter", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            t1.pipeInput(new TestRecord<>("k", "emit", depsHeader(CausalDependencies.empty())));
            t1.pipeInput(new TestRecord<>("k", "drop", depsHeader(CausalDependencies.empty())));

            assertEquals("EMIT", out.readRecord().value(), "the business record must reach its real sink");
            assertTrue(isWatermark(out.readRecord()), "the non-emitting record's stand-in watermark must reach the real sink");
            assertTrue(deadLetter.isEmpty(),
                    "neither the business forward nor the watermark broadcast may ever reach the dead-letter sink");
        }
    }

    /**
     * Regression test for BACKLOG.md's ingest-time dead-letter finding: an {@code UNRESOLVABLE_CLOCK}
     * record — its causal-dependencies header undecodable — is dead-lettered entirely inside
     * {@link ParsleyProcessor#onUnresolvableClock}, before {@code engine.onRecord} is ever called for
     * it. Unlike the buffered {@code POISON}/{@code ORPHAN_CASCADE} paths, nothing used to durably
     * record that its coordinate could never advance past that offset, so a dependent buffered on that
     * exact offset would be held forever, never proven impossible.
     *
     * <p>Single-source stage on "t1". t1@0's causal-dependencies header is corrupted (an unsupported
     * wire-version byte), so decoding throws {@link ParsleyClockResolutionException} at ingest. t1@1
     * then depends on t1@0's own coordinate — an intra-topic dependency, the finding's minimal repro:
     * t1's frontier can never legitimately reach offset 0 (it was never delivered), so before the fix
     * t1@1 would buffer forever with no trigger to ever re-check it.
     *
     * Asserts both land on the dead-letter topic (t1@0 as {@code UNRESOLVABLE_CLOCK}, t1@1 as {@code
     * ORPHAN_CASCADE}), the delegate is never invoked for either (only a heartbeat watermark — never
     * business data — may still reach "out", since each record's own receipt-time channel-clock update
     * runs regardless of its eventual dead-letter disposition), and nothing is left buffered.
     */
    @Test
    void unresolvableClockAtIngestOrphansItsCoordinateAndDeadLettersAnIntraTopicDependent() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<byte[], byte[]> deadLetter = driver.createOutputTopic(
                    "causal-streams-test-deadletter", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            Headers corrupted = ParsleyHeader.mutableHeaders();
            corrupted.add(ParsleyHeader.CAUSAL_DEPENDENCIES, new byte[] {(byte) 0xFF});
            t1.pipeInput(new TestRecord<>("k0", "zero", corrupted));

            CausalDependencies needsT1At0 = CausalDependencies.builder(TOPICS).require("t1", 0, 0).build();
            t1.pipeInput(new TestRecord<>("k1", "one", depsHeader(needsT1At0)));

            assertTrue(processed.isEmpty(), "the delegate must never run for either dead-lettered record");
            while (!out.isEmpty()) {
                assertTrue(isWatermark(out.readRecord()),
                        "only a heartbeat watermark may reach the business sink — never business data");
            }

            TestRecord<byte[], byte[]> first = deadLetter.readRecord();
            assertEquals("UNRESOLVABLE_CLOCK", headerValue(first, ParsleyHeader.DEADLETTER_REASON),
                    "t1@0's undecodable dependencies header must dead-letter it as UNRESOLVABLE_CLOCK");
            TestRecord<byte[], byte[]> second = deadLetter.readRecord();
            assertEquals("ORPHAN_CASCADE", headerValue(second, ParsleyHeader.DEADLETTER_REASON),
                    "t1@1 must be dead-lettered as a cascade victim, not left buffered forever");
            assertTrue(deadLetter.isEmpty(),
                    "exactly two dead letters — t1@1 must not still be sitting in the buffer");
        }
    }

    /** The value of header {@code key} on {@code record}, or {@code null} if absent. */
    private static @org.jspecify.annotations.Nullable String headerValue(
            TestRecord<byte[], byte[]> record, String key) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
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
