package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
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
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.cause;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.message;
import org.jspecify.annotations.Nullable;

/**
 * Exercises {@link CausalStreamsBuilder}/{@link CausalTopology} — the topology-owning causal API —
 * through a real Kafka Streams topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalStreamsTopologyTest {

    // c1/c2 = single- and dual-source test topics; c3 = the sink.
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid C3_ID = Uuid.randomUuid();
    private static final Uuid C4_ID = Uuid.randomUuid();
    private static final Uuid C5_ID = Uuid.randomUuid();

    // Sink resolution is strict at init (a declared sink must exist), so the shared admin resolves
    // the sink topics the tests declare, not just the sources.
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of(
            "c1", C1_ID, "c2", C2_ID, "c3", C3_ID, "c4", C4_ID, "c5", C5_ID));
    private static final ParsleyTopics TOPICS = ParsleyTopics.of(Map.of("c1", C1_ID, "c2", C2_ID));

    private final List<String> processed = new ArrayList<>();

    // --- helpers -------------------------------------------------------------------------------

    private static Properties config() {
        return config(null);
    }

    private static Properties config(@Nullable File stateDir) {
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

    /** Assembles {@code builder} against {@code admin}; the startup topology checks are always on. */
    private static Topology assemble(CausalStreamsBuilder builder, ParsleyTopicAdmin admin) {
        return assemble(builder, admin, config(), new ParsleyQuiesce());
    }

    private static Topology assemble(CausalStreamsBuilder builder, ParsleyTopicAdmin admin, Properties props) {
        return assemble(builder, admin, props, new ParsleyQuiesce());
    }

    private static Topology assemble(
            CausalStreamsBuilder builder, ParsleyTopicAdmin admin, Properties props, ParsleyQuiesce quiesce) {
        return builder.topicAdmin(admin).build().assemble(props, quiesce);
    }

    private static Headers depsHeader(CausalClock deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-clock", deps.toBytes());
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
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c3", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalClock.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
        }
    }

    /**
     * A two-source causal stage fans both source topics into the same processor node: a record
     * consumed from either source reaches the delegate and is forwarded to the shared sink.
     *
     * Asserts records from both c1 and c2 reach the delegate and are forwarded.
     */
    @Test
    void twoSourceStageFansBothSourcesIntoTheSameProcessor() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(List.of("c1", "c2"), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c3", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "fromC1", depsHeader(CausalClock.empty())));
            c2.pipeInput(new TestRecord<>("k", "fromC2", depsHeader(CausalClock.empty())));

            assertEquals(List.of("fromC1", "fromC2"), processed,
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
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one source");
        assertEquals(true, message(e).contains("source"), "message must name the missing source");
    }

    /**
     * {@link CausalStreamsBuilder#build()} rejects a stage with no registered sink.
     *
     * Asserts an {@link IllegalStateException} naming the missing sink.
     */
    @Test
    void buildFailsWithoutASink() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String()).process(upperCaser());

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one sink");
        assertEquals(true, message(e).contains("sink"), "message must name the missing sink");
    }

    /**
     * {@link CausalStreamsBuilder#build()} rejects a builder with no declared stage at all — the built
     * topology would silently run nothing.
     *
     * Asserts an {@link IllegalStateException} naming the missing stage declaration.
     */
    @Test
    void buildFailsOnAnEmptyBuilder() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail when no stage was declared");
        assertEquals(true, message(e).contains("no causal stage"),
                "message must say no stage was declared, got: " + message(e));
    }

    /**
     * A built {@link CausalTopology} is immutable: declaring a further sink, or swapping the
     * partitioner, through a {@link CausalProcessedStream} handle retained from before
     * {@code build()} is rejected instead of silently mutating the built topology.
     *
     * Asserts both late mutations throw {@link IllegalStateException}.
     */
    @Test
    void aBuiltTopologyRejectsLateSinkAndPartitionerMutations() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        CausalProcessedStream<String, String> handle = builder
                .<String, String>stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        builder.build();

        assertThrows(IllegalStateException.class,
                () -> handle.to("c6-sink", "c6", Serdes.String(), Serdes.String()),
                "a sink declared after build() must be rejected — the built topology is immutable");
        assertThrows(IllegalStateException.class,
                () -> handle.withPartitioner((topic, key, value, partitions) -> java.util.Optional.empty()),
                "a partitioner set after build() must be rejected — the built topology is immutable");
    }

    /**
     * A causal topology is exactly one stage. The public fluent chain cannot express a second stage at all
     * (there is no {@code process}/{@code stream} on {@link CausalProcessedStream} and no public
     * {@code build()} on the builder); this covers the one runtime-reachable path — a second
     * {@code process(...)} on the same builder — which is rejected so extra stages go into separate
     * applications.
     *
     * Asserts an {@link IllegalStateException} explaining the one-stage-per-topology rule.
     */
    @Test
    void aBuilderRejectsASecondStage() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> builder.stream("c2", Serdes.String(), Serdes.String()).process(upperCaser()),
                "a second process(...) on the same builder must be rejected — one stage per topology");
        assertEquals(true, message(e).contains("exactly one stage"),
                "message must explain the one-stage-per-topology rule");
    }

    /**
     * {@link CausalStream#merge} rejects two streams declaring the same topic: merging would silently
     * pick one stream's serdes over the other's (last-write-wins), a bug every time.
     *
     * Asserts an {@link IllegalArgumentException} naming the duplicated topic.
     */
    @Test
    void mergeRejectsADuplicateTopicAcrossStreams() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        CausalStream<String, String> first = builder.stream(List.of("c1", "c2"), Serdes.String(), Serdes.String());
        CausalStream<String, String> second = builder.stream("c2", Serdes.String(), Serdes.String());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> first.merge(second),
                "merging two streams that both declare c2 must be rejected");
        assertEquals(true, message(e).contains("c2"), "message must name the duplicated topic");
    }

    /**
     * Assembling a stage whose {@code userSupplier} is already a package-private
     * {@code ParsleyProcessorSupplier} rejects it — the double-wrap guard lives in
     * {@code ParsleyProcessorSupplier#builder}, which {@link CausalTopology#assemble} composes internally, so
     * this proves the protection is inherited rather than needing a second, separate check in the
     * public builder.
     *
     * Asserts an {@link IllegalArgumentException} naming the double-decoration.
     */
    @Test
    void assembleRejectsAnAlreadyDecoratedSupplier() {
        ParsleyProcessorSupplier<String, String, String, String> alreadyDecorated =
                ParsleyProcessorSupplier.builder(upperCaser())
                        .addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .build();

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(alreadyDecorated)
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        CausalTopology topology = builder.topicAdmin(ADMIN).build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> topology.assemble(config(), new ParsleyQuiesce()),
                "assemble() must reject an already-decorated supplier");
        assertTrue(message(e).contains("ParsleyProcessorSupplier"),
                "the message must name the double-decoration: " + message(e));
    }

    /**
     * A record-skipping {@code deserialization.exception.handler} ({@code LogAndContinue}) is rejected at
     * assemble, unconditionally — a dropped record leaves a consumer-visible hole the skip-bridge would
     * fold as a marker, delivering a dependent before its cause. Exercises the {@code Class}-valued config
     * form.
     */
    @Test
    void assembleRejectsASkippingDeserializationExceptionHandler() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        CausalTopology topology = builder.topicAdmin(ADMIN).build();
        Properties props = config();
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> topology.assemble(props, new ParsleyQuiesce()),
                "assemble() must reject a record-skipping deserialization handler");
        assertTrue(message(e).contains("skips records"),
                "the message must explain the skip hazard: " + message(e));
    }

    /**
     * A continue-mode {@code processing.exception.handler} is rejected at assemble, unconditionally — it
     * would swallow Parsley's own fail-closed throws, converting a safe stall into a silent violation.
     * Exercises the {@code String}-valued (class-name) config form.
     */
    @Test
    void assembleRejectsASkippingProcessingExceptionHandler() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        CausalTopology topology = builder.topicAdmin(ADMIN).build();
        Properties props = config();
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueProcessingExceptionHandler.class.getName());

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> topology.assemble(props, new ParsleyQuiesce()),
                "assemble() must reject a record-skipping processing handler");
        assertTrue(message(e).contains("skips records"),
                "the message must explain the skip hazard: " + message(e));
    }

    /**
     * A record-skipping handler supplied through a {@link Properties} <em>defaults</em> layer is still
     * rejected. Kafka Streams honours that layer (it reads props via {@code propertyNames()}), so a check
     * that only did a plain {@code Hashtable} {@code get()} would miss it and let the app run with a
     * skipping handler — the exact silent-violation path the guard exists to close.
     */
    @Test
    void assembleRejectsASkippingHandlerSuppliedThroughAPropertiesDefaultsLayer() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        CausalTopology topology = builder.topicAdmin(ADMIN).build();

        Properties defaults = new Properties();
        defaults.setProperty(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());
        Properties props = new Properties(defaults);   // skipping handler only in the defaults layer
        config().forEach(props::put);                   // base config (incl. EOS) in the top layer

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> topology.assemble(props, new ParsleyQuiesce()),
                "a skipping handler in a Properties defaults layer must still be rejected");
        assertTrue(message(e).contains("record-skipping handler"),
                "the message must explain the skip hazard: " + message(e));
    }

    /**
     * The assembled topology exposes the frontier state store (which carries {@code highestReceived}) on
     * its processor node, so {@code ParsleyOffsetSeeder}'s surviving-state guard can see its changelog and
     * refuse to seed over stale causal state. If a future Kafka change stopped reporting the store in
     * {@code describe()}, {@code CausalStreams.changelogTopicsOf} would silently omit it and the guard
     * would false-negative — the exact silent-violation direction — so this pins the property.
     */
    @Test
    void theAssembledTopologyExposesTheFrontierStoreForTheSurvivingStateGuard() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        Set<String> stores = new HashSet<>();
        topology.describe().subtopologies().forEach(subtopology -> subtopology.nodes().forEach(node -> {
            if (node instanceof TopologyDescription.Processor processor) {
                stores.addAll(processor.stores());
            }
        }));
        assertTrue(stores.stream().anyMatch(store -> store.endsWith("-frontier")),
                "the processor must expose its frontier store (holding highestReceived) in describe(), or "
                        + "the surviving-state guard could not detect it and would seed over stale causal "
                        + "state: " + stores);
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
                String sink = record.value().equals("toA") ? "c4-sink" : "c5-sink";
                ctx.forward(record, sink);
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(brancher)
                .to("c4-sink", "c4", Serdes.String(), Serdes.String())
                .to("c5-sink", "c5", Serdes.String(), Serdes.String())
                .withPartitioner(recorder);
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            c1.pipeInput(new TestRecord<>("k", "toA", depsHeader(CausalClock.empty())));
            c1.pipeInput(new TestRecord<>("k", "toB", depsHeader(CausalClock.empty())));

            assertTrue(recorder.invocations.contains("c4:k"),
                    "the custom partitioner must be invoked for sink-a's topic");
            assertTrue(recorder.invocations.contains("c5:k"),
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
     * null message to reach "c3". Proving the recording partitioner is genuinely wired into this sink but
     * never invoked for the null message is the whole point of this test: {@code TopologyTestDriver}'s
     * single-partition topic can't otherwise distinguish "routed by the override" from "routed by the
     * delegate" by outcome alone, since both land on the same (only) partition either way.
     *
     * Asserts the null message still reaches the output topic, and the recording partitioner was never
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
                // forwards nothing -- Parsley emits a stand-in null message for this input
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(neverEmits)
                .to("c3-sink", "c3", Serdes.String(), Serdes.String())
                .withPartitioner(recorder);
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c3", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "drop", depsHeader(CausalClock.empty())));

            assertTrue(isNullMessage(out.readRecord()), "the stand-in null message must still reach the sink");
            assertTrue(recorder.invocations.isEmpty(),
                    "the marker forward must never reach the stage's own partitioner — "
                            + "ParsleyMarkerPartition must have intercepted it first");
        }
    }

    /**
     * A delivered record the delegate forwards to only ONE named sink still causes Parsley's
     * stand-in null message (emitted because the delegate's forward count for this input was zero — see
     * {@link ParsleyProcessor#deliver}) to reach EVERY sink connected to the processor node, not just
     * the one the delegate targeted. This is Kafka Streams' own broadcast behaviour for an unqualified
     * {@code context.forward(record)} (used for null message emission, unlike the delegate's own
     * possibly-named business forward) — exercised here through a real multi-sink
     * {@link CausalStreamsBuilder} topology, not just a raw {@code context.forward} call.
     *
     * Asserts: the record that IS forwarded (business output) reaches only its named sink, and the
     * record that is NOT forwarded triggers a null message reaching BOTH sinks.
     */
    @Test
    void nullMessageForANonEmittingRecordReachesEverySinkNotJustTheNamedOne() {
        ProcessorSupplier<String, String, String, String> brancher = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if (record.value().equals("emit")) {
                    ctx.forward(record, "c4-sink");
                }
                // "drop": forward nothing — Parsley must emit a stand-in null message for this input.
            }
        };

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(brancher)
                .to("c4-sink", "c4", Serdes.String(), Serdes.String())
                .to("c5-sink", "c5", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> c4Out =
                    driver.createOutputTopic("c4", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> c5Out =
                    driver.createOutputTopic("c5", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "emit", depsHeader(CausalClock.empty())));
            c1.pipeInput(new TestRecord<>("k", "drop", depsHeader(CausalClock.empty())));

            TestRecord<String, String> businessRecord = c4Out.readRecord();
            assertEquals("emit", businessRecord.value(), "the named-sink forward must reach sink-a");

            TestRecord<String, String> nullMessageOnA = c4Out.readRecord();
            assertTrue(isNullMessage(nullMessageOnA), "the non-emitting record's null message must reach sink-a");
            TestRecord<String, String> nullMessageOnB = c5Out.readRecord();
            assertTrue(isNullMessage(nullMessageOnB),
                    "the non-emitting record's null message must ALSO reach sink-b, not just the named sink-a — "
                            + "Parsley's null message forward is unqualified and Kafka Streams broadcasts it to "
                            + "every sink connected to the processor node");
        }
    }

    /** Whether {@code record} carries Parsley's protocol-null message header. */
    private static boolean isNullMessage(TestRecord<String, String> record) {
        return record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) != null;
    }

    /**
     * A stage assembled with a {@link ParsleyQuiesce} keeps that quiesce's readiness signal in sync
     * with its actual buffer depth: a held record (unsatisfied dependency) must keep
     * {@code isSafeToClose} false even after quiesce is requested, and delivering that record via the
     * ordinary satisfying-message path (never a synthetic null message) must flip it back to true.
     *
     * <p>The held record's dependency ({@code c2@0}) is genuinely unmet, not merely undeclared: every
     * record is checked against this node's actual current state, never against its own declared
     * claim, so a record cannot prove its own prerequisite by simply asserting it.
     *
     * Asserts {@code isSafeToClose} is false while a record is held, then true once it drains.
     */
    @Test
    void quiesceTracksTheBufferThroughTheOrdinaryDeliveryPath() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(List.of("c1", "c2"), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, config(), quiesce);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());

            // Depends on c2@0, a channel this stage consumes but has not yet seen any traffic on —
            // held until a real c2 record arrives.
            c1.pipeInput(new TestRecord<>("k", "held",
                    depsHeader(CausalClock.builder(TOPICS).require("c2", 0, 0).build())));

            quiesce.requestQuiesce();
            assertEquals(false, quiesce.isSafeToClose(), "a held record must keep the stage unsafe to close");

            // Satisfies the held record's dependency (c2@0) — draining the buffer to empty through
            // the ordinary delivery path, never a synthetic null message.
            c2.pipeInput(new TestRecord<>("k", "trigger", depsHeader(CausalClock.empty())));

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
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, config(), quiesce);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            // Delivered immediately (no dependencies) — the buffer is already empty long before quiesce
            // is ever requested, with nothing further ever delivered to trigger a fresh depth-changing
            // event afterward.
            c1.pipeInput(new TestRecord<>("k", "already-drained", depsHeader(CausalClock.empty())));
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
     * A sink topic narrower than the stage's source fails startup fast, unconditionally: protocol
     * markers route to the forwarding task's own partition, so a task owning source partition 1
     * could never produce its marker to a 1-partition sink — the mismatch would crash-loop at the
     * produce, and the startup check surfaces that impossibility once, at init.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * narrow sink.
     */
    @Test
    void sinkNarrowerThanItsSourceFailsStartup() throws IOException {
        ParsleyTopicAdmin mismatched = TestTopicAdmin.of(
                Map.of("c1", C1_ID, "c3", C3_ID), Map.of("c1", 2, "c3", 1));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, mismatched, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "startup must fail when a sink is narrower than its source");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the sink-width check's failure");
        assertTrue(message(cause(thrown)).contains("declared sink topic 'c3'"),
                "the message must name the narrow sink: " + message(cause(thrown)));
    }

    /**
     * A sink topic <em>wider</em> than the stage's source passes the startup checks with no
     * opt-down: the funnel shape (narrow sources fanning into a wider, re-keyed sink) is
     * protocol-safe — every task's own partition exists on the sink, so marker produces succeed —
     * and the sink-width check deliberately permits it.
     *
     * Asserts the topology constructs and processes normally with a 2-partition source and a
     * 3-partition sink.
     */
    @Test
    void sinkWiderThanItsSourcePassesTheStartupChecks() {
        ParsleyTopicAdmin funnel = TestTopicAdmin.of(
                Map.of("c1", C1_ID, "c3", C3_ID), Map.of("c1", 2, "c3", 3));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, funnel, config());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("c1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalClock.empty())));
            assertEquals(List.of("live"), processed,
                    "a sink wider than its source must pass startup: the task starts and processes normally");
        }
    }

    /**
     * A sink topic whose partition count cannot be described (a transient admin failure) is skipped
     * for the sink-width check rather than failing the task: the sink-side checks are per-topic
     * best-effort. Sink identity itself is not — the sink's UUID and end offsets still resolve
     * here, and an unresolvable sink fails init unconditionally (see the strict-sink-resolution
     * tests below).
     *
     * Asserts the topology constructs and processes normally despite the sink's partition count
     * being undescribable.
     */
    @Test
    void undescribableSinkPartitionCountIsSkipped() {
        ParsleyTopicAdmin admin = FlakySinkAdmin.withUndescribable(
                Map.of("c1", C1_ID, "c3", C3_ID), Set.of("c3"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, config());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("c1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalClock.empty())));
            assertEquals(List.of("live"), processed,
                    "an undescribable sink partition count must be skipped, not fail the task");
        }
    }

    /**
     * A sink topic whose {@code cleanup.policy} includes {@code compact} fails startup fast,
     * unconditionally — a protocol null message is a null-value record wire-indistinguishable from
     * a compaction tombstone and could otherwise be compacted away before a slow consumer reads it.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * sink and its cleanup.policy.
     */
    @Test
    void compactedSinkCleanupPolicyFailsStartup() throws IOException {
        ParsleyTopicAdmin compacted = TestTopicAdmin.of(
                Map.of("c1", C1_ID, "c3", C3_ID), Map.of(), Map.of("c3", "compact"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compacted, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "startup must fail when a sink's cleanup.policy includes compact");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the compacted-sink check's failure");
        assertTrue(message(cause(thrown)).contains("cleanup.policy=compact"),
                "the message must name the sink's cleanup.policy: " + message(cause(thrown)));
    }

    /**
     * A sink topic's {@code cleanup.policy=compact,delete} is equally unsafe as plain {@code compact}
     * — compaction still runs and can remove a null message before it is read — so it must also fail
     * startup.
     *
     * Asserts driver construction throws for a {@code compact,delete} sink.
     */
    @Test
    void compactAndDeleteSinkCleanupPolicyFailsStartup() throws IOException {
        ParsleyTopicAdmin compacted = TestTopicAdmin.of(
                Map.of("c1", C1_ID, "c3", C3_ID), Map.of(), Map.of("c3", "compact,delete"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compacted, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "startup must fail for compact,delete too — compaction still runs");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the compacted-sink check's failure");
    }

    /**
     * A {@code delete}-policy sink (the default, and the safe choice) passes the startup checks — a
     * guard against the cleanup-policy check false-positiving on a correctly configured sink.
     *
     * Asserts the topology constructs and processes normally.
     */
    @Test
    void deleteSinkCleanupPolicyPassesTheStartupChecks() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN, config());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("c1", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "live", depsHeader(CausalClock.empty())));
            assertEquals(List.of("live"), processed,
                    "the startup checks must pass for a delete-policy sink: the task processes normally");
        }
    }

    /**
     * A causal <em>source</em> topic whose {@code cleanup.policy} includes {@code compact} fails
     * startup. The skip-bridge treats a consumer-visible hole as a transaction marker, but log
     * compaction can punch the same hole by removing a real committed record, so a compacted source
     * cannot be consumed causally at all.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} naming the
     * source and its cleanup.policy.
     */
    @Test
    void compactedSourceCleanupPolicyFailsStartup() throws IOException {
        ParsleyTopicAdmin compactedSource = TestTopicAdmin.of(
                Map.of("c1", C1_ID, "c3", C3_ID), Map.of(), Map.of("c1", "compact"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, compactedSource, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "a compacted source must fail startup — the guard is unconditional");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the unconditional source-compaction guard");
        assertTrue(message(cause(thrown)).contains("causal source topic 'c1'"),
                "the message must name the compacted source: " + message(cause(thrown)));
    }

    /**
     * A sink whose partition count cannot be described must not mask a genuinely narrow DIFFERENT
     * sink in the same stage: each sink is checked independently, so one sink's transient describe
     * failure cannot hide another sink's real misconfiguration.
     *
     * Asserts driver construction still throws for {@code c4}'s narrowness despite {@code c5}
     * being undescribable.
     */
    @Test
    void undescribableSinkDoesNotMaskANarrowerSink() throws IOException {
        ParsleyTopicAdmin admin = new FlakySinkAdmin(
                Map.of("c1", C1_ID, "c4", C4_ID, "c5", C5_ID),
                Map.of("c1", 2, "c4", 1), Map.of(), Set.of("c5"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c4-sink", "c4", Serdes.String(), Serdes.String())
                .to("c5-sink", "c5", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "the sink-width check must still catch c4's narrowness despite c5 being undescribable");
        assertTrue(message(cause(thrown)).contains("declared sink topic 'c4'"),
                "the message must name the narrow sink: " + message(cause(thrown)));
    }

    /**
     * A sink whose cleanup.policy cannot be described must not mask a genuine {@code compact} policy
     * on a DIFFERENT sink in the same stage — the same independent-per-sink guarantee as partition
     * counts, for the cleanup-policy check.
     *
     * Asserts driver construction still throws for {@code c4}'s compact policy despite
     * {@code c5} being undescribable.
     */
    @Test
    void undescribableSinkDoesNotMaskACompactPolicyOnAnotherSink() throws IOException {
        ParsleyTopicAdmin admin = new FlakySinkAdmin(
                Map.of("c1", C1_ID, "c4", C4_ID, "c5", C5_ID),
                Map.of(), Map.of("c4", "compact"), Set.of("c5"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c4-sink", "c4", Serdes.String(), Serdes.String())
                .to("c5-sink", "c5", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin, config());

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "the compacted-sink check must still catch c4's policy despite c5 being undescribable");
        assertTrue(message(cause(thrown)).contains("cleanup.policy=compact"),
                "the message must name c4's policy: " + message(cause(thrown)));
    }

    /**
     * A stale {@code parsley.*} key in the Streams {@link Properties} fails assembly loudly:
     * Parsley has no configuration keys, and a key that wires nothing must not parse quietly
     * ({@link ParsleyConfig#requireNoParsleyKeys}) — pinned here end to end through
     * {@link CausalTopology#assemble}.
     *
     * Asserts assembly throws an {@link IllegalStateException} naming the offending key.
     */
    @Test
    void aParsleyKeyInTheStreamsPropertiesFailsAssembly() {
        Properties props = config();
        props.put("parsley.topology.validation", "warn");
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> assemble(builder, ADMIN, props),
                "a parsley.* key must fail assembly — Parsley has no configuration keys");
        assertTrue(message(thrown).contains("parsley.topology.validation"),
                "the failure must name the offending key: " + message(thrown));
    }

    // --- strict sink resolution at init (R1: sinks must exist at startup) ----------------------

    /**
     * A declared sink whose UUID lookup fails at init — the topic does not exist, or the admin
     * call fails — fails init loudly instead of silently disabling own-output tracking for the
     * task's lifetime: an unresolved sink would drop every acknowledgement fold for it, so stamps
     * would under-claim this node's own outputs and a downstream consumer could deliver an effect
     * before its cause.
     *
     * Asserts driver construction throws, with an {@link IllegalStateException} in the chain
     * naming the sink and the sinks-must-exist remedy.
     */
    @Test
    void aSinkWhoseUuidLookupFailsFailsInitNamingTheSink() throws IOException {
        ParsleyTopicAdmin admin = TestTopicAdmin.of(Map.of("c1", C1_ID));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin);

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "a declared sink that cannot be resolved must fail init, not start with own-output "
                        + "tracking silently off");
        assertTrue(message(cause(thrown)).contains("declared sink topic 'c3'"),
                "the failure must name the unresolvable sink: " + message(cause(thrown)));
        assertTrue(message(cause(thrown)).contains("must exist before the stage starts"),
                "the failure must state the sinks-must-exist rule: " + message(cause(thrown)));
    }

    /**
     * The broker answering the sink lookup without a UUID (no exception, no id — e.g. a describe
     * response missing the topic) is the same failure as a thrown lookup: the sink is unresolved,
     * so init must fail rather than proceed without own-output tracking for it.
     *
     * Asserts driver construction throws naming the sink.
     */
    @Test
    void aSinkResolvedWithoutAUuidFailsInitNamingTheSink() throws IOException {
        ParsleyTopicAdmin admin = FlakySinkAdmin.withUndescribable(Map.of("c1", C1_ID), Set.of());
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin);

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "a sink the broker answered without a UUID must fail init like a thrown lookup");
        assertTrue(message(cause(thrown)).contains("declared sink topic 'c3'"),
                "the failure must name the unresolved sink: " + message(cause(thrown)));
    }

    /**
     * A declared sink whose end offsets cannot be read at init fails init loudly instead of
     * skipping the {@code ownOutputs} seed: the persisted clock deliberately trails the previous
     * run's final-transaction acks, and only the end-offset seed heals that window — live acks
     * never cover it — so starting unseeded could stamp under-claims of the node's own
     * last-transaction outputs.
     *
     * Asserts driver construction throws, with an {@link IllegalStateException} in the chain
     * naming the sink and the seed.
     */
    @Test
    void aSinkWhoseEndOffsetsCannotBeReadFailsInit() throws IOException {
        ParsleyTopicAdmin admin = TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withFailingEndOffsets(Set.of("c3"));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin);

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "a failed sink end-offset read must fail init — the ownOutputs seed cannot be skipped");
        assertTrue(message(cause(thrown)).contains("end offsets for declared sink topic 'c3'"),
                "the failure must name the sink whose end offsets failed: "
                        + message(cause(thrown)));
    }

    /**
     * A corrupted {@code parsley-causal-clock} header fails the whole assembled topology closed —
     * proving the fail-closed model collapses correctly through the full stack ({@link
     * CausalStreamsBuilder} → {@link CausalTopology} → real {@link Topology}), not just at the core
     * unit-test level. There is no dead-letter sink to divert to: an undecodable header always crashes
     * the task.
     *
     * Asserts processing the record throws (wrapped by Kafka Streams in a {@code StreamsException}) with
     * a {@link CausalVectorClockResolutionException} cause, and the delegate never runs.
     */
    @Test
    void unresolvableClockHeaderFailsTheWholeTopologyClosed() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, ADMIN);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            Headers corrupted = ParsleyHeader.mutableHeaders();
            corrupted.add(ParsleyHeader.CAUSAL_CLOCK, new byte[] {(byte) 0xFF});

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> c1.pipeInput(new TestRecord<>("k", "v", corrupted)),
                    "an undecodable causal-dependencies header must fail the task rather than be diverted");
            assertEquals(CausalVectorClockResolutionException.class, cause(thrown).getClass(),
                    "the wrapped cause must be the clock-resolution guard's exception");
            assertTrue(processed.isEmpty(), "the delegate must never run on a record that fails closed at ingest");
        }
    }

    /**
     * Regression test for BACKLOG.md's write-ordering-overclaim finding: {@link CausalTopology#assemble}
     * requires {@code processing.guarantee=exactly_once_v2} unconditionally, since the
     * crash-safety reasoning throughout {@code ParsleyCausalBroadcast}/{@code ParsleyChannels} (a torn write always
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
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());

        Properties atLeastOnce = config();
        atLeastOnce.remove(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> assemble(builder, ADMIN, atLeastOnce),
                "assemble() must fail fast without exactly_once_v2");
        assertTrue(message(thrown).contains("exactly_once_v2"),
                "the message must name the required setting: " + message(thrown));
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

    // --- own outputs ----------------------------------------------------------------

    /**
     * Task init seeds the {@code ownOutputs} clock from each resolved sink's end offsets: with the
     * sink's end offset at 5, the clock claims the last appended position 4 — whether or not this
     * task produced it (the over-claim path; it heals the persisted blob trailing the crashed
     * transaction's acks). The seed is persisted in the {@code "frontier"} value and rides the
     * outbound stamp — {@code completeness ∪ ownOutputs} — so a downstream consumer of
     * the sink gates a derived record behind this node's whole appended prefix.
     *
     * Asserts the persisted own-output clock holds end offset - 1 for the sink and the emitted
     * record's dependency stamp names the sink coordinate at exactly that position.
     */
    @Test
    void initSeedsOwnOutputsFromSinkEndOffsetsIntoTheStamp() {
        Uuid c3Id = Uuid.randomUuid();
        ParsleyTopicAdmin admin = TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", c3Id))
                .withEndOffsets(Map.of("c3", Map.of(0, 5L)));
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Topology topology = assemble(builder, admin);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c3", new StringDeserializer(), new StringDeserializer());
            c1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalClock.empty())));

            TestRecord<String, String> emitted = out.readRecord();
            ParsleyVectorClock stamp = ParsleyVectorClock.fromBytes(
                    emitted.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK).value());
            assertEquals(4L, stamp.offsetFor(c3Id, 0),
                    "the seeded own-output coordinate must ride the stamp (completeness ∪ "
                            + "ownOutputs)");

            KeyValueStore<String, byte[]> frontierStore =
                    driver.getKeyValueStore("causal-streams-test-stage-1-frontier");
            ParsleyChannels persisted = new ParsleyChannels(frontierStore, new MockForwardedIndex());
            assertEquals(4L, persisted.ownOutputs().offsetFor(c3Id, 0),
                    "init must seed ownOutputs at the sink's end offset - 1 and persist it");
        }
    }

    /** A loadable stand-in for a user-configured producer interceptor (instantiated reflectively). */
    public static final class UserNoopInterceptor implements org.apache.kafka.clients.producer.ProducerInterceptor<Object, Object> {
        @Override
        public org.apache.kafka.clients.producer.ProducerRecord<Object, Object> onSend(
                org.apache.kafka.clients.producer.ProducerRecord<Object, Object> producerRecord) {
            return producerRecord;
        }

        @Override
        public void onAcknowledgement(org.apache.kafka.clients.producer.RecordMetadata metadata, Exception exception) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    /**
     * {@link CausalStreams#registerOwnOutputTracking} wires the own-output ack machinery through
     * public configuration alone: it appends {@link ParsleyOwnOutputInterceptor} to the
     * user-configured {@code producer.interceptor.classes} — never replacing them — injects the
     * minted registry id under the {@code producer.} prefix, and registers a registry tracking
     * exactly the declared sink topics. Unit-covered directly because the construction path now
     * applies it to a copy of the caller's Properties, which is not observable from outside.
     *
     * Asserts the user's interceptor is preserved ahead of Parsley's and the registry id resolves
     * to a registry tracking the sink (and not the source).
     */
    @Test
    void registerOwnOutputTrackingAppendsTheInterceptorAfterTheUsers() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Properties props = config();
        String interceptorsKey = "producer." + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
        props.put(interceptorsKey, UserNoopInterceptor.class.getName());

        String registryId = CausalStreams.registerOwnOutputTracking(builder.build(), props);
        try {
            assertEquals(UserNoopInterceptor.class.getName() + "," + ParsleyOwnOutputInterceptor.class.getName(),
                    props.get(interceptorsKey),
                    "Parsley's interceptor must be appended after the user's, never replacing it");
            assertEquals(registryId, props.get("producer." + ParsleyOwnOutputRegistry.CONFIG_KEY),
                    "the minted registry id must ride the producer. prefix for tasks to resolve");
            ParsleyOwnOutputRegistry registry = ParsleyOwnOutputRegistry.lookup(registryId);
            assertNotNull(registry, "the minted registry id must resolve while registered");
            assertTrue(registry.tracks("c3"), "the registry must track the declared sink topic");
            assertFalse(registry.tracks("c1"), "a source topic is not an own-output coordinate");
        } finally {
            ParsleyOwnOutputRegistry.unregister(registryId);
        }
    }

    /**
     * {@link CausalStreams} construction works on a copy of the caller's {@link Properties}: the
     * interceptor entry and the minted registry/watch ids are injected into the copy only, so the
     * caller's object is never mutated and two instances built from one {@code Properties} cannot
     * duplicate the interceptor entry or cross-wire each other's registry ids.
     *
     * Asserts the caller's Properties gain no Parsley wiring keys, the two instances mint distinct
     * registries each tracking the sink, and {@code close()} unregisters each.
     */
    @Test
    void causalStreamsCopiesTheCallersPropertiesAndManagesTheRegistryLifecycle() throws IOException {
        CausalStreamsBuilder firstBuilder = new CausalStreamsBuilder();
        firstBuilder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        CausalStreamsBuilder secondBuilder = new CausalStreamsBuilder();
        secondBuilder.stream("c1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("c3-sink", "c3", Serdes.String(), Serdes.String());
        Properties props = config(tempStateDir());
        // KafkaStreams construction creates (but never connects) real clients, and client creation
        // resolves the bootstrap hostname eagerly — so unlike the TopologyTestDriver tests' "dummy",
        // this needs a resolvable (if dead) address.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        String interceptorsKey = "producer." + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
        props.put(interceptorsKey, UserNoopInterceptor.class.getName());

        String firstId;
        try (CausalStreams first = new CausalStreams(firstBuilder.build(), props)) {
            assertEquals(UserNoopInterceptor.class.getName(), props.get(interceptorsKey),
                    "the caller's interceptor entry must be untouched — no Parsley append");
            assertNull(props.get("producer." + ParsleyOwnOutputRegistry.CONFIG_KEY),
                    "the caller's Properties must not gain the minted registry id");
            firstId = first.ownOutputRegistryId();
            ParsleyOwnOutputRegistry firstRegistry = ParsleyOwnOutputRegistry.lookup(firstId);
            assertNotNull(firstRegistry, "the first instance's registry id must resolve while it lives");
            assertTrue(firstRegistry.tracks("c3"), "the registry must track the declared sink topic");
        }
        assertNull(ParsleyOwnOutputRegistry.lookup(firstId),
                "close() must unregister the registry so the JVM-wide map cannot leak instances");

        // The same, still-unmutated Properties object builds a second cleanly wired instance — the
        // duplicated-interceptor / cross-wired-id bug this copy guards against. (Sequential, not
        // concurrent: two live instances from one config would collide on the state directory at
        // the Kafka Streams level regardless.)
        String secondId;
        try (CausalStreams second = new CausalStreams(secondBuilder.build(), props)) {
            assertEquals(UserNoopInterceptor.class.getName(), props.get(interceptorsKey),
                    "the caller's interceptor entry must still be untouched after a second construction");
            secondId = second.ownOutputRegistryId();
            assertFalse(firstId.equals(secondId),
                    "each construction must mint its own registry — ids must never cross-wire");
            ParsleyOwnOutputRegistry secondRegistry = ParsleyOwnOutputRegistry.lookup(secondId);
            assertNotNull(secondRegistry, "the second instance's registry id must resolve while it lives");
            assertTrue(secondRegistry.tracks("c3"), "the registry must track the declared sink topic");
        }
        assertNull(ParsleyOwnOutputRegistry.lookup(secondId),
                "close() must unregister the second registry so the JVM-wide map cannot leak instances");
    }

    /**
     * A {@link ParsleyTopicAdmin} test double that resolves the given topic UUIDs ({@code null} for
     * a topic absent from the map — the broker answered, but knows no such topic) and reports the
     * given per-topic partition counts (default 1) and cleanup policies (default {@code "delete"}),
     * but throws when {@code partitionCounts}/{@code cleanupPolicies} is asked about any topic in
     * {@code undescribableTopics} — simulating a transient describe failure against a sink. Unlike
     * {@link TestTopicAdmin}, this throws <strong>per call</strong> if ANY requested topic is
     * undescribable (matching the real {@code Admin}'s all-or-nothing batch behaviour), which is what
     * exercises {@link ParsleyProcessor}'s per-topic resolution of the sink lints. {@code endOffsets}
     * always succeeds (an empty topic), so init's strict sink resolution passes for any sink whose
     * UUID is in {@code topicIds}.
     */
    private static final class FlakySinkAdmin implements ParsleyTopicAdmin {

        private final Map<String, Uuid> topicIds;
        private final Map<String, Integer> partitionCounts;
        private final Map<String, String> cleanupPolicies;
        private final Set<String> undescribableTopics;

        FlakySinkAdmin(Map<String, Uuid> topicIds, Map<String, Integer> partitionCounts,
                Map<String, String> cleanupPolicies, Set<String> undescribableTopics) {
            this.topicIds = topicIds;
            this.partitionCounts = partitionCounts;
            this.cleanupPolicies = cleanupPolicies;
            this.undescribableTopics = undescribableTopics;
        }

        static FlakySinkAdmin withUndescribable(Map<String, Uuid> topicIds, Set<String> undescribableTopics) {
            return new FlakySinkAdmin(topicIds, Map.of(), Map.of(), undescribableTopics);
        }

        @Override
        public Map<String, Uuid> topicIds(List<String> topics) {
            Map<String, Uuid> resolved = new HashMap<>();
            topics.forEach(t -> resolved.put(t, topicIds.get(t)));
            return resolved;
        }

        @Override
        public Map<String, Integer> partitionCounts(List<String> topics) throws Exception {
            failIfAnyUndescribable(topics);
            Map<String, Integer> counts = new HashMap<>();
            topics.forEach(t -> counts.put(t, partitionCounts.getOrDefault(t, 1)));
            return counts;
        }

        @Override
        public Map<String, String> cleanupPolicies(List<String> topics) throws Exception {
            failIfAnyUndescribable(topics);
            Map<String, String> policies = new HashMap<>();
            topics.forEach(t -> policies.put(t, cleanupPolicies.getOrDefault(t, "delete")));
            return policies;
        }

        private void failIfAnyUndescribable(List<String> topics) throws Exception {
            for (String topic : topics) {
                if (undescribableTopics.contains(topic)) {
                    throw new Exception("transient describe failure for topic '" + topic + "'");
                }
            }
        }

        @Override
        public Map<Integer, Long> endOffsets(String topic) {
            return Map.of();
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

}
