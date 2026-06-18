package io.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CausalProcessors} — the decorating causal processor — through a real Kafka Streams
 * topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalProcessorsTopologyTest {

    private static final Uuid IN_ID     = CausalPosition.deriveUuid("in");
    private static final Uuid PRICES_ID = CausalPosition.deriveUuid("prices");
    private static final Uuid ORDERS_ID = CausalPosition.deriveUuid("orders");
    private static final Uuid QUOTES_ID = CausalPosition.deriveUuid("quotes");

    private final List<String> processed = new ArrayList<>();
    private final List<CausalViolation> violations = new ArrayList<>();
    private final CausalViolationHandler onViolation = violations::add;

    // --- helpers -------------------------------------------------------------------------------

    private static Properties config(File stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "decorator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        if (stateDir != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        }
        return props;
    }

    private static Headers depsHeader(CausalDependencies deps) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("parsley-causal-dependencies", deps.toBytes()));
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

    private static Topology topology(
            ProcessorSupplier<String, String, String, String> decorated, List<String> inputTopics) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopics, Consumed.with(Serdes.String(), Serdes.String()))
                .process(decorated)
                .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static CausalDependencies outDeps(TestRecord<String, String> record) {
        return CausalDependencies.fromHeaders(record.headers()).orElseThrow();
    }

    // --- tests ---------------------------------------------------------------------------------

    @Test
    void admittedRecordRunsDelegateAndStampsTheMergedClock() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
            assertEquals(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build(), outDeps(emitted),
                    "forward must be stamped with the frontier as of admission");
            assertTrue(violations.isEmpty());
        }
    }

    @Test
    void heldRecordIsBufferedThenDrainedThroughDelegate() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("prices", "orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // Order depends on prices-0 offset 0, which hasn't arrived: held, not delivered.
            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 0)).build())));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertTrue(out.isEmpty());
            assertEquals(1, storeSize(bufferStore), "held record must be persisted to the buffer store");

            // Price arrives and advances the frontier, draining the order through the delegate.
            prices.pipeInput(new TestRecord<>("k", "price", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("price", "order"), processed);
            assertEquals(List.of("PRICE", "ORDER"), out.readValuesToList());
            assertEquals(0, storeSize(bufferStore), "drained record must be removed from the buffer store");
        }
    }

    @Test
    void strictDropDoesNotInvokeDelegateAndReportsTheGap() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Depends on a price that never arrives; size limit 1 evicts immediately.
            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 99)).build())));

            assertTrue(processed.isEmpty(), "strict policy must never run the delegate for an un-satisfiable record");
            assertTrue(out.isEmpty());
            assertEquals(1, violations.size());
            CausalViolation violation = violations.get(0);
            assertEquals(CausalViolationReason.LIMIT_REACHED, violation.reason());
            assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 100L)), violation.gap(),
                    "gap is required(99) minus offsetFor(absent=-1) = 100");
        }
    }

    @Test
    void deadLetterRoutesToSinkAndDoesNotInvokeDelegate() {
        List<String> deadLettered = new ArrayList<>();
        CausalBufferPolicy policy = CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1));
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), policy)
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation)
                        .deadLetterSink(cr -> deadLettered.add(cr.value())).build(),
                List.of("orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 99)).build())));

            assertTrue(processed.isEmpty(), "strict policy must never run the delegate");
            assertTrue(out.isEmpty());
            assertEquals(List.of("order"), deadLettered);
            assertEquals(1, violations.size());
            assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(0).reason());
        }
    }

    @Test
    void forwardUnsafeRunsDelegateUnderLagAndFlagsTheViolation() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 99)).build())));

            assertEquals(List.of("order"), processed,
                    "lenient policy delivers the un-satisfied record to the delegate");
            assertEquals(List.of("ORDER"), out.readValuesToList());
            assertEquals(1, violations.size());
            assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(0).reason());
            assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 100L)), violations.get(0).gap());
        }
    }

    @Test
    void stampingIsIdempotentAndPreservesUserHeaders() {
        ProcessorSupplier<String, String, String, String> user = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                Headers headers = new RecordHeaders();
                headers.add(new RecordHeader("user-h", "keep".getBytes()));
                // A stale dependencies header the user happens to carry — stamping must replace, not duplicate it.
                headers.add(new RecordHeader("parsley-causal-dependencies",
                        CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 5)).build().toBytes()));
                ctx.forward(record.withHeaders(headers));
            }
        };
        Topology topology = topology(
                CausalProcessors.builder(user, CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));

            TestRecord<String, String> emitted = out.readRecord();
            assertEquals(1, count(emitted.headers(), "parsley-causal-dependencies"),
                    "exactly one dependencies header — stamping is idempotent");
            assertEquals(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build(), outDeps(emitted),
                    "the stamped dependencies are the frontier, not the user's stale value");
            assertEquals("keep", new String(emitted.headers().lastHeader("user-h").value()),
                    "user headers are preserved");
        }
    }

    @Test
    void userPunctuatorForwardsAreStampedWithTheLiveFrontier() {
        ProcessorSupplier<String, String, String, String> user = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
                ctx.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME,
                        ts -> ctx.forward(new Record<>("p", "punct", ts)));
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record);
            }
        };
        Topology topology = topology(
                CausalProcessors.builder(user, CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));
            out.readRecord(); // the live record
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            TestRecord<String, String> punctuated = out.readRecord();
            assertEquals("punct", punctuated.value());
            assertEquals(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build(), outDeps(punctuated),
                    "punctuator forwards are stamped with the live frontier");
        }
    }

    @Test
    void delegateSeesAWorkingStateStoreAndRecordMetadata() {
        ProcessorSupplier<String, String, String, String> user = new ProcessorSupplier<>() {
            @Override
            public Processor<String, String, String, String> get() {
                return new Processor<>() {
                    private ProcessorContext<String, String> ctx;
                    private KeyValueStore<String, String> store;

                    @Override
                    public void init(ProcessorContext<String, String> context) {
                        this.ctx = context;
                        this.store = context.getStateStore("u-state");
                    }

                    @Override
                    public void process(Record<String, String> record) {
                        store.put(record.key(), record.value());            // causally consistent write
                        String topic = ctx.recordMetadata().map(m -> m.topic()).orElse("?");
                        ctx.forward(record.withValue(store.get(record.key()) + "@" + topic));
                    }
                };
            }

            @Override
            public Set<StoreBuilder<?>> stores() {
                return Set.of(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("u-state"), Serdes.String(), Serdes.String()));
            }
        };
        Topology topology = topology(
                CausalProcessors.builder(user, CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));

            assertEquals("v@in", out.readValue(), "getStateStore and recordMetadata pass through the proxy");
            assertEquals("v", driver.<String, String>getKeyValueStore("u-state").get("k"),
                    "the user's store write is visible");
        }
    }

    // Note: restart/restoration is covered by ParsleySerializerTest (envelope round-trip) and
    // ParsleyEngineTest#recordsAlreadyInTheBufferDrainWhenTheFrontierCatchesUp (the buffer store is
    // the source of truth, so held records are simply already present after a restart).
    // TopologyTestDriver does not persist state-store contents across driver instances, so a
    // genuine cross-restart test cannot be expressed against it.

    @Test
    void bufferSerdesAreResolvedAndInvokedWithTheSourceTopic() {
        SpyStringSerde valueSpy = new SpyStringSerde();
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> valueSpy).onViolation(onViolation).build(),
                List.of("prices", "orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());

            // An unmet dependency forces the order to be buffered, which serialises it.
            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 5)).build())));

            assertTrue(valueSpy.serializeTopics.contains("orders"),
                    "the buffer value serde must be invoked with the record's source topic, not the changelog name");
            assertEquals(List.of("orders"), valueSpy.serializeTopics.stream().distinct().toList(),
                    "only the source topic 'orders' is used to serialise the held record");
        }
    }

    @Test
    void twoDecoratorsWithDistinctStoreNamesCoexistAndKeepIsolatedFrontiers() {
        // Two causal decorators in one topology. With the default storeName both would register
        // "parsley-frontier"/"parsley-buffer" and Kafka Streams would reject the topology; distinct
        // namespaces are what make multiple decorators possible.
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> Serdes.String()).onViolation(onViolation).storeName("orders").build())
                .to("orders-out", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("prices", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> Serdes.String()).onViolation(onViolation).storeName("prices").build())
                .to("prices-out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> ordersOut =
                    driver.createOutputTopic("orders-out", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> pricesOut =
                    driver.createOutputTopic("prices-out", new StringDeserializer(), new StringDeserializer());

            orders.pipeInput(new TestRecord<>("k", "order", depsHeader(CausalDependencies.empty())));
            prices.pipeInput(new TestRecord<>("k", "price", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("ORDER"), ordersOut.readValuesToList());
            assertEquals(List.of("PRICE"), pricesOut.readValuesToList());

            // Each decorator persisted only its own branch's frontier under its own namespace.
            assertEquals(CausalFrontier.empty().observe(new CausalPosition(ORDERS_ID, 0, 0)), frontierIn(driver, "orders-frontier"));
            assertEquals(CausalFrontier.empty().observe(new CausalPosition(PRICES_ID, 0, 0)), frontierIn(driver, "prices-frontier"));
        }
    }

    @Test
    void frontierListenerPublishesRestoredThenAdvancingFrontiers() {
        List<CausalFrontier> offsetFor = new ArrayList<>();
        CausalFrontierListener listener = offsetFor::add;
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> Serdes.String()).onViolation(onViolation).storeName("in").frontierListener(listener).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());

            // The processor publishes its restored frontier at init — empty on a cold start — so an
            // observer's view is correct before the first record is admitted.
            assertEquals(List.of(CausalFrontier.empty()), offsetFor,
                    "the restored frontier is published once at startup");

            in.pipeInput(new TestRecord<>("k", "a", depsHeader(CausalDependencies.empty())));
            in.pipeInput(new TestRecord<>("k", "b", depsHeader(CausalDependencies.empty())));

            assertEquals(
                    List.of(CausalFrontier.empty(),
                            CausalFrontier.empty().observe(new CausalPosition(IN_ID, 0, 0)),
                            CausalFrontier.empty().observe(new CausalPosition(IN_ID, 0, 1))),
                    offsetFor,
                    "every frontier advance is published, in admission order");
        }
    }

    @Test
    void automaticStampFrontierIsBoundedByInputTopicCountNotRecordCount() {
        // The automatically stamped dependencies reflect the per-task frontier, which advances only
        // by each record's own source coordinate (one partition per source topic) — so its size is
        // bounded by the number of source topics in the subtopology, never by the number of records.
        // This pins the O(#input topics) envelope the docs promise for the Streams path.
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1000)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("a", "b", "c"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> a =
                    driver.createInputTopic("a", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> b =
                    driver.createInputTopic("b", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c =
                    driver.createInputTopic("c", new StringSerializer(), new StringSerializer());

            for (int i = 0; i < 50; i++) {
                a.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
                b.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
                c.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
            }

            CausalFrontier frontier = frontierIn(driver, "parsley-frontier");
            assertEquals(3, frontier.positions().size(),
                    "the stamped frontier is bounded by the number of source topics, not record count");
        }
    }

    @Test
    void aLargeInboundDependencySetIsNeverFoldedIntoTheStampedOutput() {
        // The most important regression guard: inbound dependencies are used only to gate the record;
        // they must never be merged into the frontier and re-stamped. A record carrying dependencies
        // over hundreds of (unrelated) partitions, force-forwarded under forwardUnsafe, must still be
        // stamped with only its own source coordinate — proving the inbound dependencies cannot
        // amplify the stamped output and breach the header-size budget.
        CausalDependencies.Builder bigBuilder = CausalDependencies.builder();
        for (int p = 0; p < 500; p++) {
            bigBuilder.require(new CausalPosition(CausalPosition.deriveUuid("ghost"), p, 1_000 + p));
        }
        CausalDependencies big = bigBuilder.build();

        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "v", depsHeader(big)));

            CausalDependencies stamped = outDeps(out.readRecord());
            assertEquals(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build(), stamped,
                    "a 500-partition inbound dependency set must not enlarge the stamped output");
            assertEquals(1, stamped.dependencies().size(), "the stamped output carries only the source coordinate");
        }
    }

    @Test
    void streamsMetricsSensorsArePopulatedAfterBufferingAndRelease() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("prices", "orders"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());

            // Buffer one order (depends on prices-0 offset 0, not yet arrived).
            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 0)).build())));
            // Release it (the price arrives and advances the frontier).
            prices.pipeInput(new TestRecord<>("k", "price", depsHeader(CausalDependencies.empty())));

            assertEquals(1.0, parsleyMetric(driver, "records-buffered-total"), 0.001,
                    "one record was added to the buffer");
            assertEquals(1.0, parsleyMetric(driver, "records-released-total"), 0.001,
                    "one record was released from the buffer");
            assertEquals(0.0, parsleyMetric(driver, "buffer-depth"), 0.001,
                    "buffer is empty after the drain");
        }
    }

    /**
     * A record whose dependencies contain its own source coordinate is forwarded immediately: the
     * self-referential entry is stripped at admission, leaving empty dependencies (satisfied).
     *
     * <p>The buffer size limit is set to 1 so any attempt to buffer the record would fire
     * {@link CausalViolationReason#LIMIT_REACHED} immediately — proving the record never
     * entered the buffer path.
     */
    @Test
    void selfReferentialDependency_isStrippedAndForwardedImmediately() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // Dependencies require IN_ID/0@0 — exactly the record's own source coordinate.
            in.pipeInput(new TestRecord<>("k", "hello",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "self-dep stripped → empty dependencies → forwarded immediately");
            assertTrue(violations.isEmpty(), "no violation: the circular entry is silently removed");
            assertEquals(0, storeSize(bufferStore),
                    "record must never enter the buffer even with size limit 1");
        }
    }

    /**
     * Verifies that a fused two-processor chain (no intermediate Kafka topic) does not deadlock
     * when proc1's frontier stamp includes the current record's own source coordinate.
     *
     * <p>In a fused topology, proc2's {@code context.recordMetadata()} returns the original source
     * metadata, so the forwarded record has the same source coordinate as its dependency entry.
     * Without the self-reference strip this would cause every forwarded record to be held
     * indefinitely in proc2's buffer.
     */
    @Test
    void fusedChain_proc1StampDoesNotCircularlyBlockProc2() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        // Fused chain: proc1 → proc2, no .to("intermediate") between them.
        // proc1 admits the record, stamps its frontier (which now contains the source coord), and
        // forwards. proc2 receives the stamped record; because the topology is fused,
        // context.recordMetadata() still returns the original "in" metadata → self-reference dep.
        builder.stream("in", consumed)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "record must flow through both processors and reach the output");
            assertTrue(proc1Violations.isEmpty(), "proc1 must not violate");
            assertTrue(proc2Violations.isEmpty(),
                    "proc2 must not violate: self-dep is stripped before evaluation");
            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")),
                    "proc2 buffer must be empty — record was never held");
        }
    }

    /**
     * Verifies that two {@link CausalProcessors} instances chained via a materialized Kafka topic
     * each enforce causal ordering independently, with full drainage at both layers (backlog #14).
     *
     * <h2>Topology</h2>
     * <pre>
     *   [prices, orders] ──→ proc1("first") ──→ "quotes" (materialized)
     *                                                   ↓
     *   [prices, orders] ──────────────────────→ proc2("second") ──→ out
     * </pre>
     * Proc2 subscribes to "quotes" (proc1's derived output) AND to "prices"/"orders" directly.
     *
     * <h2>Why materialization is required</h2>
     * Without {@code .to("quotes")} the chain is fused: {@code recordMetadata().topic()} in
     * proc2 returns the original Kafka source topic, so every forwarded record's dep includes its
     * own source coordinate. Self-referential entries are stripped at engine admission time, so the
     * forwarded record is forwarded immediately rather than held indefinitely — but with
     * materialization, quotes records have source {@code QUOTES_ID} and deps
     * {@code {PRICES_ID@x, ORDERS_ID@y}}: no self-reference at all, and the causal ordering across
     * two layers is preserved. See {@code fusedChain_proc1StampDoesNotCircularlyBlockProc2} for
     * the targeted regression guard.
     *
     * <h2>Why distinct {@code storeName} values are required</h2>
     * Each {@code CausalProcessorSupplier} registers three KeyValueStores
     * ({@code {name}-frontier}, {@code {name}-buffer}, {@code {name}-wait-index}). Without
     * distinct names Kafka Streams rejects the topology with a duplicate-store error.
     *
     * <h2>How proc2 bootstraps without circular dependency</h2>
     * Direct "prices" and "orders" subscriptions advance proc2's frontier on {@code PRICES_ID}
     * and {@code ORDERS_ID}. Once both dimensions are satisfied, proc2 drains the buffered
     * quotes records. Both buffer stores are empty at the end.
     */
    @Test
    void chainedProcessors_materializationEnablesFullDrainAtBothLayers() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String>  consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String>  produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        // Each source topic is declared once; the DSL branches the node when the same KStream
        // object appears in multiple merge() calls. Declaring separate builder.stream() calls
        // for the same topic would create overlapping source nodes, which Kafka Streams rejects.
        // Each source topic is declared once; the DSL branches the node when the same KStream
        // object appears in multiple merge() calls. Declaring separate builder.stream() calls
        // for the same topic would create overlapping source nodes, which Kafka Streams rejects.
        var pricesSrc = builder.stream("prices", consumed);
        var ordersSrc = builder.stream("orders", consumed);
        var quotesSrc = builder.stream("quotes",  consumed);

        // Proc1: holds orders (dep on prices) until prices arrive. Output materializes to "quotes"
        // — a new derived event type. Quotes records carry dep {PRICES_ID@x, ORDERS_ID@y} from
        // proc1's frontier stamp; their source is QUOTES_ID (not self-stamped).
        pricesSrc.merge(ordersSrc)
                .process(CausalProcessors.builder(upperCaser(),
                             CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .to("quotes", produced);

        // Proc2: receives "quotes" (proc1's derived output) AND "prices"/"orders" directly
        // (the same source KStream objects, branched by the DSL). Direct prices/orders bootstrap
        // proc2's frontier on PRICES_ID and ORDERS_ID. Once satisfied, proc2 drains the buffered
        // quotes records without any circular dependency.
        quotesSrc.merge(pricesSrc).merge(ordersSrc)
                .process(CausalProcessors.builder(upperCaser(),
                             CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdesByTopic(t -> Serdes.String(), t -> Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Phase 1: orders arrive before prices; held at proc1 AND at proc2 (direct subscription).
            orders.pipeInput(new TestRecord<>("k", "order",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 0)).build())));
            assertTrue(out.isEmpty(), "orders must be held before prices arrive");
            assertEquals(1, storeSize(driver.getKeyValueStore("first-buffer")),
                    "proc1 must buffer the order");
            assertEquals(1, storeSize(driver.getKeyValueStore("second-buffer")),
                    "proc2 must buffer the direct order");

            // Phase 2: prices arrives — proc1 drains and materializes two quotes records, then
            // proc2 bootstraps its frontier from the direct prices/orders and drains both quotes.
            prices.pipeInput(new TestRecord<>("k", "price",
                    depsHeader(CausalDependencies.empty())));

            // Both layers drained completely; no records stuck in either buffer.
            assertEquals(0, storeSize(driver.getKeyValueStore("first-buffer")));
            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")));

            // "out" has 4 records: direct price, direct order (proc2's own admissions) plus
            // the proc1-forwarded price and order (arriving via "quotes"). Exact ordering is
            // TopologyTestDriver-dependent; assert the value multiset.
            List<String> outValues = out.readValuesToList();
            assertEquals(4, outValues.size());
            assertEquals(2, outValues.stream().filter("PRICE"::equals).count(),
                    "two PRICE records: direct and via quotes");
            assertEquals(2, outValues.stream().filter("ORDER"::equals).count(),
                    "two ORDER records: direct and via quotes");

            // Proc1's frontier spans both source topics after draining.
            assertEquals(CausalFrontier.empty().observe(new CausalPosition(PRICES_ID, 0, 0)).observe(new CausalPosition(ORDERS_ID, 0, 0)),
                    frontierIn(driver, "first-frontier"));

            // Proc2's frontier spans prices and orders (from direct admissions) plus quotes@1
            // (the last quotes record proc2 drained).
            CausalFrontier f2 = frontierIn(driver, "second-frontier");
            assertEquals(0L, f2.offsetFor(PRICES_ID, 0), "proc2 saw prices directly");
            assertEquals(0L, f2.offsetFor(ORDERS_ID, 0), "proc2 saw orders directly");
            assertEquals(1L, f2.offsetFor(QUOTES_ID, 0), "proc2 drained both quotes records");

            assertTrue(proc1Violations.isEmpty(),
                    "proc1 must not violate: prices has empty dependencies, orders has real dependencies");
            assertTrue(proc2Violations.isEmpty(),
                    "proc2 must not violate: all records carry valid dependencies");
        }
    }

    /**
     * Materialized chain where proc2 also consumes a genuinely independent third topic
     * ("discounts") whose records carry a causal dep on the derived "quotes" topic.
     *
     * <h2>Topology</h2>
     * <pre>
     *   "prices" ──→ proc1("first") ──→ "quotes" (materialized)
     *                                          ↓
     *   "prices" ─────────────────────→ proc2("second") ──→ "out"
     *   "discounts" ──────────────────→ proc2("second")
     * </pre>
     * The discount record arrives at proc2 before "quotes" has been produced, so it is buffered.
     * Once proc1 materialises quotes@0, proc2's frontier advances through PRICES (direct) and then
     * QUOTES (quotes@0 drains), satisfying the discount's dep and releasing it last.
     */
    @Test
    void materializationChain_clockedSidecar_bufferedUntilDerivedTopicArrives() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        var pricesSrc    = builder.stream("prices",    consumed);
        var quotesSrc    = builder.stream("quotes",    consumed);
        var discountsSrc = builder.stream("discounts", consumed);

        pricesSrc
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .to("quotes", produced);

        // proc2 takes proc1's materialised output, the original prices (to bootstrap PRICES
        // frontier), and the independent "discounts" stream.
        quotesSrc.merge(pricesSrc).merge(discountsSrc)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> discounts =
                    driver.createInputTopic("discounts", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Discount arrives before quotes@0 has been produced — must be buffered.
            discounts.pipeInput(new TestRecord<>("k", "discount",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(QUOTES_ID, 0, 0)).build())));
            assertTrue(out.isEmpty(), "discount must be held: quotes@0 not yet in proc2's frontier");
            assertEquals(1, storeSize(driver.getKeyValueStore("second-buffer")));

            // Price arrives: proc1 materialises quotes@0 (stamped {PRICES_ID@0}).
            // Proc2's direct price subscription advances its PRICES frontier, allowing it to admit
            // quotes@0, which advances its QUOTES frontier and drains the discount.
            prices.pipeInput(new TestRecord<>("k", "price", depsHeader(CausalDependencies.empty())));

            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")),
                    "discount must have drained once quotes@0 was admitted");
            List<String> values = out.readValuesToList();
            assertEquals(3, values.size(), "price (direct), price (via quotes), and discount");
            assertEquals(2, values.stream().filter("PRICE"::equals).count());
            assertEquals(1, values.stream().filter("DISCOUNT"::equals).count());
            assertEquals("DISCOUNT", values.get(values.size() - 1),
                    "discount is last: buffered until quotes@0 advanced the QUOTES frontier");
            assertTrue(proc1Violations.isEmpty());
            assertTrue(proc2Violations.isEmpty());
        }
    }

    /**
     * Fused chain where proc2 also consumes a clocked independent topic ("discounts") whose
     * records carry a dep on the original "in" source.
     *
     * <h2>Topology</h2>
     * <pre>
     *   "in"        ──→ proc1("first") ──→ (fused) ──→ proc2("second") ──→ "out"
     *   "discounts" ──────────────────────────────────→ proc2("second")
     * </pre>
     * The discount arrives before proc1 has seen "in"@0, so proc2 buffers it. When proc1 processes
     * "in"@0 and fuses to proc2, the self-dep is stripped and proc2 admits the "in" record,
     * advancing its IN_ID frontier. The discount then drains immediately.
     *
     * <p>This proves the self-dep strip does not prevent the frontier from advancing for IN_ID:
     * proc2 correctly recognises that IN_ID@0 is now satisfied and releases the discount.
     */
    @Test
    void fusedChain_clockedSidecar_bufferedUntilFusedOutputAdvancesFrontier() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        var discountsSrc = builder.stream("discounts", consumed);

        // proc1 fused directly into proc2 (no .to("intermediate")); discounts also merge into proc2.
        builder.stream("in", consumed)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .merge(discountsSrc)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> discounts =
                    driver.createInputTopic("discounts", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Discount depends on in@0 which hasn't been processed yet — must be buffered.
            discounts.pipeInput(new TestRecord<>("k", "discount",
                    depsHeader(CausalDependencies.builder().require(new CausalPosition(IN_ID, 0, 0)).build())));
            assertTrue(out.isEmpty(), "discount must be held: IN_ID@0 not yet in proc2's frontier");
            assertEquals(1, storeSize(driver.getKeyValueStore("second-buffer")));

            // "in"@0 arrives: proc1 admits it, stamps frontier {IN_ID@0}, fuses to proc2.
            // Proc2's engine strips the self-dep → admits → frontier has IN_ID@0 → discount drains.
            in.pipeInput(new TestRecord<>("k", "in-record", depsHeader(CausalDependencies.empty())));

            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")));
            assertEquals(List.of("IN-RECORD", "DISCOUNT"), out.readValuesToList(),
                    "in-record admitted first (self-dep stripped), discount drains immediately after");
            assertTrue(proc1Violations.isEmpty());
            assertTrue(proc2Violations.isEmpty());
        }
    }

    /**
     * Materialized chain: an unclocked sidecar record is admitted immediately with a
     * {@link CausalViolationReason#MISSING_HEADER} violation and never enters the buffer,
     * regardless of where the materialized chain's frontier stands.
     */
    @Test
    void materializationChain_unclockedSidecar_admittedImmediately() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        var pricesSrc    = builder.stream("prices",    consumed);
        var quotesSrc    = builder.stream("quotes",    consumed);
        var discountsSrc = builder.stream("discounts", consumed);

        pricesSrc
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .to("quotes", produced);

        quotesSrc.merge(pricesSrc).merge(discountsSrc)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> discounts =
                    driver.createInputTopic("discounts", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Unclocked discount: no parsley-causal-dependencies header → MISSING_HEADER → forwarded
            // immediately (forwardUnsafe policy) even though the materialized chain hasn't produced anything yet.
            discounts.pipeInput(new TestRecord<>("k", "discount"));
            assertEquals(List.of("DISCOUNT"), out.readValuesToList(),
                    "unclocked record must be forwarded immediately without buffering under forwardUnsafe policy");
            assertEquals(1, proc2Violations.size());
            assertEquals(CausalViolationReason.MISSING_HEADER, proc2Violations.get(0).reason());
            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")));

            // Normal materialized flow continues without interference from the unclocked record.
            prices.pipeInput(new TestRecord<>("k", "price", depsHeader(CausalDependencies.empty())));
            assertEquals(2, out.readValuesToList().size(), "price (direct) and quotes-derived");
            assertEquals(1, proc2Violations.size(), "no new violations from clocked price/quotes");
            assertTrue(proc1Violations.isEmpty());
        }
    }

    /**
     * Fused chain: an unclocked sidecar record is admitted immediately with a
     * {@link CausalViolationReason#MISSING_HEADER} violation, interleaved correctly alongside the
     * fused proc1 output.
     */
    @Test
    void fusedChain_unclockedSidecar_admittedImmediately() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
        List<CausalViolation> proc1Violations = new ArrayList<>();
        List<CausalViolation> proc2Violations = new ArrayList<>();

        var discountsSrc = builder.stream("discounts", consumed);

        builder.stream("in", consumed)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc1Violations::add)
                        .storeName("first").build())
                .merge(discountsSrc)
                .process(CausalProcessors.builder(upperCaser(),
                                CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)))
                        .serdes(Serdes.String(), Serdes.String())
                        .onViolation(proc2Violations::add)
                        .storeName("second").build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> discounts =
                    driver.createInputTopic("discounts", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Unclocked discount forwarded immediately under forwardUnsafe policy, before proc1 has processed anything.
            discounts.pipeInput(new TestRecord<>("k", "discount"));
            assertEquals(List.of("DISCOUNT"), out.readValuesToList());
            assertEquals(1, proc2Violations.size());
            assertEquals(CausalViolationReason.MISSING_HEADER, proc2Violations.get(0).reason());
            assertEquals(0, storeSize(driver.getKeyValueStore("second-buffer")),
                    "unclocked record must never enter the buffer");

            // Fused proc1 output flows through proc2 normally alongside the earlier unclocked record.
            in.pipeInput(new TestRecord<>("k", "in-record", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("IN-RECORD"), out.readValuesToList());
            assertEquals(1, proc2Violations.size(), "no new violations from clocked in-record");
            assertTrue(proc1Violations.isEmpty());
        }
    }

    // --- small utilities -----------------------------------------------------------------------

    private static double parsleyMetric(TopologyTestDriver driver, String metricName) {
        Map<MetricName, ? extends Metric> all = driver.metrics();
        return all.entrySet().stream()
                .filter(e -> e.getKey().name().equals(metricName)
                          && e.getKey().group().contains("parsley"))
                .findFirst()
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .orElseThrow(() -> new AssertionError("Parsley metric not found: " + metricName));
    }

    private static CausalFrontier frontierIn(TopologyTestDriver driver, String frontierStoreName) {
        KeyValueStore<String, byte[]> store = driver.getKeyValueStore(frontierStoreName);
        return CausalFrontier.fromBytes(store.get("f"));
    }

    private static int storeSize(KeyValueStore<String, byte[]> store) {
        int n = 0;
        try (var it = store.all()) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        return n;
    }

    private static int count(Headers headers, String key) {
        int n = 0;
        for (var ignored : headers.headers(key)) {
            n++;
        }
        return n;
    }

    /** A String serde that records the topic argument passed to its serializer/deserializer. */
    static final class SpyStringSerde implements Serde<String> {
        final List<String> serializeTopics = new ArrayList<>();
        // Recorded for symmetry/debugging; no test asserts on it yet.
        @SuppressWarnings("unused")
        final List<String> deserializeTopics = new ArrayList<>();
        private final Serde<String> delegate = Serdes.String();

        @Override
        public Serializer<String> serializer() {
            return (topic, data) -> {
                serializeTopics.add(topic);
                return delegate.serializer().serialize(topic, data);
            };
        }

        @Override
        public Deserializer<String> deserializer() {
            return (topic, data) -> {
                deserializeTopics.add(topic);
                return delegate.deserializer().deserialize(topic, data);
            };
        }
    }
}
