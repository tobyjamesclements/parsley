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

    private static Headers clockHeader(CausalDependencies clock) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("parsley-vector-clock", clock.toBytes()));
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

    private static CausalDependencies outClock(TestRecord<String, String> record) {
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

            in.pipeInput(new TestRecord<>("k", "hello", clockHeader(CausalDependencies.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
            assertEquals(CausalDependencies.empty().advance(IN_ID, 0, 0), outClock(emitted),
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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 0))));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertTrue(out.isEmpty());
            assertEquals(1, storeSize(bufferStore), "held record must be persisted to the buffer store");

            // Price arrives and advances the frontier, draining the order through the delegate.
            prices.pipeInput(new TestRecord<>("k", "price", clockHeader(CausalDependencies.empty())));

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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 99))));

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
        CausalBufferPolicy policy = CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1), "dlq");
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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 99))));

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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 99))));

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
                // A stale clock the user happens to carry — stamping must replace, not duplicate it.
                headers.add(new RecordHeader("parsley-vector-clock",
                        CausalDependencies.empty().advance(PRICES_ID, 0, 5).toBytes()));
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

            in.pipeInput(new TestRecord<>("k", "v", clockHeader(CausalDependencies.empty())));

            TestRecord<String, String> emitted = out.readRecord();
            assertEquals(1, count(emitted.headers(), "parsley-vector-clock"),
                    "exactly one clock header — stamping is idempotent");
            assertEquals(CausalDependencies.empty().advance(IN_ID, 0, 0), outClock(emitted),
                    "the stamped clock is the frontier, not the user's stale clock");
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

            in.pipeInput(new TestRecord<>("k", "v", clockHeader(CausalDependencies.empty())));
            out.readRecord(); // the live record
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            TestRecord<String, String> punctuated = out.readRecord();
            assertEquals("punct", punctuated.value());
            assertEquals(CausalDependencies.empty().advance(IN_ID, 0, 0), outClock(punctuated),
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

            in.pipeInput(new TestRecord<>("k", "v", clockHeader(CausalDependencies.empty())));

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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 5))));

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

            orders.pipeInput(new TestRecord<>("k", "order", clockHeader(CausalDependencies.empty())));
            prices.pipeInput(new TestRecord<>("k", "price", clockHeader(CausalDependencies.empty())));

            assertEquals(List.of("ORDER"), ordersOut.readValuesToList());
            assertEquals(List.of("PRICE"), pricesOut.readValuesToList());

            // Each decorator persisted only its own branch's frontier under its own namespace.
            assertEquals(CausalFrontier.empty().advance(ORDERS_ID, 0, 0), frontierIn(driver, "orders-frontier"));
            assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 0), frontierIn(driver, "prices-frontier"));
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

            in.pipeInput(new TestRecord<>("k", "a", clockHeader(CausalDependencies.empty())));
            in.pipeInput(new TestRecord<>("k", "b", clockHeader(CausalDependencies.empty())));

            assertEquals(
                    List.of(CausalFrontier.empty(),
                            CausalFrontier.empty().advance(IN_ID, 0, 0),
                            CausalFrontier.empty().advance(IN_ID, 0, 1)),
                    offsetFor,
                    "every frontier advance is published, in admission order");
        }
    }

    @Test
    void automaticStampFrontierIsBoundedByInputTopicCountNotRecordCount() {
        // The automatically stamped clock is the per-task frontier, which advances only by each
        // record's own source coordinate (one partition per source topic) — so its size is bounded
        // by the number of source topics in the subtopology, never by the number of records. This
        // pins the O(#input topics) envelope the docs promise for the Streams path.
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
                a.pipeInput(new TestRecord<>("k", "v" + i, clockHeader(CausalDependencies.empty())));
                b.pipeInput(new TestRecord<>("k", "v" + i, clockHeader(CausalDependencies.empty())));
                c.pipeInput(new TestRecord<>("k", "v" + i, clockHeader(CausalDependencies.empty())));
            }

            CausalFrontier frontier = frontierIn(driver, "parsley-frontier");
            assertEquals(3, frontier.positions().size(),
                    "the stamped frontier is bounded by the number of source topics, not record count");
        }
    }

    @Test
    void aLargeInboundClockIsNeverFoldedIntoTheStampedOutput() {
        // The most important regression guard: an inbound dependency clock is used only to gate the
        // record; it must never be merged into the frontier and re-stamped. A record carrying a clock
        // over hundreds of (unrelated) partitions, force-forwarded under forwardUnsafe, must still be
        // stamped with only its own source coordinate — proving the inbound clock cannot amplify the
        // stamped output and breach the header-size budget.
        CausalDependencies big = CausalDependencies.empty();
        for (int p = 0; p < 500; p++) {
            big = big.advance(CausalPosition.deriveUuid("ghost"), p, 1_000 + p);
        }

        Topology topology = topology(
                CausalProcessors.builder(upperCaser(), CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String()).onViolation(onViolation).build(),
                List.of("in"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            in.pipeInput(new TestRecord<>("k", "v", clockHeader(big)));

            CausalDependencies stamped = outClock(out.readRecord());
            assertEquals(CausalDependencies.empty().advance(IN_ID, 0, 0), stamped,
                    "a 500-partition inbound clock must not enlarge the stamped output clock");
            assertEquals(1, stamped.dependencies().size(), "the stamped clock carries only the source coordinate");
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
                    clockHeader(CausalDependencies.empty().advance(PRICES_ID, 0, 0))));
            // Release it (the price arrives and advances the frontier).
            prices.pipeInput(new TestRecord<>("k", "price", clockHeader(CausalDependencies.empty())));

            assertEquals(1.0, parsleyMetric(driver, "records-buffered-total"), 0.001,
                    "one record was added to the buffer");
            assertEquals(1.0, parsleyMetric(driver, "records-released-total"), 0.001,
                    "one record was released from the buffer");
            assertEquals(0.0, parsleyMetric(driver, "buffer-depth"), 0.001,
                    "buffer is empty after the drain");
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
