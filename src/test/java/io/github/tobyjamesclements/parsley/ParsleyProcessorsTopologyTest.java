package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.cause;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.message;

/**
 * Exercises {@link ParsleyProcessorSupplier} — the decorating causal processor — through a real Kafka Streams
 * topology using the {@link TopologyTestDriver} (no broker required).
 */
class ParsleyProcessorsTopologyTest {

    // Topic name → topic-id constant mapping used across tests.
    // c1 = default single-input topic; c2/c3 = two-source tests; c4 = materialized derived topic;
    // c5 = independent sidecar topic.
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid C3_ID = Uuid.randomUuid();
    private static final Uuid C4_ID = Uuid.randomUuid();
    private static final Uuid C5_ID = Uuid.randomUuid();
    private static final Uuid GHOST_ID = Uuid.randomUuid();

    // Fake admin resolving the test topics' UUIDs (no broker under TopologyTestDriver); injected into
    // every builder via the package-private topicAdmin(...) seam.
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of(
            "c1", C1_ID, "c2", C2_ID, "c3", C3_ID, "c4", C4_ID, "c5", C5_ID));

    // Resolver mapping the same test topic names to their UUIDs, for building CausalClock.
    private static final ParsleyTopics TOPICS = ParsleyTopics.of(Map.of(
            "c1", C1_ID, "c2", C2_ID, "c3", C3_ID, "c4", C4_ID, "c5", C5_ID, "ghost", GHOST_ID));

    private final List<String> processed = new ArrayList<>();

    /** A state directory per driver: the application id is fixed, so a shared one would collide. */
    @RegisterExtension
    static final TestStateDirectories STATE_DIRS = new TestStateDirectories("decorator-test-");

    // --- helpers -------------------------------------------------------------------------------

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "decorator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRS.create().toAbsolutePath().toString());
        return props;
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

    /** A user processor that consumes every record and forwards nothing (a pure filter/sink). */
    private ProcessorSupplier<String, String, String, String> dropper() {
        return () -> new Processor<>() {
            @Override
            public void process(Record<String, String> record) {
                processed.add(record.value());
            }
        };
    }

    private static Topology topology(
            ProcessorSupplier<String, String, String, String> decorated, List<String> inputTopics) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopics, Consumed.with(Serdes.String(), Serdes.String()))
                .process(decorated)
                .to("c6", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static CausalClock outDeps(TestRecord<String, String> record) {
        return CausalClock.fromHeaders(record.headers()).orElseThrow();
    }

    // --- tests ---------------------------------------------------------------------------------

    /**
     * A record with satisfied dependencies is admitted immediately: the delegate processor runs,
     * transforms the value, and the output record is stamped with the updated frontier.
     *
     * Asserts that the delegate runs exactly once, the value is transformed, and the emitted
     * record carries the frontier as the dependency stamp.
     */
    @Test
    void admittedRecordRunsDelegateAndStampsTheMergedClock() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalClock.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
            assertEquals(CausalClock.builder(TOPICS).require("c1", 0, 0).build(), outDeps(emitted),
                    "forward must be stamped with the frontier as of admission");
        }
    }

    /**
     * A record carrying no Parsley dependency header at all — a raw, never-stamped record from an
     * unaware producer — is treated as having empty (vacuously satisfied) dependencies: the delegate
     * runs immediately on the first poll, the record is forwarded, and the frontier is bumped through
     * its source coordinate so the emitted record is stamped with it. This exercises the absent-header
     * path end-to-end through the real decorator, where {@code ParsleyMessage.from} normalises the
     * missing header to empty.
     *
     * Asserts the header-less record runs the delegate at once and is forwarded stamped with the
     * advanced frontier.
     */
    @Test
    void recordWithNoDependencyHeaderIsForwardedImmediatelyAndBumpsTheFrontier() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // No headers at all — not even an empty-dependencies header.
            c1.pipeInput(new TestRecord<>("k", "raw"));

            assertEquals(List.of("raw"), processed,
                    "a record with no dependency header must be admitted immediately, not held");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("RAW", emitted.value(), "the admitted record must flow through the delegate");
            assertEquals(CausalClock.builder(TOPICS).require("c1", 0, 0).build(), outDeps(emitted),
                    "the absent header is treated as empty and the frontier is bumped through the source coordinate");
        }
    }

    /**
     * A record whose dependencies were derived at the edge with
     * {@link CausalClock#from(ParsleyTopics, org.apache.kafka.clients.consumer.ConsumerRecord)}
     * is gated on the triggering record's own position: it is held until the processor observes that
     * position, then delivered. This proves the edge API's own-coordinate semantic against the real
     * gate — a record produced after consuming {@code c2@0} must not be delivered before {@code c2@0}
     * is genuinely observed; its own declared claim is not proof enough on its own (every record is
     * checked against this node's actual current state, never against its own stamp).
     *
     * Asserts the c1 record stamped via {@code from(c2@0)} is buffered until a c2 record at offset 0
     * arrives, then drains.
     */
    @Test
    void recordStampedViaObserveIsHeldUntilItsTriggerCoordinateIsObserved() {
        // The dependencies a downstream producer would attach after consuming c2@0 (no carried deps).
        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> trigger =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("c2", 0, 0L, "tk", "tv");
        CausalClock stampedFromTrigger = CausalClock.using(TOPICS).observe(trigger);

        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1", "c2"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());

            // The c1 record depends on c2@0, which has not arrived: it must be held.
            c1.pipeInput(new TestRecord<>("k", "held", depsHeader(stampedFromTrigger)));
            assertEquals(List.of(), processed,
                    "a record stamped via from(c2@0) must be held until c2@0 is observed");

            // The triggering coordinate arrives (lands at c2 offset 0), draining the held record.
            c2.pipeInput(new TestRecord<>("tk", "trigger", depsHeader(CausalClock.empty())));
            assertEquals(List.of("trigger", "held"), processed,
                    "once c2@0 is observed, the record stamped via from(c2@0) must drain in causal order");
        }
    }

    /**
     * A record whose dependencies are not yet satisfied is buffered. When the satisfying record
     * arrives, the core releases the buffered record through the delegate in causal order.
     *
     * <p>The buffer store must hold the record while it is waiting and be empty after drain.
     *
     * Asserts that the buffered record is not delivered until the dependency arrives, and that
     * both records flow through the delegate in order after the dependency is satisfied.
     */
    @Test
    void heldRecordIsBufferedThenDrainedThroughDelegate() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSources(List.of("c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build(),
                List.of("c2", "c3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // c3-record depends on c2-0 offset 0, which hasn't arrived: held, not delivered.
            c3.pipeInput(new TestRecord<>("k", "c3-val",
                    depsHeader(CausalClock.builder(TOPICS).require("c2", 0, 0).build())));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertTrue(out.isEmpty(), "held record must not appear in the output topic");
            assertEquals(1, storeSize(bufferStore), "held record must be persisted to the buffer store");

            // c2-record arrives and advances the frontier, draining the c3-record through the delegate.
            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));

            assertEquals(List.of("c2-val", "c3-val"), processed,
                    "delegate must see both records in causal order after the dependency arrives");
            assertEquals(List.of("C2-VAL", "C3-VAL"), out.readValuesToList(),
                    "both transformed values must appear in the output in order");
            assertEquals(0, storeSize(bufferStore), "drained record must be removed from the buffer store");
        }
    }

    /**
     * {@code addSources(Collection<ParsleySource>)} registers every buffer in the collection, exactly
     * like calling {@code addSource} once per element — the convenience overload for buffers with
     * distinct (non-shared) serdes.
     *
     * Asserts that records on every topic registered via the collection overload are admitted and
     * forwarded through the delegate.
     */
    @Test
    void addBuffersCollectionOverloadRegistersEveryBuffer() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSources(List.of(
                                new ParsleySource<>("c2", Serdes.String(), Serdes.String()),
                                new ParsleySource<>("c3", Serdes.String(), Serdes.String())))
                        .topicAdmin(ADMIN).build(),
                List.of("c2", "c3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));
            c3.pipeInput(new TestRecord<>("k", "c3-val", depsHeader(CausalClock.empty())));

            assertEquals(List.of("C2-VAL", "C3-VAL"), out.readValuesToList(),
                    "both buffers registered via the collection overload must admit and forward their records");
        }
    }

    /**
     * Closing the driver (which closes every processor node) invokes the delegate processor's own
     * {@code close()} and removes Parsley's registered Streams metrics sensors — not just that the
     * topology runs without throwing while open.
     */
    @Test
    void closeInvokesTheDelegateAndRemovesTheRegisteredSensors() {
        List<String> delegateCloseCalls = new ArrayList<>();
        ProcessorSupplier<String, String, String, String> recordingDelegate = () -> new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) {}
            @Override public void close() { delegateCloseCalls.add("closed"); }
        };
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(recordingDelegate).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        TopologyTestDriver driver = new TopologyTestDriver(topology, config());
        // Sanity: a Parsley sensor is registered while the driver is open.
        assertEquals(0.0, parsleyMetric(driver, "buffer-depth"), 0.001,
                "a parsley sensor must be registered while the driver is open");

        driver.close();

        assertEquals(List.of("closed"), delegateCloseCalls,
                "closing the driver must invoke the delegate's own close()");
        assertThrows(AssertionError.class, () -> parsleyMetric(driver, "buffer-depth"),
                "closing the driver must remove Parsley's registered sensors");
    }

    /**
     * While delivering a record, {@code context.recordMetadata()} reports that record's own true
     * source coordinate. This is what lets a delegate correctly attribute state to the record it is
     * actually handling — verified here across two independently-delivered records on different
     * source topics, each of which must report its own coordinate, never the other's.
     *
     * <p>The two records each deliver on their own rather than one deferring on the other, because a
     * record declaring a dependency on another cannot be held at this level: a record's own declared
     * dependency is folded into its own channel before its own gate check runs, so under
     * single-witness merge it always proves itself immediately. There is no "held, then
     * cascade-released" case reachable from a normal record's own dependency here, and the
     * metadata-correctness property does not need one.
     */
    @Test
    void recordMetadataDuringDeliveryReportsEachRecordsOwnSource() {
        List<String> reportedSources = new ArrayList<>();
        ProcessorSupplier<String, String, String, String> recordingDelegate = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                RecordMetadata meta = ctx.recordMetadata().orElseThrow();
                reportedSources.add(meta.topic() + "-" + meta.partition() + "@" + meta.offset());
            }
        };
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(recordingDelegate).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1", "c2"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());

            // Two independent records, each with no dependencies — each delivers on its own.
            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));
            c1.pipeInput(new TestRecord<>("k", "c1-val", depsHeader(CausalClock.empty())));

            assertEquals(List.of("c2-0@0", "c1-0@0"), reportedSources,
                    "each record's delivery must report its own source coordinate");
        }
    }

    /**
     * When the user processor forwards a record that already carries a stale
     * causal-dependencies header, the Parsley wrapper replaces it with the current frontier
     * stamp (idempotent stamping). User headers unrelated to Parsley are preserved.
     *
     * Asserts that the output carries exactly one dependencies header with the frontier value,
     * and that the user's own headers are intact.
     */
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
                Headers headers = ParsleyHeader.mutableHeaders();
                headers.add("user-h", "keep".getBytes());
                // A stale dependencies header the user happens to carry — stamping must replace, not duplicate it.
                headers.add("parsley-causal-clock",
                        CausalClock.builder(TOPICS).require("c2", 0, 5).build().toBytes());
                ctx.forward(record.withHeaders(headers));
            }
        };
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(user).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalClock.empty())));

            TestRecord<String, String> emitted = out.readRecord();
            assertEquals(1, count(emitted.headers(), "parsley-causal-clock"),
                    "exactly one dependencies header — stamping must replace, not duplicate");
            assertEquals(CausalClock.builder(TOPICS).require("c1", 0, 0).build(), outDeps(emitted),
                    "the stamped dependencies must be the frontier, not the user's stale value");
            assertEquals("keep", new String(emitted.headers().lastHeader("user-h").value()),
                    "user headers must be preserved");
        }
    }

    /**
     * Records forwarded by a user-registered punctuator are stamped with the frontier that
     * is live at the time the punctuator fires, not with a stale or empty frontier.
     *
     * Asserts that the punctuator's output carries the frontier that was current after the
     * last admitted record.
     */
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
                ParsleyProcessorSupplier.builder(user).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalClock.empty())));
            out.readRecord(); // the live record
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            TestRecord<String, String> punctuated = out.readRecord();
            assertEquals("punct", punctuated.value(), "punctuator output must reach the topic");
            assertEquals(CausalClock.builder(TOPICS).require("c1", 0, 0).build(), outDeps(punctuated),
                    "punctuator forwards must be stamped with the live frontier");
        }
    }

    /**
     * The delegate processor receives a working {@link ProcessorContext} that exposes both
     * its registered state stores and the originating record's metadata (topic, partition,
     * offset) via {@code recordMetadata()}.
     *
     * Asserts that a state-store write is visible after processing, and that
     * {@code recordMetadata().topic()} returns the source topic name.
     */
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
                ParsleyProcessorSupplier.builder(user).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalClock.empty())));

            assertEquals("v@c1", out.readValue(),
                    "getStateStore and recordMetadata must pass through the proxy context");
            assertEquals("v", driver.<String, String>getKeyValueStore("u-state").get("k"),
                    "the user's state-store write must be visible via the driver");
        }
    }

    // The buffer value serde resolves using the record's source topic rather than the changelog
    // name, which schema-registry Avro subjects depend on. That property is not tested at this
    // level, because genuine buffering cannot be forced here: a record's own declared dependency is
    // folded into its own channel before its own gate check runs, so under single-witness merge it
    // always proves itself immediately. The direct, lower-level test is
    // StoreBackedBufferStoreTest.addResolvesTheValueSerdeUsingTheRecordsSourceTopic.

    /**
     * Two {@link ParsleyProcessorSupplier} instances in the same topology, each with a distinct
     * {@code storeName}, coexist without state-store conflicts and maintain independent
     * frontiers.
     *
     * <p>Without distinct store names, Kafka Streams would reject the topology with a
     * duplicate-store error.
     *
     * Asserts that both processors independently forward their records, and that each
     * maintains its own frontier state under its own namespace.
     */
    @Test
    void twoDecoratorsWithDistinctStoreNamesCoexistAndKeepIsolatedFrontiers() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("c3", Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("c3")
                        .addSource(new ParsleySource<>("c3", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("c3-out", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("c2", Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("c2")
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("c2-out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> c3Out =
                    driver.createOutputTopic("c3-out", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> c2Out =
                    driver.createOutputTopic("c2-out", new StringDeserializer(), new StringDeserializer());

            c3.pipeInput(new TestRecord<>("k", "c3-val", depsHeader(CausalClock.empty())));
            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));

            assertEquals(List.of("C3-VAL"), c3Out.readValuesToList(),
                    "c3 processor must forward to its own output topic");
            assertEquals(List.of("C2-VAL"), c2Out.readValuesToList(),
                    "c2 processor must forward to its own output topic");

            // Each decorator persisted only its own branch's frontier under its own namespace.
            assertEquals(ParsleyVectorClock.empty().observe(C3_ID, 0, 0),
                    frontierIn(driver, "c3-frontier"),
                    "c3 frontier must reflect only c3's source coordinate");
            assertEquals(ParsleyVectorClock.empty().observe(C2_ID, 0, 0),
                    frontierIn(driver, "c2-frontier"),
                    "c2 frontier must reflect only c2's source coordinate");
        }
    }

    /**
     * The automatically stamped dependency clock on output records reflects only the
     * per-task frontier, whose width is bounded by the number of source topics in the
     * subtopology — not by the number of records processed.
     *
     * <p>This pins the O(#input topics) header-size guarantee documented for the Streams path.
     *
     * Asserts that after processing 150 records across 3 source topics, the frontier contains
     * exactly 3 entries.
     */
    @Test
    void automaticStampFrontierIsBoundedByInputTopicCountNotRecordCount() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSources(List.of("c1", "c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build(),
                List.of("c1", "c2", "c3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());

            for (int i = 0; i < 50; i++) {
                c1.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalClock.empty())));
                c2.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalClock.empty())));
                c3.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalClock.empty())));
            }

            ParsleyVectorClock frontier = frontierIn(driver, "parsley-frontier");
            assertEquals(3, frontier.size(),
                    "the stamped frontier width must equal the number of source topics, not the record count");
        }
    }

    /**
     * The gate's ignore branch at the processor level, at width: a dependency clock spanning
     * many partitions of an entirely unconsumed topic ({@code ghost}) is ignored coordinate by
     * coordinate — with transitively complete stamps carried by unconditional merges,
     * any consumed causal ancestor is claimed directly in the same clock, so the unconsumed
     * entries only proxy ancestry the clock already states.
     *
     * Asserts the record delivers to the delegate immediately and every ignored coordinate is
     * counted by the {@code deps-out-of-scope-ignored} sensor.
     */
    @Test
    void manyDependenciesOnAnUnconsumedTopicAreIgnoredAndDeliver() {
        CausalClock.Builder bigBuilder = CausalClock.builder(TOPICS);
        for (int p = 0; p < 500; p++) {
            bigBuilder.require("ghost", p, 1_000 + p);
        }
        CausalClock big = bigBuilder.build();

        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            c1.pipeInput(new TestRecord<>("k", "v", depsHeader(big)));

            assertEquals(List.of("v"), processed,
                    "a record whose only dependencies are unconsumed coordinates must deliver "
                            + "immediately — the ignore branch, not a failure");
            assertEquals(500.0, parsleyMetric(driver, "deps-out-of-scope-ignored-total"), 0.001,
                    "every ignored coordinate must count the out-of-scope-ignored sensor");
        }
    }

    /**
     * The two-branch dispatch is per coordinate: a clock naming an unconsumed topic
     * ({@code ghost}) and a partition of a consumed topic this task does not own ({@code c1}
     * partition 7) sends both to the ignore branch — producers stamp a clock spanning everything
     * they consume, so a downstream processor routinely sees coordinates it can never observe
     * directly, and ignoring them is sound because the same clock claims every consumed ancestor
     * directly, by transitive completeness and unconditional merge.
     *
     * Asserts the record delivers to the delegate and both ignored coordinates count the
     * {@code deps-out-of-scope-ignored} sensor.
     */
    @Test
    void dependenciesOnUnconsumedCoordinatesAreIgnoredAndDeliver() {
        CausalClock deps = CausalClock.builder(TOPICS)
                .require("ghost", 0, 5)     // un-consumed topic
                .require("c1", 7, 9)        // consumed topic, but a partition this task does not own
                .build();

        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            c1.pipeInput(new TestRecord<>("k", "hello", depsHeader(deps)));

            assertEquals(List.of("hello"), processed,
                    "unconsumed-coordinate dependencies must be ignored and the record delivered");
            assertEquals(2.0, parsleyMetric(driver, "deps-out-of-scope-ignored-total"), 0.001,
                    "both the unconsumed topic and the unowned partition must count the sensor");
        }
    }

    /**
     * The Kafka Streams metrics sensors registered by Parsley are populated after buffering
     * and drain events. The {@code records-buffered-total}, {@code records-released-total},
     * and {@code buffer-depth} sensors reflect the correct values after one buffer/drain cycle.
     *
     * Asserts that each sensor holds the expected value after the cycle.
     */
    @Test
    void streamsMetricsSensorsArePopulatedAfterBufferingAndRelease() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSources(List.of("c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build(),
                List.of("c2", "c3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());

            // Buffer one c3-record (depends on c2-0 offset 0, not yet arrived).
            c3.pipeInput(new TestRecord<>("k", "c3-val",
                    depsHeader(CausalClock.builder(TOPICS).require("c2", 0, 0).build())));
            // Release it (the c2-record arrives and advances the frontier).
            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));

            assertEquals(1.0, parsleyMetric(driver, "records-buffered-total"), 0.001,
                    "one record was added to the buffer");
            assertEquals(1.0, parsleyMetric(driver, "records-released-total"), 0.001,
                    "one record was released from the buffer");
            assertEquals(0.0, parsleyMetric(driver, "buffer-depth"), 0.001,
                    "buffer is empty after the drain");
        }
    }

    /**
     * A record whose dependencies contain its own source coordinate is forwarded immediately:
     * the self-referential entry is stripped at admission, leaving empty effective dependencies.
     *
     * <p>The buffer size limit is set to 1 so any attempt to buffer the record would evict it
     * immediately — proving the record never entered the buffer path.
     *
     * Asserts that the record is forwarded without entering the buffer.
     */
    @Test
    void stripSelfReferentialDependencyAndForwardImmediately() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // Dependencies require C1_ID/0@0 — exactly the record's own source coordinate.
            c1.pipeInput(new TestRecord<>("k", "hello",
                    depsHeader(CausalClock.builder(TOPICS).require("c1", 0, 0).build())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "self-dep stripped → effective dependencies empty → forwarded immediately");
            assertEquals(0, storeSize(bufferStore),
                    "record must never enter the buffer even with size limit 1");
        }
    }

    /**
     * In a fused two-processor chain (no intermediate Kafka topic), p1's frontier stamp
     * includes the current record's own source coordinate. Without self-reference stripping,
     * p2 would hold every forwarded record indefinitely (circular dependency).
     *
     * <p>With stripping, p2 recognises the self-reference and immediately admits the record.
     *
     * <p>Topology:
     * <pre>
     *   "c1" ──→ p1("p1") ──→ (fused) ──→ p2("p2") ──→ "c6"
     * </pre>
     *
     * Asserts that the record flows through both processors and p2's buffer is empty at the
     * end.
     */
    @Test
    void p1StampDoesNotCircularlyBlockDownstreamInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        // Fused chain: p1 → p2, no .to("intermediate") between them.
        // p1 admits the record, stamps its frontier (which now contains the source coord), and
        // forwards. p2 receives the stamped record; because the topology is fused,
        // context.recordMetadata() still returns the original "c1" metadata → self-reference dep.
        builder.stream("c1", consumed)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p1")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p2")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("c6", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalClock.empty())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "record must flow through both processors and reach the output");
            assertEquals(0, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "p2 buffer must be empty — record was never held");
        }
    }

    /**
     * Verifies that two {@link ParsleyProcessorSupplier} instances chained via a materialized Kafka
     * topic each enforce causal ordering independently and produce a consistent frontier at both
     * layers.
     *
     * <h2>Topology</h2>
     * <pre>
     *   [c2, c3] ──→ p1("p1") ──→ "c4" (materialized)
     *                                          ↓
     *   [c2, c3] ──────────────────→ p2("p2") ──→ "c6"
     * </pre>
     * p2 subscribes to "c4" (p1's derived output) AND to "c2"/"c3" directly.
     *
     * <h2>Why materialization is required</h2>
     * Without {@code .to("c4")} the chain is fused: {@code recordMetadata().topic()} in p2
     * returns the original Kafka source topic, so every forwarded record's dep includes its own
     * source coordinate — stripped at core admission. With materialization, c4 records have
     * source {@code C4_ID} and deps {@code {C2_ID@x, C3_ID@y}}: no self-reference at all, and
     * the causal ordering across two layers is preserved.
     *
     * <h2>Why distinct {@code storeName} values are required</h2>
     * Each {@link ParsleyProcessorSupplier} registers three KeyValueStores. Without distinct names
     * Kafka Streams rejects the topology with a duplicate-store error.
     *
     * <h2>Neither layer ever holds anything</h2>
     * A record's own declared dependency (e.g. c3-val's claim of {@code c2@0}) is folded into its
     * own channel before its own gate check runs, at both p1 and p2 independently — under
     * single-witness merge this always proves itself immediately, without needing either processor
     * to separately observe {@code c2@0} through some other record. Direct "c2"/"c3" subscriptions
     * still matter for p2's own frontier to genuinely span both source coordinates by the end,
     * not for gating anything.
     */
    @Test
    void materializedChainEnablesFullDrainAtBothProcessorLayers() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String>  consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String>  produced = Produced.with(Serdes.String(), Serdes.String());

        var c2Src = builder.stream("c2", consumed);
        var c3Src = builder.stream("c3", consumed);
        var c4Src = builder.stream("c4",  consumed);

        // p1: holds c3-records (dep on c2) until c2 arrives. Output materializes to "c4".
        c2Src.merge(c3Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p1")
                        .addSources(List.of("c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("c4", produced);

        // p2: receives "c4" (p1's derived output) AND direct c2/c3 feeds to bootstrap its frontier.
        c4Src.merge(c2Src).merge(c3Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p2")
                        .addSources(List.of("c4", "c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("c6", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // Phase 1: c3-record arrives before c2. Its own declared dependency on c2@0 is folded
            // into its own channel before its own gate check runs at both p1 and p2 (each has
            // its own independent frontier/channels), so it delivers immediately at both, rather than
            // being held — a single genuine witness (even a record's own claim) suffices under
            // single-witness merge.
            c3.pipeInput(new TestRecord<>("k", "c3-val",
                    depsHeader(CausalClock.builder(TOPICS).require("c2", 0, 0).build())));

            // Phase 2: c2-record arrives, delivering at both layers too.
            c2.pipeInput(new TestRecord<>("k", "c2-val",
                    depsHeader(CausalClock.empty())));

            // Both layers empty; nothing was ever buffered.
            assertEquals(0, storeSize(driver.getKeyValueStore("p1-buffer")),
                    "p1 buffer must be empty after drain");
            assertEquals(0, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "p2 buffer must be empty after drain");

            // "c6" has 4 records: direct c2-val, direct c3-val (p2's own admissions) plus
            // the p1-forwarded c2-val and c3-val (arriving via "c4").
            List<String> outValues = out.readValuesToList();
            assertEquals(4, outValues.size(), "four records must reach the output");
            assertEquals(2, outValues.stream().filter("C2-VAL"::equals).count(),
                    "two C2-VAL records: direct and via c4");
            assertEquals(2, outValues.stream().filter("C3-VAL"::equals).count(),
                    "two C3-VAL records: direct and via c4");

            // p1's frontier spans both source topics after draining.
            assertEquals(
                    ParsleyVectorClock.empty()
                            .observe(C2_ID, 0, 0)
                            .observe(C3_ID, 0, 0),
                    frontierIn(driver, "p1-frontier"),
                    "p1 frontier must span both c2 and c3 after drain");

            // p2's frontier spans c2 and c3 (from direct admissions) plus c4@1 (last c4-record drained).
            ParsleyVectorClock f2 = frontierIn(driver, "p2-frontier");
            assertEquals(0L, f2.offsetFor(C2_ID, 0), "p2 must have seen c2 directly");
            assertEquals(0L, f2.offsetFor(C3_ID, 0), "p2 must have seen c3 directly");
            assertEquals(1L, f2.offsetFor(C4_ID, 0), "p2 must have drained both c4-records");
        }
    }

    /**
     * Backlog #15 ("fused processor chain footgun") regression: the same multi-topic fan-in shape
     * as {@link #materializedChainEnablesFullDrainAtBothProcessorLayers()}, but WITHOUT the
     * {@code .to("c4")} materialization step — p1's output feeds p2 directly (fused), in
     * addition to p2's direct c2/c3 subscriptions.
     *
     * <h2>Why this could deadlock</h2>
     * In a fused chain, {@code context.recordMetadata()} in p2 reports the ORIGINAL source
     * topic/offset (not a synthetic intermediate one), so a record relayed from p1 can carry a
     * dependency that is NOT its own coordinate (e.g. a c2-sourced record carrying a c3 dependency
     * p1 had already observed). #15 claimed this circularly blocks p2 forever. Under
     * single-witness merge this concern dissolves even more directly than the original fix
     * intended: any record's own declared dependency is folded into its own channel before its own
     * gate check runs, so it always proves itself immediately regardless of what coordinate it
     * names — there is no scenario left in which a relayed record's "other dimension" dependency
     * could ever hold it up.
     *
     * <h2>Topology</h2>
     * <pre>
     *   [c2, c3] ──→ p1("p1") ──┐ (fused, no .to())
     *                                  ↓
     *   [c2, c3] ─────────────────→ p2("p2") ──→ "c6"
     * </pre>
     */
    @Test
    void fusedChainWithoutMaterializationStillFullyDrainsBothLayers() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var c2Src = builder.stream("c2", consumed);
        var c3Src = builder.stream("c3", consumed);

        // p1: holds c3-records (dep on c2) until c2 arrives. No .to() — output stays in-process.
        var viaProc1 = c2Src.merge(c3Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p1")
                        .addSources(List.of("c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build());

        // p2: receives p1's output directly (FUSED) AND direct c2/c3 feeds to bootstrap its frontier.
        viaProc1.merge(c2Src).merge(c3Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p2")
                        .addSources(List.of("c2", "c3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("c6", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c3 =
                    driver.createInputTopic("c3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // Phase 1: c3-record arrives before c2. Its own declared dependency on c2@0 is folded
            // into its own channel before its own gate check runs, at both p1 and p2
            // independently, so it delivers immediately at both.
            c3.pipeInput(new TestRecord<>("k", "c3-val",
                    depsHeader(CausalClock.builder(TOPICS).require("c2", 0, 0).build())));

            // Phase 2: c2-record arrives, delivering at both layers too.
            c2.pipeInput(new TestRecord<>("k", "c2-val", depsHeader(CausalClock.empty())));

            // Neither layer ever buffered anything (the deadlock #15 describes would manifest as a
            // non-zero p2-buffer size here).
            assertEquals(0, storeSize(driver.getKeyValueStore("p1-buffer")),
                    "p1 buffer must be empty after drain");
            assertEquals(0, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "p2 buffer must be empty after drain — no fused-chain deadlock");

            // "c6" has 4 records: direct c2-val, direct c3-val (p2's own admissions) plus the
            // p1-relayed c2-val and c3-val (arriving via the fused edge, not a topic).
            List<String> outValues = out.readValuesToList();
            assertEquals(4, outValues.size(), "four records must reach the output");
            assertEquals(2, outValues.stream().filter("C2-VAL"::equals).count(),
                    "two C2-VAL records: direct and via the fused p1 edge");
            assertEquals(2, outValues.stream().filter("C3-VAL"::equals).count(),
                    "two C3-VAL records: direct and via the fused p1 edge");

            // Both processors' frontiers span both source topics after draining.
            assertEquals(
                    ParsleyVectorClock.empty().observe(C2_ID, 0, 0).observe(C3_ID, 0, 0),
                    frontierIn(driver, "p1-frontier"),
                    "p1 frontier must span both c2 and c3 after drain");
            assertEquals(
                    ParsleyVectorClock.empty().observe(C2_ID, 0, 0).observe(C3_ID, 0, 0),
                    frontierIn(driver, "p2-frontier"),
                    "p2 frontier must span both c2 and c3 after drain");
        }
    }

    /**
     * Fused chain: a clocked sidecar record ("c5") whose dependency points to the upstream
     * source ("c1") is buffered in p2 until p1 processes a "c1" record and fuses to p2.
     *
     * <h2>Topology</h2>
     * <pre>
     *   "c1"  ──→ p1("p1") ──→ (fused) ──→ p2("p2") ──→ "c6"
     *   "c5" ──────────────────────────────────→ p2("p2")
     * </pre>
     *
     * <p>This proves that self-dep stripping does not prevent p2's C1_ID frontier from
     * advancing: p2 correctly recognises that C1_ID@0 is now satisfied and releases the sidecar.
     *
     * Asserts that the sidecar is held while C1_ID@0 is absent from p2's frontier, and
     * released immediately after p1 processes the "c1" record.
     */
    @Test
    void bufferClockedSidecarUntilFusedOutputAdvancesFrontierInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var c5Src = builder.stream("c5", consumed);

        // p1 fused directly into p2 (no .to("intermediate")); c5 also merges into p2.
        builder.stream("c1", consumed)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p1")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .merge(c5Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p2")
                        .addSources(List.of("c1", "c5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("c6", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c5 =
                    driver.createInputTopic("c5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // c5-record depends on c1@0 which hasn't been processed yet — must be buffered.
            c5.pipeInput(new TestRecord<>("k", "c5-val",
                    depsHeader(CausalClock.builder(TOPICS).require("c1", 0, 0).build())));
            assertTrue(out.isEmpty(), "sidecar must be held: C1_ID@0 not yet in p2's frontier");
            assertEquals(1, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "sidecar must be in p2's buffer");

            // "c1"@0 arrives: p1 admits it, stamps frontier {C1_ID@0}, fuses to p2.
            // p2's core strips the self-dep → admits → frontier has C1_ID@0 → sidecar drains.
            c1.pipeInput(new TestRecord<>("k", "c1-val", depsHeader(CausalClock.empty())));

            assertEquals(0, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "sidecar must have drained once C1_ID@0 was in p2's frontier");
            assertEquals(List.of("C1-VAL", "C5-VAL"), out.readValuesToList(),
                    "c1-val admitted first (self-dep stripped), c5-val drains immediately after");
        }
    }

    /**
     * Fused chain: an unclocked sidecar record (no causal-dependencies header) is treated as
     * trivially satisfied and admitted immediately, interleaved correctly alongside the fused
     * p1 output.
     *
     * Asserts that the unclocked record is forwarded immediately and the buffer remains empty.
     * Subsequent clocked records flow normally.
     */
    @Test
    void admitUnclockedSidecarImmediatelyInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var c5Src = builder.stream("c5", consumed);

        builder.stream("c1", consumed)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p1")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .merge(c5Src)
                .process(ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("p2")
                        .addSources(List.of("c1", "c5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("c6", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c5 =
                    driver.createInputTopic("c5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // Unclocked c5-record forwarded immediately, trivially satisfied.
            c5.pipeInput(new TestRecord<>("k", "c5-val"));
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("C5-VAL", emitted.value(), "unclocked sidecar must be forwarded immediately");
            assertEquals(0, storeSize(driver.getKeyValueStore("p2-buffer")),
                    "unclocked record must never enter the buffer");

            // Fused p1 output flows through p2 normally alongside the earlier unclocked record.
            c1.pipeInput(new TestRecord<>("k", "c1-val", depsHeader(CausalClock.empty())));
            assertEquals(List.of("C1-VAL"), out.readValuesToList(),
                    "clocked c1-val must flow through both processors normally");
        }
    }

    /**
     * A record arrives on a topic for which no {@link ParsleySource} was registered (e.g. an input
     * topic added to the stream but never wired up via {@code addSource}). The processor's intake
     * guard rejects it rather than silently treating it as dependency-free.
     *
     * Asserts that processing the record throws (wrapped by Kafka Streams in a
     * {@code StreamsException}) with a cause naming the unregistered topic.
     */
    @Test
    void ingestThrowsForATopicWithNoRegisteredBuffer() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1", "ghost"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> ghost =
                    driver.createInputTopic("ghost", new StringSerializer(), new StringSerializer());

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> ghost.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalClock.empty()))),
                    "a record on an unregistered topic must not be silently admitted");
            assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                    "the wrapped cause must be the intake guard's exception");
            assertTrue(message(cause(thrown)).contains("no ParsleySource registered for topic 'ghost'"),
                    "the cause must name the unregistered topic: " + message(cause(thrown)));
        }
    }

    /**
     * A null message whose {@code parsley-causal-clock} header is present but undecodable fails the
     * task exactly like a business record's ({@link CausalVectorClockResolutionException}): the
     * carried clock is the emitting node's stamp, so folding nothing while delivering the offset
     * would permanently drop the peer's progress claims from this node's channel fold — a later
     * stamp here would under-claim them. The transaction aborts, so the offset is not committed and
     * the message is refetched on restart.
     *
     * Asserts piping the corrupt null message throws (wrapped in a {@code StreamsException}) with a
     * {@link CausalVectorClockResolutionException} cause naming the coordinate.
     */
    @Test
    @SuppressWarnings("NullAway") // the null message TestRecord intentionally has null key/value
    void corruptNullMessageClockHeaderFailsTheTask() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            Headers corrupt = ParsleyHeader.mutableHeaders();
            corrupt.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
            corrupt.add(ParsleyHeader.CAUSAL_CLOCK, new byte[] {(byte) 0xFF});

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> c1.pipeInput(new TestRecord<>(null, null, corrupt)),
                    "an undecodable null-message carried clock must fail the task, never fold as empty");
            assertEquals(CausalVectorClockResolutionException.class, cause(thrown).getClass(),
                    "the wrapped cause must be the clock-resolution guard's exception, mirroring the "
                            + "business path");
            assertTrue(message(cause(thrown)).contains("c1-0@0"),
                    "the failure must name the null message's coordinate: " + message(cause(thrown)));
        }
    }

    /**
     * A null message with an <em>absent</em> clock header stays deliverable: an empty carried clock
     * teaches the channel nothing, but the message's own offset is still delivered into the
     * frontier — a producer that stamps nothing claims nothing, matching the business-path
     * semantics for an absent header. Pins the absent/undecodable distinction the fail-fast rule
     * turns on.
     *
     * Asserts a business record depending on the clockless null message's offset is subsequently
     * delivered — the offset entered the frontier.
     */
    @Test
    @SuppressWarnings("NullAway") // the null message TestRecord intentionally has null key/value
    void absentNullMessageClockHeaderStillDeliversTheOffset() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1", "c2"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> c2 =
                    driver.createInputTopic("c2", new StringSerializer(), new StringSerializer());

            Headers clockless = ParsleyHeader.mutableHeaders();
            clockless.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
            c1.pipeInput(new TestRecord<>(null, null, clockless));

            c2.pipeInput(new TestRecord<>("k", "dependent",
                    depsHeader(CausalClock.builder(TOPICS).require("c1", 0, 0).build())));

            assertEquals(List.of("dependent"), processed,
                    "the clockless null message's own offset must have entered the frontier, so a "
                            + "record depending on c1@0 is deliverable");
        }
    }

    /**
     * A null message arriving on a topic with no registered {@link ParsleySource} fails the task
     * with the same intake-guard {@code IllegalStateException} as a business record
     * ({@link #ingestThrowsForATopicWithNoRegisteredBuffer}): skipping it would commit the offset
     * past a record on a channel this node claims not to know.
     *
     * Asserts piping the null message on the unregistered topic throws with a cause naming it.
     */
    @Test
    @SuppressWarnings("NullAway") // the null message TestRecord intentionally has null key/value
    void nullMessageOnAnUnregisteredTopicFailsTheTask() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1", "ghost"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> ghost =
                    driver.createInputTopic("ghost", new StringSerializer(), new StringSerializer());

            Headers marker = ParsleyHeader.mutableHeaders();
            marker.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> ghost.pipeInput(new TestRecord<>(null, null, marker)),
                    "a null message on an unregistered topic must fail the task, not be skipped past");
            assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                    "the wrapped cause must be the intake guard's exception, mirroring the business path");
            assertTrue(message(cause(thrown)).contains("no ParsleySource registered for topic 'ghost'"),
                    "the cause must name the unregistered topic: " + message(cause(thrown)));
        }
    }

    /**
     * The broker (via {@link ParsleyTopicAdmin}) fails to return a UUID for one of the registered
     * causal-buffer topics at startup — a defensive guard against an admin implementation that
     * returns successfully but with an incomplete result.
     *
     * Asserts that constructing the driver throws (wrapped in a {@code StreamsException}) with a
     * cause naming the topic the broker did not resolve.
     */
    @Test
    void resolveTopicUuidsThrowsWhenTheAdminOmitsARegisteredTopic() {
        ParsleyTopicAdmin incomplete = new ParsleyTopicAdmin() {
            @Override public Map<String, Uuid> topicIds(List<String> topics) { return Map.of(); }
            @Override public Map<String, Integer> partitionCounts(List<String> topics) { return Map.of(); }
            @Override public Map<String, String> cleanupPolicies(List<String> topics) { return Map.of(); }
            @Override public Map<Integer, Long> endOffsets(String topic) { return Map.of(); }
            @Override public void close() {}
        };
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(incomplete).build(),
                List.of("c1"));

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config()),
                "startup must fail when the admin does not resolve every registered topic");
        Throwable guardFailure = cause(thrown);
        assertEquals(IllegalStateException.class, guardFailure.getClass(),
                "the cause must be the UUID-resolution guard's exception directly, not re-wrapped");
        assertTrue(message(guardFailure).contains("broker did not return a UUID for topic 'c1'"),
                "the cause must name the unresolved topic: " + message(guardFailure));
    }

    /**
     * The {@link ParsleyTopicAdmin} itself fails (e.g. the broker is unreachable) while resolving
     * causal-buffer topic UUIDs at startup. The failure is wrapped with the topics that were being
     * resolved, rather than the raw admin exception escaping.
     *
     * Asserts that constructing the driver throws (wrapped in a {@code StreamsException}) with a
     * cause chain of: the wrapping {@code IllegalStateException}, then the original admin failure.
     */
    @Test
    void resolveTopicUuidsWrapsAnAdminFailure() {
        ParsleyTopicAdmin throwing = new ParsleyTopicAdmin() {
            @Override public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                throw new TimeoutException("no broker reachable");
            }
            @Override public Map<String, Integer> partitionCounts(List<String> topics) { return Map.of(); }
            @Override public Map<String, String> cleanupPolicies(List<String> topics) { return Map.of(); }
            @Override public Map<Integer, Long> endOffsets(String topic) { return Map.of(); }
            @Override public void close() {}
        };
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(throwing).build(),
                List.of("c1"));

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config()),
                "startup must fail when the admin itself throws");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the resolveTopicUuids catch-and-rethrow");
        assertTrue(message(cause(thrown)).contains("failed to resolve topic metadata for causal buffers"),
                "the wrapping message must name the failed resolution: " + message(cause(thrown)));
        assertEquals(TimeoutException.class, cause(cause(thrown)).getClass(),
                "the original admin failure must be preserved as the innermost cause");
    }

    /**
     * A non-emitting delegate produces one protocol null message per delivered input, and that null message
     * reuses the triggering record's key so it routes, through whatever partitioner the business
     * records use, to the same partition the record's output would have landed on. With a null key
     * a sink partitioner would send it to an arbitrary partition and a downstream task could starve.
     *
     * Asserts the emitted null message carries the triggering key ("alpha"), a null value, and the
     * null message marker header.
     */
    @Test
    void nullMessageReusesTriggeringRecordKeySoItCoRoutesWithThatKeysRecords() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(dropper()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("alpha", "dropped", depsHeader(CausalClock.empty())));

            List<TestRecord<String, String>> emitted = out.readRecordsToList();
            assertEquals(1, emitted.size(), "a non-emitting delegate must emit exactly one null message");
            TestRecord<String, String> nullMessage = emitted.get(0);
            assertTrue(nullMessage.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) != null,
                    "the emitted record must carry the _parsley_null_message marker header");
            assertEquals("alpha", nullMessage.key(),
                    "the null message must reuse the triggering record's key so it co-routes to that partition");
            assertEquals(null, nullMessage.value(), "a null message carries no business value");
        }
    }

    /**
     * A null message emitted for a delivered-but-unforwarded record (the non-emitting path)
     * carries the triggering record's timestamp, never the wall clock: Kafka Streams advances
     * downstream stream time from every polled record's timestamp, so a wall-clock stamp during a
     * reprocessing run would expire downstream windows and suppressions.
     *
     * Asserts the emitted null message's timestamp equals the dropped record's.
     */
    @Test
    void nullMessageCarriesTheTriggersTimestampOnTheNonEmittingPath() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(dropper()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "dropped", depsHeader(CausalClock.empty()), 1234L));

            List<TestRecord<String, String>> emitted = out.readRecordsToList();
            assertEquals(1, emitted.size(), "a non-emitting delegate must emit exactly one null message");
            assertEquals(1234L, emitted.get(0).timestamp(),
                    "the null message must carry the trigger's timestamp, not the wall clock");
        }
    }

    /**
     * A heartbeat null message — emitted for a record that was buffered (nothing delivered) but
     * whose receipt still advanced a consumed channel — carries the buffered record's own timestamp,
     * the same trigger-timestamp rule as the non-emitting path. The heartbeat fires only when the
     * receipt advanced a consumed channel, i.e. a channel's first sighting at a nonzero offset (whose
     * baseline seed lifts the frontier below it); a {@code TopologyTestDriver} cannot skip
     * offsets, so this drives the processor directly through a {@link MockProcessorContext}.
     *
     * Asserts exactly one heartbeat is forwarded and its timestamp equals the held record's.
     */
    @Test
    void heartbeatNullMessageCarriesTheHeldRecordsTimestamp() {
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                upperCaser().get(),
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String())),
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("c1", "c2"), Set.of(), List.of(),
                configs -> ADMIN, null);
        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier"));
        context.addStateStore(new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer"));
        context.addStateStore(new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index"));
        context.addStateStore(new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index"));
        processor.init(context);

        // First sighting of c2 at offset 5: the baseline seed lifts the frontier to c2@4
        // (the frontier advances) while the unsatisfied c1 dependency holds the record.
        context.setRecordMetadata("c2", 0, 5);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK,
                CausalClock.builder(TOPICS).require("c1", 0, 0).build().toBytes());
        processor.process(new Record<>("k", "held", 5678L, headers));

        List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertEquals(1, forwarded.size(),
                "a buffered record whose receipt advanced a consumed channel must emit exactly one heartbeat");
        Record<? extends String, ? extends String> heartbeat = forwarded.get(0).record();
        assertTrue(heartbeat.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) != null,
                "the heartbeat must be a null message");
        assertEquals(5678L, heartbeat.timestamp(),
                "the heartbeat must carry the held record's timestamp, not the wall clock");
    }

    /**
     * A relayed null message — re-emitted because the received one carried news — propagates the
     * received message's own timestamp onward: a relay carries the original trigger's event time,
     * never re-stamping it with this node's wall clock.
     *
     * Asserts the relayed null message's timestamp equals the received one's.
     */
    @Test
    @SuppressWarnings("NullAway") // the null message TestRecord intentionally has null key/value
    void relayedNullMessageCarriesTheReceivedMessagesTimestamp() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("c1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c6", new StringDeserializer(), new StringDeserializer());

            // Carries news on the consumed channel itself (a c1 claim above anything this node
            // has received — offsets 1..3 still in flight), so the relay trigger fires and it
            // relays. A claim on an unconsumed coordinate would be custody: folded, never
            // relayed.
            Headers newsworthy = ParsleyHeader.mutableHeaders();
            newsworthy.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
            newsworthy.add(ParsleyHeader.CAUSAL_CLOCK,
                    CausalClock.builder(TOPICS).require("c1", 0, 3).build().toBytes());
            c1.pipeInput(new TestRecord<>(null, null, newsworthy, 4242L));

            List<TestRecord<String, String>> emitted = out.readRecordsToList();
            assertEquals(1, emitted.size(), "a null message carrying news must be relayed exactly once");
            assertEquals(4242L, emitted.get(0).timestamp(),
                    "the relay must propagate the received null message's own timestamp");
        }
    }

    /**
     * The event-time consequence the trigger-timestamp rule exists for: backfill-timestamped input
     * through a non-emitting causal stage must not advance a downstream delegate's stream time to
     * the wall clock. Kafka Streams advances partition/stream time from every polled record's
     * timestamp — a null message included — before any processor classifies it, so a
     * wall-clock-stamped null message would expire downstream windows, grace periods, and
     * suppressions mid-backfill.
     *
     * Asserts the downstream stage's observed stream time equals the backfill timestamp, not the
     * driver's wall clock.
     */
    @Test
    void backfillThroughANonEmittingStageDoesNotAdvanceDownstreamStreamTimeToWallClock() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("c1", Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(dropper()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build())
                .to("c7", Produced.with(Serdes.String(), Serdes.String()));
        List<Long> downstreamStreamTimes = new ArrayList<>();
        builder.stream("c7", Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new Processor<String, String, String, String>() {
                    private ProcessorContext<String, String> ctx;
                    @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
                    @Override public void process(Record<String, String> record) {
                        downstreamStreamTimes.add(ctx.currentStreamTimeMs());
                    }
                });

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());

            // A backfill-aged record: its event time is far behind the driver's wall clock.
            c1.pipeInput(new TestRecord<>("k", "historic", depsHeader(CausalClock.empty()), 1000L));

            assertEquals(List.of(1000L), downstreamStreamTimes,
                    "the downstream stage's stream time must follow the data's event time through "
                            + "the null message — never jump to the wall clock");
        }
    }

    /**
     * Causal input topics with mismatched partition counts fail startup fast, unconditionally:
     * co-partitioning is impossible, so the causal frontier would evaluate against an
     * incomplete partition set — a causal-safety hole with no opt-down.
     *
     * Asserts driver construction throws, wrapping an {@link IllegalStateException} whose message
     * names the mismatch.
     */
    @Test
    void mismatchedInputPartitionCountsFailStartup() {
        ParsleyTopicAdmin mismatched = TestTopicAdmin.of(
                Map.of("c2", C2_ID, "c3", C3_ID), Map.of("c2", 2, "c3", 3));
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c3", Serdes.String(), Serdes.String()))
                        .topicAdmin(mismatched).build(),
                List.of("c2", "c3"));

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config()),
                "a source partition-count mismatch must fail startup");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the parity check's failure");
        assertTrue(message(cause(thrown)).contains("mismatched partition counts"),
                "the failure must name the mismatch: " + message(cause(thrown)));
    }

    /**
     * A malformed {@code delivery.timeout.ms} override fails init naming the key and the value,
     * instead of silently falling back to Kafka's 120 s default: the value bounds the crossing
     * wait and is the stall-diagnostic threshold, so a typo that quietly became the default would
     * misconfigure both. (An absent key still defaults to Kafka's 120 s.)
     *
     * Asserts driver construction throws with an {@link IllegalStateException} naming the key and
     * the malformed value.
     */
    @Test
    void malformedDeliveryTimeoutFailsInitNamingTheKeyAndValue() {
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("c1"));
        Properties props = config();
        props.put("producer.delivery.timeout.ms", "not-a-number");

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, props),
                "a malformed delivery.timeout.ms must fail init, never silently default");
        assertEquals(IllegalStateException.class, cause(thrown).getClass(),
                "the wrapped cause must be the malformed-timeout guard's exception");
        assertTrue(message(cause(thrown)).contains("delivery.timeout.ms")
                        && message(cause(thrown)).contains("not-a-number"),
                "the failure must name the key and the malformed value: " + message(cause(thrown)));
    }

    /**
     * The {@code delivery.timeout.ms} resolution's happy paths, pinned directly (the value bounds
     * the crossing wait and is the stall-diagnostic threshold): a {@code producer.}-prefixed
     * override wins whether it arrives as a {@link Number} or a numeric string, the un-prefixed
     * client key Streams passes through is honoured next, and an absent key resolves to Kafka's
     * 120 s default.
     *
     * Asserts each source of the value resolves to exactly the configured milliseconds.
     */
    @Test
    void deliveryTimeoutResolutionHonoursOverridesAndDefaults() {
        assertEquals(45_000L, ParsleyProcessor.deliveryTimeoutMs(
                        Map.of("producer.delivery.timeout.ms", 45_000)),
                "a Number-typed producer.-prefixed override must win");
        assertEquals(45_000L, ParsleyProcessor.deliveryTimeoutMs(
                        Map.of("producer.delivery.timeout.ms", " 45000 ")),
                "a numeric-string producer.-prefixed override must parse (trimmed)");
        assertEquals(30_000L, ParsleyProcessor.deliveryTimeoutMs(
                        Map.of("delivery.timeout.ms", "30000")),
                "the un-prefixed client key Streams passes through must be honoured");
        assertEquals(120_000L, ParsleyProcessor.deliveryTimeoutMs(Map.of()),
                "an absent key must resolve to Kafka's 120 s default");
    }

    /**
     * Causal input topics that share a partition count pass the startup checks and start normally —
     * a guard against the parity check false-positiving on a correctly co-partitioned topology.
     *
     * Asserts the topology constructs with two equally-partitioned inputs.
     */
    @Test
    void equalInputPartitionCountsPassTheStartupChecks() {
        ParsleyTopicAdmin equal = TestTopicAdmin.of(
                Map.of("c2", C2_ID, "c3", C3_ID), Map.of("c2", 4, "c3", 4));
        Topology topology = topology(
                ParsleyProcessorSupplier.builder(upperCaser()).addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c3", Serdes.String(), Serdes.String()))
                        .topicAdmin(equal).build(),
                List.of("c2", "c3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            driver.createInputTopic("c2", new StringSerializer(), new StringSerializer())
                    .pipeInput(new TestRecord<>("k", "ok", depsHeader(CausalClock.empty())));
            assertEquals(List.of("ok"), processed,
                    "the parity check must pass when input partition counts match: the task processes normally");
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

    private static ParsleyVectorClock frontierIn(TopologyTestDriver driver, String frontierStoreName) {
        KeyValueStore<String, byte[]> store = driver.getKeyValueStore(frontierStoreName);
        byte[] blob = store.get(ParsleyStores.FRONTIER_KEY);
        if (blob == null) {
            return ParsleyVectorClock.empty();
        }
        return ParsleyFrontierState.fromBytes(blob).frontier();
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
