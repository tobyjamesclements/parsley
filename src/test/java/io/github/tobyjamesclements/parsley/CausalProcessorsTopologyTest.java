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
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CausalProcessors} — the decorating causal processor — through a real Kafka Streams
 * topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalProcessorsTopologyTest {

    // Topic name → topic-id constant mapping used across tests.
    // t1 = default single-input topic; t2/t3 = two-source tests; t4 = materialized derived topic;
    // t5 = independent sidecar topic.
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    private static final Uuid T3_ID = Uuid.randomUuid();
    private static final Uuid T4_ID = Uuid.randomUuid();
    private static final Uuid T5_ID = Uuid.randomUuid();
    private static final Uuid GHOST_ID = Uuid.randomUuid();

    // Fake admin resolving the test topics' UUIDs (no broker under TopologyTestDriver); injected into
    // every builder via the package-private topicAdmin(...) seam.
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of(
            "t1", T1_ID, "t2", T2_ID, "t3", T3_ID, "t4", T4_ID, "t5", T5_ID));

    // Resolver mapping the same test topic names to their UUIDs, for building CausalDependencies.
    private static final CausalTopics TOPICS = CausalTopics.of(Map.of(
            "t1", T1_ID, "t2", T2_ID, "t3", T3_ID, "t4", T4_ID, "t5", T5_ID, "ghost", GHOST_ID));

    private final List<String> processed = new ArrayList<>();

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
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
            assertEquals(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build(), outDeps(emitted),
                    "forward must be stamped with the frontier as of admission");
        }
    }

    /**
     * A record whose dependencies were derived at the edge with
     * {@link CausalDependencies#from(CausalTopics, org.apache.kafka.clients.consumer.ConsumerRecord)}
     * is gated on the triggering record's own position: it is held until the processor observes that
     * position, then delivered. This proves the edge API's own-coordinate semantic against the real
     * gate — a record produced after consuming {@code t2@0} must not be delivered before {@code t2@0}.
     *
     * Asserts the t1 record stamped via {@code from(t2@0)} is buffered until a t2 record at offset 0
     * arrives, then drains.
     */
    @Test
    void recordStampedViaFromIsHeldUntilItsTriggerCoordinateIsObserved() {
        // The dependencies a downstream producer would attach after consuming t2@0 (no carried deps).
        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> trigger =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("t2", 0, 0L, "tk", "tv");
        CausalDependencies stampedFromTrigger = CausalDependencies.from(TOPICS, trigger);

        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("t1", "t2"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());

            // The t1 record depends on t2@0, which has not arrived: it must be held.
            t1.pipeInput(new TestRecord<>("k", "held", depsHeader(stampedFromTrigger)));
            assertEquals(List.of(), processed,
                    "a record stamped via from(t2@0) must be held until t2@0 is observed");

            // The triggering coordinate arrives (lands at t2 offset 0), draining the held record.
            t2.pipeInput(new TestRecord<>("tk", "trigger", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("trigger", "held"), processed,
                    "once t2@0 is observed, the record stamped via from(t2@0) must drain in causal order");
        }
    }

    /**
     * A record whose dependencies are not yet satisfied is buffered. When the satisfying record
     * arrives, the engine releases the buffered record through the delegate in causal order.
     *
     * <p>The buffer store must hold the record while it is waiting and be empty after drain.
     *
     * Asserts that the buffered record is not delivered until the dependency arrives, and that
     * both records flow through the delegate in order after the dependency is satisfied.
     */
    @Test
    void heldRecordIsBufferedThenDrainedThroughDelegate() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build(),
                List.of("t2", "t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // t3-record depends on t2-0 offset 0, which hasn't arrived: held, not delivered.
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 0).build())));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertTrue(out.isEmpty(), "held record must not appear in the output topic");
            assertEquals(1, storeSize(bufferStore), "held record must be persisted to the buffer store");

            // t2-record arrives and advances the frontier, draining the t3-record through the delegate.
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("t2-val", "t3-val"), processed,
                    "delegate must see both records in causal order after the dependency arrives");
            assertEquals(List.of("T2-VAL", "T3-VAL"), out.readValuesToList(),
                    "both transformed values must appear in the output in order");
            assertEquals(0, storeSize(bufferStore), "drained record must be removed from the buffer store");
        }
    }

    /**
     * {@code addBuffers(Collection<CausalBuffer>)} registers every buffer in the collection, exactly
     * like calling {@code addBuffer} once per element — the convenience overload for buffers with
     * distinct (non-shared) serdes.
     *
     * Asserts that records on every topic registered via the collection overload are admitted and
     * forwarded through the delegate.
     */
    @Test
    void addBuffersCollectionOverloadRegistersEveryBuffer() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of(
                                CausalBuffer.of("t2", Serdes.String(), Serdes.String()),
                                CausalBuffer.of("t3", Serdes.String(), Serdes.String())))
                        .topicAdmin(ADMIN).build(),
                List.of("t2", "t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));
            t3.pipeInput(new TestRecord<>("k", "t3-val", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("T2-VAL", "T3-VAL"), out.readValuesToList(),
                    "both buffers registered via the collection overload must admit and forward their records");
        }
    }

    /**
     * A record whose dependency cannot be satisfied within a buffer size limit of 1 is evicted —
     * but per the always-forward model, the delegate still runs and the record still reaches the
     * output (delivered out of causal order).
     *
     * Asserts that the delegate runs and the transformed record reaches the output.
     */
    @Test
    void evictedRecordIsForwardedToDelegate() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t3", Serdes.String(), Serdes.String()))
                        .withConfig("parsley.buffer.eviction.failure.policy", "continue")
                        .topicAdmin(ADMIN).build(),
                List.of("t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Depends on a t2 record that never arrives; size limit 1 evicts immediately.
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 99).build())));

            assertEquals(List.of("t3-val"), processed,
                    "eviction still runs the delegate — Parsley never drops a record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("T3-VAL", emitted.value(), "the evicted record's transform must still be applied");
        }
    }

    /**
     * Under a duration-based buffer limit, the eviction punctuator must only evict records that
     * have individually aged past the configured duration — not the entire buffer, which was the
     * bug being fixed here. Two records are buffered one duration-interval apart; when the
     * punctuator fires at the older record's age boundary, only that older record must be
     * evicted (and forwarded), leaving the younger one held.
     *
     * Asserts that after the punctuator fires, only the older record reaches the output and the
     * younger record remains in the buffer store.
     */
    @Test
    void durationEvictionOnlyEvictsRecordsThatHaveIndividuallyAgedOut() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofDuration(Duration.ofSeconds(2)))
                        .addBuffer(CausalBuffer.of("t3", Serdes.String(), Serdes.String()))
                        .withConfig("parsley.buffer.eviction.failure.policy", "continue")
                        .topicAdmin(ADMIN).build(),
                List.of("t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // Record A is buffered first; it will be 2s old (the full duration) once we advance below.
            t3.pipeInput(new TestRecord<>("k", "t3-val-A",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 99).build())));

            driver.advanceWallClockTime(Duration.ofSeconds(1));
            assertEquals(1, storeSize(bufferStore), "advancing by less than the duration must not fire eviction yet");
            assertTrue(out.isEmpty(), "no record must be evicted before the duration elapses");

            // Record B is buffered one second after A — only one second old at the next punctuation.
            t3.pipeInput(new TestRecord<>("k", "t3-val-B",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 99).build())));

            // Total elapsed since A was buffered is now 2s: the punctuator fires and A has aged out,
            // but B (buffered 1s ago) has not.
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            assertEquals(List.of("T3-VAL-A"), out.readValuesToList(),
                    "only the record that has individually aged past the duration must be evicted and forwarded");
            assertEquals(1, storeSize(bufferStore), "the younger record must remain held in the buffer");
        }
    }

    /**
     * When a size limit fires, only the oldest record needed to bring the buffer back under the
     * limit is evicted (and forwarded), leaving younger records held — the same partial-eviction
     * guarantee as the duration limit, but triggered synchronously by buffer depth instead of a
     * punctuator.
     *
     * Asserts that after the second record overflows a size-2 limit, only the older record
     * reaches the output and the younger record remains in the buffer store.
     */
    @Test
    void sizeLimitEvictionOnlyEvictsTheOldestOverflowingRecord() {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(2))
                        .addBuffer(CausalBuffer.of("t3", Serdes.String(), Serdes.String()))
                        .withConfig("parsley.buffer.eviction.failure.policy", "continue")
                        .topicAdmin(ADMIN).build(),
                List.of("t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            t3.pipeInput(new TestRecord<>("k", "t3-val-A",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 99).build())));
            assertEquals(1, storeSize(bufferStore), "buffering the first record must not fire eviction yet");
            assertTrue(out.isEmpty(), "no record must be evicted before the limit is reached");

            t3.pipeInput(new TestRecord<>("k", "t3-val-B",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 99).build())));

            assertEquals(List.of("T3-VAL-A"), out.readValuesToList(),
                    "only the oldest record must be evicted and forwarded once the size limit is reached");
            assertEquals(1, storeSize(bufferStore), "the younger record must remain held in the buffer");
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
                headers.add("parsley-causal-dependencies",
                        CausalDependencies.builder(TOPICS).require("t2", 0, 5).build().toBytes());
                ctx.forward(record.withHeaders(headers));
            }
        };
        Topology topology = topology(
                CausalProcessors.builder(user).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));

            TestRecord<String, String> emitted = out.readRecord();
            assertEquals(1, count(emitted.headers(), "parsley-causal-dependencies"),
                    "exactly one dependencies header — stamping must replace, not duplicate");
            assertEquals(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build(), outDeps(emitted),
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
                CausalProcessors.builder(user).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));
            out.readRecord(); // the live record
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            TestRecord<String, String> punctuated = out.readRecord();
            assertEquals("punct", punctuated.value(), "punctuator output must reach the topic");
            assertEquals(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build(), outDeps(punctuated),
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
                CausalProcessors.builder(user).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty())));

            assertEquals("v@t1", out.readValue(),
                    "getStateStore and recordMetadata must pass through the proxy context");
            assertEquals("v", driver.<String, String>getKeyValueStore("u-state").get("k"),
                    "the user's state-store write must be visible via the driver");
        }
    }

    /**
     * The buffer serializer resolves the key and value serdes using the buffered record's source
     * topic — not the changelog topic name of the buffer state store.
     *
     * <p>This matters for topic-specific serdes such as schema-registry Avro serdes, which must
     * use the original source topic as the schema-registry subject.
     *
     * Asserts that the value serde is invoked with the source topic name of the buffered record.
     */
    @Test
    void bufferSerdesAreResolvedAndInvokedWithTheSourceTopic() {
        SpyStringSerde valueSpy = new SpyStringSerde();
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), valueSpy))
                        .addBuffer(CausalBuffer.of("t3", Serdes.String(), valueSpy))
                        .topicAdmin(ADMIN).build(),
                List.of("t2", "t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());

            // An unmet dependency forces the t3-record to be buffered, which serialises it.
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 5).build())));

            assertTrue(valueSpy.serializeTopics.contains("t3"),
                    "the buffer value serde must be invoked with the record's source topic, not the changelog name");
            assertEquals(List.of("t3"), valueSpy.serializeTopics.stream().distinct().toList(),
                    "only the source topic 't3' is used to serialise the held record");
        }
    }

    /**
     * Two {@link CausalProcessors} instances in the same topology, each with a distinct
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
        builder.stream("t3", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("t3", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t3", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("t3-out", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("t2", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("t2", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("t2-out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3Out =
                    driver.createOutputTopic("t3-out", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> t2Out =
                    driver.createOutputTopic("t2-out", new StringDeserializer(), new StringDeserializer());

            t3.pipeInput(new TestRecord<>("k", "t3-val", depsHeader(CausalDependencies.empty())));
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("T3-VAL"), t3Out.readValuesToList(),
                    "t3 processor must forward to its own output topic");
            assertEquals(List.of("T2-VAL"), t2Out.readValuesToList(),
                    "t2 processor must forward to its own output topic");

            // Each decorator persisted only its own branch's frontier under its own namespace.
            assertEquals(ParsleyClock.empty().observe(T3_ID, 0, 0),
                    frontierIn(driver, "t3-frontier"),
                    "t3 frontier must reflect only t3's source coordinate");
            assertEquals(ParsleyClock.empty().observe(T2_ID, 0, 0),
                    frontierIn(driver, "t2-frontier"),
                    "t2 frontier must reflect only t2's source coordinate");
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
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(1000))
                        .addBuffers(List.of("t1", "t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build(),
                List.of("t1", "t2", "t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());

            for (int i = 0; i < 50; i++) {
                t1.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
                t2.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
                t3.pipeInput(new TestRecord<>("k", "v" + i, depsHeader(CausalDependencies.empty())));
            }

            ParsleyClock frontier = frontierIn(driver, "parsley-frontier");
            assertEquals(3, frontier.size(),
                    "the stamped frontier width must equal the number of source topics, not the record count");
        }
    }

    /**
     * Inbound dependency entries are used only to gate admissibility; they are never merged
     * into the frontier or re-stamped onto the output record.
     *
     * <p>This is the most important regression guard: a record carrying dependencies over
     * hundreds of unrelated partitions, force-forwarded after an eviction, must still carry
     * only its own source coordinate on the output. The inbound deps must not amplify the output
     * header size.
     *
     * Asserts that a record with 500 inbound dependency entries produces an output stamped
     * with exactly one dependency (the record's own source coordinate).
     */
    @Test
    void aLargeInboundDependencySetIsNeverFoldedIntoTheStampedOutput() {
        CausalDependencies.Builder bigBuilder = CausalDependencies.builder(TOPICS);
        for (int p = 0; p < 500; p++) {
            bigBuilder.require("ghost", p, 1_000 + p);
        }
        CausalDependencies big = bigBuilder.build();

        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .withConfig("parsley.buffer.eviction.failure.policy", "continue")
                        .topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "v", depsHeader(big)));

            CausalDependencies stamped = outDeps(out.readRecord());
            assertEquals(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build(), stamped,
                    "a 500-partition inbound dependency set must not enlarge the stamped output");
            assertEquals(1, stamped.clock().size(),
                    "the stamped output must carry only the source coordinate");
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
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build(),
                List.of("t2", "t3"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());

            // Buffer one t3-record (depends on t2-0 offset 0, not yet arrived).
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 0).build())));
            // Release it (the t2-record arrives and advances the frontier).
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));

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
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            // Dependencies require T1_ID/0@0 — exactly the record's own source coordinate.
            t1.pipeInput(new TestRecord<>("k", "hello",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "self-dep stripped → effective dependencies empty → forwarded immediately");
            assertEquals(0, storeSize(bufferStore),
                    "record must never enter the buffer even with size limit 1");
        }
    }

    /**
     * In a fused two-processor chain (no intermediate Kafka topic), proc1's frontier stamp
     * includes the current record's own source coordinate. Without self-reference stripping,
     * proc2 would hold every forwarded record indefinitely (circular dependency).
     *
     * <p>With stripping, proc2 recognises the self-reference and immediately admits the record.
     *
     * <p>Topology:
     * <pre>
     *   "t1" ──→ proc1("node1") ──→ (fused) ──→ proc2("node2") ──→ "out"
     * </pre>
     *
     * Asserts that the record flows through both processors and proc2's buffer is empty at the
     * end.
     */
    @Test
    void proc1StampDoesNotCircularlyBlockDownstreamInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        // Fused chain: proc1 → proc2, no .to("intermediate") between them.
        // proc1 admits the record, stamps its frontier (which now contains the source coord), and
        // forwards. proc2 receives the stamped record; because the topology is fused,
        // context.recordMetadata() still returns the original "t1" metadata → self-reference dep.
        builder.stream("t1", consumed)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("HELLO"), out.readValuesToList(),
                    "record must flow through both processors and reach the output");
            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "proc2 buffer must be empty — record was never held");
        }
    }

    /**
     * Verifies that two {@link CausalProcessors} instances chained via a materialized Kafka
     * topic each enforce causal ordering independently, with full drainage at both layers.
     *
     * <h2>Topology</h2>
     * <pre>
     *   [t2, t3] ──→ proc1("node1") ──→ "t4" (materialized)
     *                                          ↓
     *   [t2, t3] ──────────────────→ proc2("node2") ──→ "out"
     * </pre>
     * proc2 subscribes to "t4" (proc1's derived output) AND to "t2"/"t3" directly.
     *
     * <h2>Why materialization is required</h2>
     * Without {@code .to("t4")} the chain is fused: {@code recordMetadata().topic()} in proc2
     * returns the original Kafka source topic, so every forwarded record's dep includes its own
     * source coordinate — stripped at engine admission. With materialization, t4 records have
     * source {@code T4_ID} and deps {@code {T2_ID@x, T3_ID@y}}: no self-reference at all, and
     * the causal ordering across two layers is preserved.
     *
     * <h2>Why distinct {@code storeName} values are required</h2>
     * Each {@link CausalProcessorSupplier} registers three KeyValueStores. Without distinct names
     * Kafka Streams rejects the topology with a duplicate-store error.
     *
     * <h2>How proc2 bootstraps without circular dependency</h2>
     * Direct "t2" and "t3" subscriptions advance proc2's frontier. Once both dimensions are
     * satisfied, proc2 drains the buffered t4 records.
     */
    @Test
    void materializedChainEnablesFullDrainAtBothProcessorLayers() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String>  consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String>  produced = Produced.with(Serdes.String(), Serdes.String());

        var t2Src = builder.stream("t2", consumed);
        var t3Src = builder.stream("t3", consumed);
        var t4Src = builder.stream("t4",  consumed);

        // proc1: holds t3-records (dep on t2) until t2 arrives. Output materializes to "t4".
        t2Src.merge(t3Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("t4", produced);

        // proc2: receives "t4" (proc1's derived output) AND direct t2/t3 feeds to bootstrap its frontier.
        t4Src.merge(t2Src).merge(t3Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t4", "t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Phase 1: t3-records arrive before t2; held at proc1 AND at proc2 (direct subscription).
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 0).build())));
            assertTrue(out.isEmpty(), "t3-records must be held before t2 arrives");
            assertEquals(1, storeSize(driver.getKeyValueStore("node1-buffer")),
                    "proc1 must buffer the t3-record");
            assertEquals(1, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "proc2 must buffer the direct t3-record");

            // Phase 2: t2-record arrives — proc1 drains and materializes two t4-records, then
            // proc2 bootstraps its frontier from the direct t2/t3 and drains both t4-records.
            t2.pipeInput(new TestRecord<>("k", "t2-val",
                    depsHeader(CausalDependencies.empty())));

            // Both layers drained completely; no records stuck in either buffer.
            assertEquals(0, storeSize(driver.getKeyValueStore("node1-buffer")),
                    "node1 buffer must be empty after drain");
            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "node2 buffer must be empty after drain");

            // "out" has 4 records: direct t2-val, direct t3-val (proc2's own admissions) plus
            // the proc1-forwarded t2-val and t3-val (arriving via "t4").
            List<String> outValues = out.readValuesToList();
            assertEquals(4, outValues.size(), "four records must reach the output");
            assertEquals(2, outValues.stream().filter("T2-VAL"::equals).count(),
                    "two T2-VAL records: direct and via t4");
            assertEquals(2, outValues.stream().filter("T3-VAL"::equals).count(),
                    "two T3-VAL records: direct and via t4");

            // proc1's frontier spans both source topics after draining.
            assertEquals(
                    ParsleyClock.empty()
                            .observe(T2_ID, 0, 0)
                            .observe(T3_ID, 0, 0),
                    frontierIn(driver, "node1-frontier"),
                    "proc1 frontier must span both t2 and t3 after drain");

            // proc2's frontier spans t2 and t3 (from direct admissions) plus t4@1 (last t4-record drained).
            ParsleyClock f2 = frontierIn(driver, "node2-frontier");
            assertEquals(0L, f2.offsetFor(T2_ID, 0), "proc2 must have seen t2 directly");
            assertEquals(0L, f2.offsetFor(T3_ID, 0), "proc2 must have seen t3 directly");
            assertEquals(1L, f2.offsetFor(T4_ID, 0), "proc2 must have drained both t4-records");
        }
    }

    /**
     * Backlog #15 ("fused processor chain footgun") regression: the same multi-topic fan-in shape
     * as {@link #materializedChainEnablesFullDrainAtBothProcessorLayers()}, but WITHOUT the
     * {@code .to("t4")} materialization step — proc1's output feeds proc2 directly (fused), in
     * addition to proc2's direct t2/t3 subscriptions.
     *
     * <h2>Why this could deadlock</h2>
     * In a fused chain, {@code context.recordMetadata()} in proc2 reports the ORIGINAL source
     * topic/offset (not a synthetic intermediate one), so a record relayed from proc1 can carry a
     * dependency that is NOT its own coordinate (e.g. a t2-sourced record carrying a t3 dependency
     * proc1 had already observed). #15 claimed this circularly blocks proc2 forever. This test
     * proves that claim false today: {@link ParsleyEngine#effectiveDependencies} strips only the
     * exact self-coordinate match, and proc1 always relays records in its own admission order, so
     * any "other dimension" a relayed record still depends on was already established at proc2 by
     * an earlier relayed (or directly-subscribed) record.
     *
     * <h2>Topology</h2>
     * <pre>
     *   [t2, t3] ──→ proc1("node1") ──┐ (fused, no .to())
     *                                  ↓
     *   [t2, t3] ─────────────────→ proc2("node2") ──→ "out"
     * </pre>
     */
    @Test
    void fusedChainWithoutMaterializationStillFullyDrainsBothLayers() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var t2Src = builder.stream("t2", consumed);
        var t3Src = builder.stream("t3", consumed);

        // proc1: holds t3-records (dep on t2) until t2 arrives. No .to() — output stays in-process.
        var viaProc1 = t2Src.merge(t3Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build());

        // proc2: receives proc1's output directly (FUSED) AND direct t2/t3 feeds to bootstrap its frontier.
        viaProc1.merge(t2Src).merge(t3Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t2", "t3"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t3 =
                    driver.createInputTopic("t3", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Phase 1: t3-records arrive before t2; held at proc1 AND at proc2 (direct subscription).
            t3.pipeInput(new TestRecord<>("k", "t3-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t2", 0, 0).build())));
            assertTrue(out.isEmpty(), "t3-records must be held before t2 arrives");
            assertEquals(1, storeSize(driver.getKeyValueStore("node1-buffer")),
                    "proc1 must buffer the t3-record");
            assertEquals(1, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "proc2 must buffer the direct t3-record");

            // Phase 2: t2-record arrives — proc1 drains and relays both records straight into
            // proc2 (no intermediate topic); proc2 also bootstraps directly from t2/t3 and must
            // NOT get stuck on the relayed records' non-self-coordinate dependencies.
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));

            // Both layers drained completely; no records stuck in either buffer (the deadlock #15
            // describes would manifest as a non-zero node2-buffer size here).
            assertEquals(0, storeSize(driver.getKeyValueStore("node1-buffer")),
                    "node1 buffer must be empty after drain");
            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "node2 buffer must be empty after drain — no fused-chain deadlock");

            // "out" has 4 records: direct t2-val, direct t3-val (proc2's own admissions) plus the
            // proc1-relayed t2-val and t3-val (arriving via the fused edge, not a topic).
            List<String> outValues = out.readValuesToList();
            assertEquals(4, outValues.size(), "four records must reach the output");
            assertEquals(2, outValues.stream().filter("T2-VAL"::equals).count(),
                    "two T2-VAL records: direct and via the fused proc1 edge");
            assertEquals(2, outValues.stream().filter("T3-VAL"::equals).count(),
                    "two T3-VAL records: direct and via the fused proc1 edge");

            // Both processors' frontiers span both source topics after draining.
            assertEquals(
                    ParsleyClock.empty().observe(T2_ID, 0, 0).observe(T3_ID, 0, 0),
                    frontierIn(driver, "node1-frontier"),
                    "proc1 frontier must span both t2 and t3 after drain");
            assertEquals(
                    ParsleyClock.empty().observe(T2_ID, 0, 0).observe(T3_ID, 0, 0),
                    frontierIn(driver, "node2-frontier"),
                    "proc2 frontier must span both t2 and t3 after drain");
        }
    }

    /**
     * Materialized chain: a clocked sidecar record ("t5") whose dependency points to the
     * derived topic ("t4") is buffered in proc2 until proc1 materializes that record.
     *
     * <h2>Topology</h2>
     * <pre>
     *   "t2" ──→ proc1("node1") ──→ "t4" (materialized)
     *                                       ↓
     *   "t2" ─────────────────────→ proc2("node2") ──→ "out"
     *   "t5" ─────────────────────→ proc2("node2")
     * </pre>
     *
     * Asserts that the sidecar is held while t4@0 is absent from proc2's frontier, and
     * released (last, in causal order) once the dependency is satisfied.
     */
    @Test
    void bufferClockedSidecarUntilDerivedTopicArrivesInMaterializedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var t2Src    = builder.stream("t2",    consumed);
        var t4Src    = builder.stream("t4",    consumed);
        var t5Src = builder.stream("t5", consumed);

        t2Src
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("t4", produced);

        // proc2 takes proc1's materialised output, the original t2 (to bootstrap T2 frontier),
        // and the independent "t5" stream.
        t4Src.merge(t2Src).merge(t5Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t4", "t2", "t5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t5 =
                    driver.createInputTopic("t5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // t5-record (sidecar) arrives before t4@0 has been produced — must be buffered.
            t5.pipeInput(new TestRecord<>("k", "t5-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t4", 0, 0).build())));
            assertTrue(out.isEmpty(), "sidecar must be held: t4@0 not yet in proc2's frontier");
            assertEquals(1, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "sidecar must be in proc2's buffer");

            // t2-record arrives: proc1 materialises t4@0 (stamped {T2_ID@0}).
            // proc2's direct t2 subscription advances its T2 frontier, allowing it to admit
            // t4@0, which advances its T4 frontier and drains the sidecar.
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));

            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "sidecar must have drained once t4@0 was admitted by proc2");
            List<String> values = out.readValuesToList();
            assertEquals(3, values.size(), "t2-val (direct), t2-val (via t4), and t5-val");
            assertEquals(2, values.stream().filter("T2-VAL"::equals).count(),
                    "two T2-VAL records: direct and via t4");
            assertEquals(1, values.stream().filter("T5-VAL"::equals).count(),
                    "one T5-VAL record released after t4@0 satisfied its dependency");
            assertEquals("T5-VAL", values.get(values.size() - 1),
                    "sidecar must be last: buffered until t4@0 advanced the T4 frontier");
        }
    }

    /**
     * Fused chain: a clocked sidecar record ("t5") whose dependency points to the upstream
     * source ("t1") is buffered in proc2 until proc1 processes a "t1" record and fuses to proc2.
     *
     * <h2>Topology</h2>
     * <pre>
     *   "t1"  ──→ proc1("node1") ──→ (fused) ──→ proc2("node2") ──→ "out"
     *   "t5" ──────────────────────────────────→ proc2("node2")
     * </pre>
     *
     * <p>This proves that self-dep stripping does not prevent proc2's T1_ID frontier from
     * advancing: proc2 correctly recognises that T1_ID@0 is now satisfied and releases the sidecar.
     *
     * Asserts that the sidecar is held while T1_ID@0 is absent from proc2's frontier, and
     * released immediately after proc1 processes the "t1" record.
     */
    @Test
    void bufferClockedSidecarUntilFusedOutputAdvancesFrontierInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var t5Src = builder.stream("t5", consumed);

        // proc1 fused directly into proc2 (no .to("intermediate")); t5 also merges into proc2.
        builder.stream("t1", consumed)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .merge(t5Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t1", "t5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t5 =
                    driver.createInputTopic("t5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // t5-record depends on t1@0 which hasn't been processed yet — must be buffered.
            t5.pipeInput(new TestRecord<>("k", "t5-val",
                    depsHeader(CausalDependencies.builder(TOPICS).require("t1", 0, 0).build())));
            assertTrue(out.isEmpty(), "sidecar must be held: T1_ID@0 not yet in proc2's frontier");
            assertEquals(1, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "sidecar must be in proc2's buffer");

            // "t1"@0 arrives: proc1 admits it, stamps frontier {T1_ID@0}, fuses to proc2.
            // proc2's engine strips the self-dep → admits → frontier has T1_ID@0 → sidecar drains.
            t1.pipeInput(new TestRecord<>("k", "t1-val", depsHeader(CausalDependencies.empty())));

            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "sidecar must have drained once T1_ID@0 was in proc2's frontier");
            assertEquals(List.of("T1-VAL", "T5-VAL"), out.readValuesToList(),
                    "t1-val admitted first (self-dep stripped), t5-val drains immediately after");
        }
    }

    /**
     * Materialized chain: an unclocked sidecar record (no causal-dependencies header) is treated
     * as trivially satisfied — {@code CausalDependencies.empty()} — so it is admitted immediately
     * and never enters the buffer, regardless of where the materialized chain's frontier stands.
     *
     * Asserts that the unclocked record is forwarded immediately, stamped SATISFIED, and the
     * buffer remains empty.
     */
    @Test
    void admitUnclockedSidecarImmediatelyInMaterializedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var t2Src    = builder.stream("t2",    consumed);
        var t4Src    = builder.stream("t4",    consumed);
        var t5Src = builder.stream("t5", consumed);

        t2Src
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .to("t4", produced);

        t4Src.merge(t2Src).merge(t5Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t4", "t2", "t5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN)
                        .build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t5 =
                    driver.createInputTopic("t5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Unclocked t5-record: no parsley-causal-dependencies header → treated as
            // CausalDependencies.empty() → trivially satisfied → forwarded immediately.
            t5.pipeInput(new TestRecord<>("k", "t5-val"));
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("T5-VAL", emitted.value(),
                    "unclocked record must be forwarded immediately, trivially satisfied");
            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "unclocked record must never enter the buffer");

            // Normal materialized flow continues without interference from the unclocked record.
            t2.pipeInput(new TestRecord<>("k", "t2-val", depsHeader(CausalDependencies.empty())));
            assertEquals(2, out.readValuesToList().size(),
                    "t2-val direct and t4-derived must both appear after the unclocked record");
        }
    }

    /**
     * Fused chain: an unclocked sidecar record (no causal-dependencies header) is treated as
     * trivially satisfied and admitted immediately, interleaved correctly alongside the fused
     * proc1 output.
     *
     * Asserts that the unclocked record is forwarded immediately and the buffer remains empty.
     * Subsequent clocked records flow normally.
     */
    @Test
    void admitUnclockedSidecarImmediatelyInFusedChain() {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        var t5Src = builder.stream("t5", consumed);

        builder.stream("t1", consumed)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node1", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String())).topicAdmin(ADMIN).build())
                .merge(t5Src)
                .process(CausalProcessors.builder(upperCaser()).addBufferStore("node2", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of("t1", "t5"), Serdes.String(), Serdes.String()).topicAdmin(ADMIN).build())
                .to("out", produced);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t5 =
                    driver.createInputTopic("t5", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Unclocked t5-record forwarded immediately, trivially satisfied.
            t5.pipeInput(new TestRecord<>("k", "t5-val"));
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("T5-VAL", emitted.value(), "unclocked sidecar must be forwarded immediately");
            assertEquals(0, storeSize(driver.getKeyValueStore("node2-buffer")),
                    "unclocked record must never enter the buffer");

            // Fused proc1 output flows through proc2 normally alongside the earlier unclocked record.
            t1.pipeInput(new TestRecord<>("k", "t1-val", depsHeader(CausalDependencies.empty())));
            assertEquals(List.of("T1-VAL"), out.readValuesToList(),
                    "clocked t1-val must flow through both processors normally");
        }
    }

    /**
     * A {@link CausalAudit} registered via {@link CausalProcessors.Builder#withAudit} receives the
     * processor lifecycle events — {@code processorInitialized} on startup and {@code
     * processorClosing} when the driver shuts the topology down — in addition to the per-record
     * events already covered by {@link ParsleyEngineTest}.
     *
     * Asserts that {@code processorInitialized} fires once with {@code frontierRestored = false}
     * on a fresh state directory, and {@code processorClosing} fires once when the driver closes.
     */
    @Test
    void withAuditReceivesProcessorLifecycleEvents() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).withAudit(audit).build(),
                List.of("t1"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(null))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            t1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(1, audit.initializations.size(), "processorInitialized must fire once on startup");
            assertEquals(false, audit.initializations.get(0).frontierRestored(),
                    "a fresh state directory has no frontier to restore");
            assertTrue(audit.closings.isEmpty(), "processorClosing must not fire before the driver closes");
        }

        assertEquals(1, audit.closings.size(), "processorClosing must fire once when the driver closes");
    }

    /**
     * A record arrives on a topic for which no {@link CausalBuffer} was registered (e.g. an input
     * topic added to the stream but never wired up via {@code addBuffer}). The processor's intake
     * guard rejects it rather than silently treating it as dependency-free.
     *
     * Asserts that processing the record throws (wrapped by Kafka Streams in a
     * {@code StreamsException}) with a cause naming the unregistered topic.
     */
    @Test
    void ingestThrowsForATopicWithNoRegisteredBuffer() throws IOException {
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .topicAdmin(ADMIN).build(),
                List.of("t1", "ghost"));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(tempStateDir()))) {
            TestInputTopic<String, String> ghost =
                    driver.createInputTopic("ghost", new StringSerializer(), new StringSerializer());

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> ghost.pipeInput(new TestRecord<>("k", "v", depsHeader(CausalDependencies.empty()))),
                    "a record on an unregistered topic must not be silently admitted");
            assertEquals(IllegalStateException.class, thrown.getCause().getClass(),
                    "the wrapped cause must be the intake guard's exception");
            assertTrue(thrown.getCause().getMessage().contains("no CausalBuffer registered for topic 'ghost'"),
                    "the cause must name the unregistered topic: " + thrown.getCause().getMessage());
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
    void resolveTopicUuidsThrowsWhenTheAdminOmitsARegisteredTopic() throws IOException {
        ParsleyTopicAdmin incomplete = new ParsleyTopicAdmin() {
            @Override public Map<String, Uuid> topicIds(List<String> topics) { return Map.of(); }
            @Override public Map<String, Integer> partitionCounts(List<String> topics) { return Map.of(); }
            @Override public void createTopic(String name, int partitions) {}
            @Override public void close() {}
        };
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .topicAdmin(incomplete).build(),
                List.of("t1"));

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "startup must fail when the admin does not resolve every registered topic");
        Throwable guardFailure = thrown.getCause();
        assertEquals(IllegalStateException.class, guardFailure.getClass(),
                "the cause must be the UUID-resolution guard's exception directly, not re-wrapped");
        assertTrue(guardFailure.getMessage().contains("broker did not return a UUID for topic 't1'"),
                "the cause must name the unresolved topic: " + guardFailure.getMessage());
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
    void resolveTopicUuidsWrapsAnAdminFailure() throws IOException {
        ParsleyTopicAdmin throwing = new ParsleyTopicAdmin() {
            @Override public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                throw new TimeoutException("no broker reachable");
            }
            @Override public Map<String, Integer> partitionCounts(List<String> topics) { return Map.of(); }
            @Override public void createTopic(String name, int partitions) {}
            @Override public void close() {}
        };
        Topology topology = topology(
                CausalProcessors.builder(upperCaser()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .topicAdmin(throwing).build(),
                List.of("t1"));

        StreamsException thrown = assertThrows(StreamsException.class,
                () -> new TopologyTestDriver(topology, config(tempStateDir())),
                "startup must fail when the admin itself throws");
        assertEquals(IllegalStateException.class, thrown.getCause().getClass(),
                "the wrapped cause must be the resolveTopicUuids catch-and-rethrow");
        assertTrue(thrown.getCause().getMessage().contains("failed to resolve topic UUIDs for causal buffers"),
                "the wrapping message must name the failed resolution: " + thrown.getCause().getMessage());
        assertEquals(TimeoutException.class, thrown.getCause().getCause().getClass(),
                "the original admin failure must be preserved as the innermost cause");
    }

    /** A fresh, unique state directory — required when a test expects driver construction itself to
     * fail, since a failed construction cannot be closed to release its RocksDB locks. */
    private static File tempStateDir() throws IOException {
        return Files.createTempDirectory("parsley-topology-test-").toFile();
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

    private static ParsleyClock frontierIn(TopologyTestDriver driver, String frontierStoreName) {
        KeyValueStore<String, byte[]> store = driver.getKeyValueStore(frontierStoreName);
        return ParsleyClock.fromBytes(store.get("f"));
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
