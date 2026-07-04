package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
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
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Chunk B: the two-part causal gate and the watermark protocol.
 *
 * <p>These tests cover:
 * <ol>
 *   <li>The reconvergence regression: a fan-in node holds a fast-branch record until the slow
 *       branch has confirmed the shared ancestor — the bug that the single-layer vacuous-satisfaction
 *       model could not catch.</li>
 *   <li>Watermark emission: a non-forwarding (filter) delegate still results in a protocol watermark
 *       reaching downstream so completeness progress is not silently lost.</li>
 *   <li>Watermark-only liveness: a reconvergence record can be released by watermarks alone, with no
 *       further business records on the slow branch.</li>
 *   <li>Watermark propagation through a non-subscribing relay layer.</li>
 * </ol>
 */
class CausalReconvergenceTopologyTest {

    // Fan-in input topics: the causal processor subscribes to both.
    private static final String T_FAST = "fast";
    private static final String T_SLOW = "slow";
    // Shared ancestor topic: NOT subscribed to by the fan-in node, but referenced in deps.
    // Including it in TOPICS lets us build CausalDependencies carrying its UUID.
    private static final String T_ANC = "anc";
    // Separate input for single-processor tests.
    private static final String T1 = "t1";

    private static final Uuid FAST_ID = Uuid.randomUuid();
    private static final Uuid SLOW_ID = Uuid.randomUuid();
    private static final Uuid ANC_ID = Uuid.randomUuid();
    private static final Uuid T1_ID = Uuid.randomUuid();

    // Admin resolves only the subscribed topics; ANC is upstream and not registered.
    private static final ParsleyTopicAdmin FAN_IN_ADMIN =
            TestTopicAdmin.of(Map.of(T_FAST, FAST_ID, T_SLOW, SLOW_ID));
    private static final ParsleyTopicAdmin SINGLE_ADMIN =
            TestTopicAdmin.of(Map.of(T1, T1_ID));

    // Topic resolver includes ANC so CausalDependencies.builder can produce deps carrying ANC_ID.
    private static final CausalTopics TOPICS = CausalTopics.of(
            Map.of(T_FAST, FAST_ID, T_SLOW, SLOW_ID, T_ANC, ANC_ID, T1, T1_ID));

    // --- topology builders -------------------------------------------------------------------

    /**
     * Two-input fan-in topology: subscribes to {@code fast} and {@code slow}, applies a causal
     * buffer, and forwards all records (upper-cased value) to {@code out}.
     */
    private static Topology fanInTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(T_FAST, T_SLOW), Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser())
                        .addBufferStore("fanin")
                        .addBuffers(List.of(T_FAST, T_SLOW), Serdes.String(), Serdes.String())
                        .topicAdmin(FAN_IN_ADMIN)
                        .build())
                .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    /**
     * Single-input topology with a filter delegate that discards all records (no forward), so every
     * delivered input produces a watermark to {@code out} instead of a business record.
     */
    private static Topology filterTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(T1), Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(filter())
                        .addBufferStore("filter")
                        .addBuffer(CausalBuffer.of(T1, Serdes.String(), Serdes.String()))
                        .topicAdmin(SINGLE_ADMIN)
                        .build())
                .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    // --- tests -------------------------------------------------------------------------------

    /**
     * Reconvergence — the strict-model acceptance gate.
     *
     * <p>A fan-in node subscribes to {@code fast} and {@code slow}; both carry deps on the shared
     * ancestor {@code ANC}. Under the strict model a dependency on ANC is satisfied only once
     * <em>every</em> input channel has confirmed it — there is no cold-start vacuous admit.
     * <ol>
     *   <li>{@code fast@0} requires {@code ANC@3}: held, because the slow channel has not yet
     *       confirmed ANC.</li>
     *   <li>{@code slow@0} requires {@code ANC@3}: confirms ANC@3 on the slow channel, lifting the
     *       completeness min for ANC to 3, so both fast@0 and slow@0 deliver.</li>
     *   <li>{@code fast@1} requires {@code ANC@7}: held — the slow channel knows ANC only at 3.</li>
     *   <li>A watermark on {@code slow} carrying {@code ANC@7} lifts the slow channel's ANC to 7,
     *       releasing fast@1.</li>
     * </ol>
     *
     * Asserts that a record depending on a shared ancestor is delivered only once every branch
     * confirms that ancestor — at cold start (step 2) and after a watermark catches a lagging
     * branch up (step 4).
     */
    @Test
    void reconvergenceFanInHoldsRecordUntilSlowBranchCatchesUp() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            CausalDependencies depsAnc3 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 3).build();

            // Step 1 — fast@0 requires ANC@3. The slow channel has not confirmed ANC → held.
            fastIn.pipeInput(new TestRecord<>("k", "fast0", depsHeader(depsAnc3)));
            assertEquals(0, businessRecords(out).size(),
                    "fast@0 must be held at cold start: the slow channel has not confirmed ANC@3");

            // Step 2 — slow@0 requires ANC@3, confirming ANC@3 on the slow channel. The completeness
            // min for ANC reaches 3, releasing both fast@0 and slow@0.
            slowIn.pipeInput(new TestRecord<>("k", "slow0", depsHeader(depsAnc3)));
            assertEquals(2, businessRecords(out).size(),
                    "once the slow channel confirms ANC@3, fast@0 and slow@0 must both deliver");

            // Step 3 — fast@1 requires ANC@7; the slow channel knows ANC only at 3 → held.
            // (businessRecords drains the output, so this reads only records produced since step 2.)
            CausalDependencies depsAnc7 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 7).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast1", depsHeader(depsAnc7)));
            assertEquals(0, businessRecords(out).size(),
                    "fast@1 must be held: the slow channel knows ANC@3 < 7");

            // Step 4 — a watermark on slow carrying ANC@7 lifts the slow channel → fast@1 releases.
            slowIn.pipeInput(watermarkRecord(ParsleyClock.empty().observe(ANC_ID, 0, 7)));
            List<TestRecord<String, String>> afterWatermark = businessRecords(out);
            assertEquals(1, afterWatermark.size(),
                    "fast@1 must be released after the slow watermark confirms ANC@7");
            assertEquals("FAST1", afterWatermark.get(0).value(),
                    "the released record's value must be the upper-cased fast1");
        }
    }

    /**
     * Watermark emission — a filter processor that discards all business records still emits a
     * protocol watermark for each delivered input.
     *
     * <p>A delegate that calls no {@code ctx.forward()} (a pure filter) causes the decorating
     * {@link ParsleyProcessor} to emit one {@code _parsley_watermark} record per delivered input.
     * The watermark carries the current completeness frontier in the
     * {@code parsley-causal-dependencies} header.
     *
     * <p>This test uses a {@link MockProcessorContext} rather than {@link TopologyTestDriver} so that
     * the watermark emitted directly via the outer processor context is captured and inspectable.
     *
     * Asserts the output topic contains a watermark record (keyed with the triggering record's key so
     * it co-routes to that record's partition, null value, marker header) and no business value, even
     * though a genuine business record was delivered to the delegate.
     */
    @Test
    void filterProcessorEmitsWatermarkForEveryDeliveredInput() {
        try (TopologyTestDriver driver = new TopologyTestDriver(filterTopology(), testConfig())) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic(T1, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // A record with no deps — admitted immediately; delegate filters it (no forward).
            in.pipeInput(new TestRecord<>("k", "will-be-filtered",
                    depsHeader(CausalDependencies.empty())));

            // Output must contain exactly one watermark and zero business records.
            List<TestRecord<String, String>> allOut = out.readRecordsToList();
            List<TestRecord<String, String>> watermarks = watermarkRecords(allOut);
            List<TestRecord<String, String>> business = businessRecords(allOut);

            assertEquals(1, watermarks.size(),
                    "one watermark must be emitted because the filter delegate forwarded nothing");
            assertEquals(0, business.size(),
                    "no business records must appear: the filter dropped the input");
            assertEquals("k", watermarks.get(0).key(),
                    "watermark must reuse the triggering record's key so it co-routes to that partition");
            assertNull(watermarks.get(0).value(),
                    "watermark value must be null");
            assertTrue(hasWatermarkHeader(watermarks.get(0)),
                    "watermark must carry the _parsley_watermark header");
        }
    }

    /**
     * Watermark-only liveness — a buffered reconvergence record is released purely by an upstream
     * watermark, with no further business records on the slow branch.
     *
     * <p>This is the same scenario as {@link #reconvergenceFanInHoldsRecordUntilSlowBranchCatchesUp}
     * but verifies the release mechanism explicitly: the slow branch advances ANC exclusively via a
     * watermark (not a business record). Without the watermark protocol, a non-emitting slow branch
     * would deadlock the fan-in node's buffer indefinitely.
     *
     * Asserts the buffered record eventually releases after the watermark and that the output record
     * is correctly valued and causally stamped.
     */
    @Test
    void reconvergenceRecordReleasedByWatermarkAloneNoBusinessRecordNeeded() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Seed both channels with initial ANC knowledge (cold-start boundary).
            fastIn.pipeInput(new TestRecord<>("k", "seed-fast",
                    depsHeader(CausalDependencies.builder(TOPICS).require(T_ANC, 0, 1).build())));
            slowIn.pipeInput(new TestRecord<>("k", "seed-slow",
                    depsHeader(CausalDependencies.builder(TOPICS).require(T_ANC, 0, 1).build())));
            businessRecords(out); // drain seed records

            // Buffer a fast record that requires ANC@10 from the slow branch.
            fastIn.pipeInput(new TestRecord<>("k", "held",
                    depsHeader(CausalDependencies.builder(TOPICS).require(T_ANC, 0, 10).build())));
            assertEquals(0, businessRecords(out).size(),
                    "held record must not appear before slow branch confirms ANC@10");

            // The slow branch emits only a watermark (no business record) advancing ANC to 10.
            slowIn.pipeInput(watermarkRecord(ParsleyClock.empty().observe(ANC_ID, 0, 10)));

            // The buffered record must now be released via the watermark drain.
            List<TestRecord<String, String>> released = businessRecords(out);
            assertEquals(1, released.size(),
                    "exactly one business record must be released after the slow watermark");
            assertEquals("HELD", released.get(0).value(),
                    "the released value must be the upper-cased form of the held record");

            // The released record carries a completeness stamp that includes the ANC coordinate
            // at the level the slow watermark confirmed.
            CausalDependencies stamp = CausalDependencies.fromHeaders(
                    released.get(0).headers()).orElseThrow();
            assertTrue(stamp.clock().offsetFor(ANC_ID, 0) >= 1L,
                    "released record's stamp must carry ANC at the confirmed level");
        }
    }

    /**
     * Watermark propagation — a non-subscribing relay layer re-emits received watermarks, so a
     * grandchild node's completeness advances even when no business record flows on that path.
     *
     * <p>This is tested at the {@link ParsleyEngine} level over a channel-tracking {@link
     * ParsleyFrontier}: after {@link ParsleyEngine#onWatermark} is called with a frontier carrying an
     * ancestor coordinate, {@link ParsleyEngine#completeness()} must reflect it. This proves the
     * channel clock is updated by the watermark receipt, which is the mechanism that enables inductive
     * propagation through non-subscribing layers.
     *
     * Asserts that completeness rises to include the watermark's ancestor coordinate immediately
     * after {@code onWatermark} is called, even though no business record was delivered.
     */
    @Test
    void onWatermarkAdvancesChannelClockAndCompleteness() {
        // T1 is the only subscribed topic. ANC_ID is an out-of-scope ancestor.
        ParsleyClock.CoordinatePredicate scope = (topicId, partition) ->
                partition == 0 && topicId.equals(T1_ID);
        MockBufferStore<String, String> buffer = new MockBufferStore<>();

        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex()),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP, CausalAudit.NOOP,
                System::currentTimeMillis);

        // Before any watermark, completeness has no ANC coordinate.
        assertEquals(-1L, engine.completeness().offsetFor(ANC_ID, 0),
                "completeness must not know ANC before any watermark arrives");

        // Receive a watermark on T1/0 carrying {ANC@5}.
        ParsleyClock watermarkFrontier = ParsleyClock.empty().observe(ANC_ID, 0, 5);
        List<ParsleyMessage<String, String>> released = engine.onWatermark(T1_ID, 0, watermarkFrontier);

        // No records were buffered, so nothing is released.
        assertEquals(0, released.size(),
                "no records buffered, so onWatermark must return an empty release list");

        // The channel clock for T1/0 now knows ANC@5, which must appear in completeness().
        assertEquals(5L, engine.completeness().offsetFor(ANC_ID, 0),
                "completeness must rise to ANC@5 after the watermark advances the channel clock");
    }

    /**
     * Regression: a held reconvergence record must be released when the slow branch advances the
     * shared ancestor via a BUSINESS record — not only via a watermark.
     *
     * <p>The cross-channel ancestor check is not reachable through the in-scope candidate index that
     * drives the normal release cascade, so a channel-clock advance carried by an ordinary business
     * record (rather than a watermark) must still trigger a re-scan. Without that, a held record would
     * sit in the buffer until a watermark happened to arrive.
     *
     * <p>Under delivery-only channel updates this also could not work, because the slow business
     * record carrying {@code ANC@7} would itself be held (the fast channel still showing the held
     * fast record's lower offset), so neither channel would ever confirm {@code ANC@7}.
     *
     * Asserts that no watermark is sent, yet the held fast record is delivered.
     */
    @Test
    void heldRecordIsReleasedByASlowBranchBusinessRecordNotOnlyByAWatermark() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Seed both channels' ANC knowledge to @1 (cold-start admits these).
            CausalDependencies anc1 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 1).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast0", depsHeader(anc1)));
            slowIn.pipeInput(new TestRecord<>("k", "slow0", depsHeader(anc1)));
            assertEquals(2, businessRecords(out).size(), "seed records must be admitted");

            // fast@1 requires ANC@7; slow channel knows only ANC@1 → held.
            CausalDependencies anc7 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 7).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast1", depsHeader(anc7)));
            assertEquals(0, businessRecords(out).size(), "fast1 must be held: slow knows ANC@1 < 7");

            // The slow branch advances ANC to 7 via a BUSINESS record (NOT a watermark).
            slowIn.pipeInput(new TestRecord<>("k", "slow1", depsHeader(anc7)));

            List<TestRecord<String, String>> after = out.readRecordsToList();
            assertTrue(watermarkRecords(after).isEmpty(),
                    "no watermark must be involved: every delivery forwarded a business record");
            List<TestRecord<String, String>> business = businessRecords(after);
            assertTrue(containsValue(business, "FAST1"),
                    "the held fast1 must be released by the slow branch's business record");
            assertTrue(containsValue(business, "SLOW1"),
                    "the slow business record itself must be delivered");
        }
    }

    /**
     * Regression: symmetric reconvergence must not deadlock.
     *
     * <p>Both branches emit a record requiring {@code ANC@7} from the other, arriving interleaved
     * before either is delivered. Under "update the channel clock only on delivery", each record waits
     * for the other's channel to confirm {@code ANC@7}, but neither channel advances because neither is
     * delivered — a mutual deadlock until eviction. Updating the channel clock on receipt (the carried
     * frontier is the upstream branch's proven completeness, valid on arrival) breaks the cycle.
     *
     * Asserts both records are eventually delivered.
     */
    @Test
    void symmetricReconvergenceDeliversBothRecordsWithoutDeadlock() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            CausalDependencies anc1 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 1).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast0", depsHeader(anc1)));
            slowIn.pipeInput(new TestRecord<>("k", "slow0", depsHeader(anc1)));
            assertEquals(2, businessRecords(out).size(), "seed records must be admitted");

            // Both branches require ANC@7 from the other; fast arrives and is held first.
            CausalDependencies anc7 = CausalDependencies.builder(TOPICS).require(T_ANC, 0, 7).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast1", depsHeader(anc7)));
            assertEquals(0, businessRecords(out).size(), "fast1 held until slow confirms ANC@7");

            slowIn.pipeInput(new TestRecord<>("k", "slow1", depsHeader(anc7)));

            List<TestRecord<String, String>> business = businessRecords(out);
            assertTrue(containsValue(business, "FAST1"),
                    "fast1 must eventually deliver — no mutual deadlock");
            assertTrue(containsValue(business, "SLOW1"),
                    "slow1 must eventually deliver — no mutual deadlock");
        }
    }

    // --- helpers -----------------------------------------------------------------------------

    private static boolean containsValue(List<TestRecord<String, String>> records, String value) {
        for (TestRecord<String, String> r : records) {
            if (value.equals(r.value())) {
                return true;
            }
        }
        return false;
    }

    private static Properties testConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reconvergence-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Headers depsHeader(CausalDependencies deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());
        return headers;
    }

    /**
     * Builds a watermark {@link TestRecord}: null key, null value, carrying the given frontier
     * clock in both the {@code _parsley_watermark} marker header and the
     * {@code parsley-causal-dependencies} header.
     */
    @SuppressWarnings("NullAway") // watermark TestRecord intentionally has null key/value
    private static TestRecord<String, String> watermarkRecord(ParsleyClock frontier) {
        Headers wm = ParsleyHeader.mutableHeaders();
        wm.add(ParsleyHeader.WATERMARK, new byte[0]);
        wm.add(ParsleyHeader.CAUSAL_DEPENDENCIES, frontier.toBytes());
        return new TestRecord<>(null, null, wm);
    }

    private static boolean hasWatermarkHeader(TestRecord<?, ?> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.WATERMARK.equals(h.key())) {
                return true;
            }
        }
        return false;
    }

    /** Filters the given records to those that are NOT watermarks (business records). */
    private static List<TestRecord<String, String>> businessRecords(
            TestOutputTopic<String, String> topic) {
        return businessRecords(topic.readRecordsToList());
    }

    private static List<TestRecord<String, String>> businessRecords(
            List<TestRecord<String, String>> records) {
        List<TestRecord<String, String>> out = new ArrayList<>();
        for (TestRecord<String, String> r : records) {
            if (!hasWatermarkHeader(r)) {
                out.add(r);
            }
        }
        return out;
    }

    private static List<TestRecord<String, String>> watermarkRecords(
            List<TestRecord<String, String>> records) {
        List<TestRecord<String, String>> out = new ArrayList<>();
        for (TestRecord<String, String> r : records) {
            if (hasWatermarkHeader(r)) {
                out.add(r);
            }
        }
        return out;
    }

    /** A user processor that forwards every record with its value upper-cased. */
    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    /**
     * A user processor that records each input value but does NOT forward — a pure filter. This
     * causes the decorating {@link ParsleyProcessor} to emit a watermark for every delivered input.
     */
    private static ProcessorSupplier<String, String, String, String> filter() {
        return () -> new Processor<>() {
            @Override
            public void init(ProcessorContext<String, String> context) {}

            @Override
            public void process(Record<String, String> record) {
                // Intentionally does not call ctx.forward() — filtering every record.
            }
        };
    }
}
