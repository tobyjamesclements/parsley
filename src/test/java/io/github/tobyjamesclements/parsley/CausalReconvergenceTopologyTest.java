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
 * Tests for the two-part causal gate and the null message protocol.
 *
 * <p>Every record is checked against this node's actual, already-proven state — never against a stamp
 * the same record just supplied — so a fan-in record depending on a shared ancestor genuinely holds
 * until some channel has actually, gatedly delivered up to the required offset. A dependency on a
 * coordinate this node has no channel for at all is a different, fail-closed case (see
 * {@link ParsleyCausalBroadcastTest}), not a shortcut for genuine reconvergence — so the shared
 * ancestor here ({@code anc}) is a real, directly-consumed third input, not an out-of-scope one.
 *
 * <p>These tests cover:
 * <ol>
 *   <li>A shared-ancestor dependency is held until the ancestor's own channel genuinely, contiguously
 *       reaches the required offset, then releases, stamped with the offset actually reached.</li>
 *   <li>Null message emission: a non-forwarding (filter) delegate still results in a protocol null message
 *       reaching downstream so completeness progress is not silently lost.</li>
 *   <li>A null message carrying no business record still advances completeness, visible in a later,
 *       unrelated record's own outgoing stamp.</li>
 *   <li>Null message propagation through a non-subscribing relay layer.</li>
 * </ol>
 */
class CausalReconvergenceTopologyTest {

    // Fan-in input topics: the causal processor subscribes to all three.
    private static final String T_FAST = "fast";
    private static final String T_SLOW = "slow";
    // Shared ancestor topic: directly subscribed to by the fan-in node, so a dependency on it is a
    // genuine, gated wait — not an out-of-scope, fail-closed case.
    private static final String T_ANC = "anc";
    // Separate input for single-processor tests.
    private static final String C1 = "c1";

    private static final Uuid FAST_ID = Uuid.randomUuid();
    private static final Uuid SLOW_ID = Uuid.randomUuid();
    private static final Uuid ANC_ID = Uuid.randomUuid();
    private static final Uuid C1_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin FAN_IN_ADMIN =
            TestTopicAdmin.of(Map.of(T_FAST, FAST_ID, T_SLOW, SLOW_ID, T_ANC, ANC_ID));
    private static final ParsleyTopicAdmin SINGLE_ADMIN =
            TestTopicAdmin.of(Map.of(C1, C1_ID));

    private static final ParsleyTopics TOPICS = ParsleyTopics.of(
            Map.of(T_FAST, FAST_ID, T_SLOW, SLOW_ID, T_ANC, ANC_ID, C1, C1_ID));

    // --- topology builders -------------------------------------------------------------------

    /**
     * Three-input fan-in topology: subscribes to {@code fast}, {@code slow}, and their shared
     * ancestor {@code anc}, applies a causal buffer, and forwards all records (upper-cased value) to
     * {@code out}.
     */
    private static Topology fanInTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(T_FAST, T_SLOW, T_ANC), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(upperCaser())
                        .addBufferStore("fanin")
                        .addSources(List.of(T_FAST, T_SLOW, T_ANC), Serdes.String(), Serdes.String())
                        .topicAdmin(FAN_IN_ADMIN)
                        .build())
                .to("c2", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    /**
     * Single-input topology with a filter delegate that discards all records (no forward), so every
     * delivered input produces a null message to {@code out} instead of a business record.
     */
    private static Topology filterTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(C1), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(filter())
                        .addBufferStore("filter")
                        .addSource(new ParsleySource<>(C1, Serdes.String(), Serdes.String()))
                        .topicAdmin(SINGLE_ADMIN)
                        .build())
                .to("c2", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    // --- tests -------------------------------------------------------------------------------

    /**
     * A fan-in node directly subscribes to {@code fast}, {@code slow}, and their shared ancestor
     * {@code anc}. A record's own declared dependency is never proof enough on its own — every record
     * is checked against this node's actual, already-proven state — so a dependency on the shared
     * ancestor genuinely holds until {@code anc}'s own channel has actually, gatedly delivered up to
     * the required offset.
     * <ol>
     *   <li>{@code fast@0} requires {@code anc@3}: held — nothing has genuinely delivered {@code anc}
     *       yet.</li>
     *   <li>{@code anc@0..3} genuinely, contiguously deliver: releases the held {@code fast@0}.</li>
     *   <li>{@code slow@0} requires {@code anc@3}: delivers immediately, already satisfied.</li>
     *   <li>{@code fast@1} requires {@code anc@7}, higher than {@code anc}'s current frontier (3):
     *       held again.</li>
     *   <li>{@code anc@4..7} genuinely deliver: releases {@code fast@1}, stamped with {@code anc@7} —
     *       the offset {@code anc}'s own frontier had actually reached.</li>
     * </ol>
     *
     * Asserts each dependent record is held until {@code anc}'s own frontier genuinely reaches the
     * required offset, then released, and that the released stamp carries that exact offset.
     */
    @Test
    void sharedAncestorDependencyHeldUntilGenuineWitnessThenStampedWithMax() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> ancIn =
                    driver.createInputTopic(T_ANC, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c2", new StringDeserializer(), new StringDeserializer());

            CausalClock depsAnc3 = CausalClock.builder(TOPICS).require(T_ANC, 0, 3).build();

            // fast@0 requires anc@3 — held, nothing has genuinely delivered anc yet.
            fastIn.pipeInput(new TestRecord<>("k", "fast0", depsHeader(depsAnc3)));
            assertEquals(0, businessRecords(out).size(),
                    "fast@0 must be held: anc has not genuinely delivered anything yet");

            // anc@0..3 genuinely, contiguously deliver — releases the held fast@0.
            for (long offset = 0; offset <= 3; offset++) {
                ancIn.pipeInput(new TestRecord<>("ak", "anc" + offset, depsHeader(CausalClock.empty())));
            }
            assertTrue(businessRecords(out).stream().anyMatch(r -> "FAST0".equals(r.value())),
                    "fast0 must be released once anc genuinely reaches offset 3");

            // slow@0 requires anc@3 — already genuinely satisfied, delivers immediately.
            slowIn.pipeInput(new TestRecord<>("k", "slow0", depsHeader(depsAnc3)));
            assertTrue(businessRecords(out).stream().anyMatch(r -> "SLOW0".equals(r.value())),
                    "slow@0 must deliver immediately: anc@3 is already genuinely satisfied");

            // fast@1 requires anc@7, higher than anc's current frontier (3) — held again.
            CausalClock depsAnc7 = CausalClock.builder(TOPICS).require(T_ANC, 0, 7).build();
            fastIn.pipeInput(new TestRecord<>("k", "fast1", depsHeader(depsAnc7)));
            assertTrue(businessRecords(out).stream().noneMatch(r -> "FAST1".equals(r.value())),
                    "fast@1 must be held: anc has only genuinely reached 3, not 7");

            // anc@4..7 genuinely deliver — releases fast@1.
            for (long offset = 4; offset <= 7; offset++) {
                ancIn.pipeInput(new TestRecord<>("ak", "anc" + offset, depsHeader(CausalClock.empty())));
            }
            TestRecord<String, String> fast1 = businessRecords(out).stream()
                    .filter(r -> "FAST1".equals(r.value())).findFirst().orElseThrow();
            CausalClock fast1Stamp = CausalClock.fromHeaders(fast1.headers()).orElseThrow();
            assertEquals(7L, fast1Stamp.clock().offsetFor(ANC_ID, 0),
                    "fast1's released stamp must carry anc@7 — the offset anc's own frontier had "
                            + "genuinely reached");
        }
    }

    /**
     * Null message emission — a filter processor that discards all business records still emits a
     * protocol null message for each delivered input.
     *
     * <p>A delegate that calls no {@code ctx.forward()} (a pure filter) causes the decorating
     * {@link ParsleyProcessor} to emit one {@code _parsley_null_message} record per delivered input.
     * The null message carries the current completeness frontier in the
     * {@code parsley-causal-clock} header.
     *
     * <p>This test uses a {@link MockProcessorContext} rather than {@link TopologyTestDriver} so that
     * the null message emitted directly via the outer processor context is captured and inspectable.
     *
     * Asserts the output topic contains a null message record (keyed with the triggering record's key so
     * it co-routes to that record's partition, null value, marker header) and no business value, even
     * though a genuine business record was delivered to the delegate.
     */
    @Test
    void filterProcessorEmitsNullMessageForEveryDeliveredInput() {
        try (TopologyTestDriver driver = new TopologyTestDriver(filterTopology(), testConfig())) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic(C1, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c2", new StringDeserializer(), new StringDeserializer());

            // A record with no deps — admitted immediately; delegate filters it (no forward).
            in.pipeInput(new TestRecord<>("k", "will-be-filtered",
                    depsHeader(CausalClock.empty())));

            // Output must contain exactly one null message and zero business records.
            List<TestRecord<String, String>> allOut = out.readRecordsToList();
            List<TestRecord<String, String>> nullMessages = nullMessageRecords(allOut);
            List<TestRecord<String, String>> business = businessRecords(allOut);

            assertEquals(1, nullMessages.size(),
                    "one null message must be emitted because the filter delegate forwarded nothing");
            assertEquals(0, business.size(),
                    "no business records must appear: the filter dropped the input");
            assertEquals("k", nullMessages.get(0).key(),
                    "null message must reuse the triggering record's key so it co-routes to that partition");
            assertNull(nullMessages.get(0).value(),
                    "null message value must be null");
            assertTrue(hasNullMessageHeader(nullMessages.get(0)),
                    "null message must carry the _parsley_null_message header");
        }
    }

    /**
     * A null message carrying no business record still genuinely advances completeness — visible in a
     * later, unrelated record's own outgoing stamp, not merely in admission.
     *
     * <p>A record's own dependency claim always self-satisfies (see the previous test), so a null message
     * can no longer be demonstrated by "releasing a held record". What remains real and testable: a
     * completely unrelated record — one whose own declared deps say nothing about {@code ANC} at all —
     * still has {@code ANC} appear in its own outgoing stamp, because completeness max-merges every
     * channel's knowledge, including a channel that has only ever received a null message, never a
     * business record.
     *
     * Asserts a record with no ANC dependency of its own still stamps ANC at the level a sibling
     * channel's null message-only advertisement established.
     */
    @Test
    void nullMessageAloneAdvancesCompletenessVisibleInALaterUnrelatedRecordsStamp() {
        try (TopologyTestDriver driver = new TopologyTestDriver(fanInTopology(), testConfig())) {
            TestInputTopic<String, String> fastIn =
                    driver.createInputTopic(T_FAST, new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> slowIn =
                    driver.createInputTopic(T_SLOW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("c2", new StringDeserializer(), new StringDeserializer());

            // The slow branch advertises ANC@10 via a null message alone — no business record ever
            // flows on slow carrying this fact.
            slowIn.pipeInput(nullMessageRecord(ParsleyVectorClock.empty().observe(ANC_ID, 0, 10)));
            assertEquals(0, businessRecords(out).size(), "a null message alone must not itself be a business record");

            // A fast record with empty deps of its own — no ANC claim at all — still delivers, and its
            // own stamp must reflect ANC@10, learned purely from the slow channel's null message.
            fastIn.pipeInput(new TestRecord<>("k", "unrelated", depsHeader(CausalClock.empty())));
            List<TestRecord<String, String>> delivered = businessRecords(out);
            assertEquals(1, delivered.size(), "the unrelated fast record must deliver immediately");
            assertEquals("UNRELATED", delivered.get(0).value(),
                    "the delivered value must be the upper-cased form of the input");

            CausalClock stamp = CausalClock.fromHeaders(
                    delivered.get(0).headers()).orElseThrow();
            assertEquals(10L, stamp.clock().offsetFor(ANC_ID, 0),
                    "the unrelated record's own stamp must carry ANC@10, learned purely from the slow "
                            + "channel's null message, even though its own declared deps never mentioned ANC");
        }
    }

    // sharedAncestorDependencyHeldUntilGenuineWitnessThenStampedWithMax above covers held-then-released
    // reconvergence via the shared ancestor's own channel. A mutual-deadlock construction (two sibling
    // records each depending on the other's not-yet-confirmed claim) is not sound to build: per
    // ParsleyCausalBroadcastCompletenessTest's fanInRecordHeldUntilAGenuineWitnessProvesTheSharedAncestor, using
    // one sibling's own unconfirmed claim as the "witness" for the other is circular, not a genuine
    // reconvergence proof — a real, independent witness (here, anc's own channel) is required instead.

    /**
     * Null-message custody — a non-subscribing layer folds a received null message's carried clock
     * into its own outbound stamp, so a grandchild node's completeness advances with this node's
     * next emission even when no business record flows on the path.
     *
     * <p>This is tested at the {@link ParsleyGossip} level over a channel-tracking {@link
     * ParsleyChannels}: after {@link ParsleyGossip#receive} is called with a frontier carrying an
     * ancestor coordinate, {@link ParsleyCausalBroadcast#completeness()} must reflect it. This proves the
     * channel clock is updated by the null-message receipt, which is the custody mechanism (I9)
     * behind propagation through non-subscribing layers. Also asserts {@link
     * ParsleyGossip.Reception#advancedConsumedChannel()} reports {@code false} — the ancestor is a
     * channel this node neither consumes nor produces, so the claim is custody: it folds into the
     * stamp and rides every later emission, but it must not itself oblige a relay (the I6 trigger
     * scope on {@link ParsleyGossip} — relaying hearsay is what let topic cycles storm).
     *
     * Asserts that completeness rises to include the null message's ancestor coordinate immediately
     * after {@code ParsleyGossip.receive} is called, even though no business record was delivered.
     */
    @Test
    void nullMessageReceiveAdvancesChannelClockAndCompleteness() {
        // C1 is the only subscribed topic. ANC_ID is an out-of-scope ancestor.
        ParsleyVectorClock.CoordinatePredicate scope = (topicId, partition) ->
                partition == 0 && topicId.equals(C1_ID);
        MockBufferStore<String, String> buffer = new MockBufferStore<>();

        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
        ParsleyGossip<String, String> gossip = new ParsleyGossip<>(causalBroadcast, Set.of(), scope);

        // Before any null message, completeness has no ANC coordinate.
        assertEquals(-1L, causalBroadcast.completeness().offsetFor(ANC_ID, 0),
                "completeness must not know ANC before any null message arrives");

        // Receive a null message at C1/0 offset 0, carrying {ANC@5}.
        ParsleyVectorClock carriedFrontier = ParsleyVectorClock.empty().observe(ANC_ID, 0, 5);
        ParsleyGossip.Reception<String, String> reception =
                gossip.receive(C1_ID, 0, 0, carriedFrontier);
        List<ParsleyMessage<String, String>> released = reception.delivered();

        // No records were buffered, so nothing is released.
        assertEquals(0, released.size(),
                "no records buffered, so the null-message receive must return an empty release list");
        assertFalse(reception.advancedConsumedChannel(),
                "ANC is a channel this node neither consumes nor produces — the claim is custody, "
                        + "which folds into the stamp but must not oblige a relay (I6 trigger scope)");

        // The channel clock for C1/0 now knows ANC@5, which must appear in completeness().
        assertEquals(5L, causalBroadcast.completeness().offsetFor(ANC_ID, 0),
                "completeness must rise to ANC@5 after the null message advances the channel clock");
    }

    // --- helpers -----------------------------------------------------------------------------

    private static Properties testConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reconvergence-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Headers depsHeader(CausalClock deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, deps.toBytes());
        return headers;
    }

    /**
     * Builds a null message {@link TestRecord}: null key, null value, carrying the given frontier
     * clock in both the {@code _parsley_null_message} marker header and the
     * {@code parsley-causal-clock} header.
     */
    @SuppressWarnings("NullAway") // nullMessage TestRecord intentionally has null key/value
    private static TestRecord<String, String> nullMessageRecord(ParsleyVectorClock frontier) {
        Headers wm = ParsleyHeader.mutableHeaders();
        wm.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
        wm.add(ParsleyHeader.CAUSAL_CLOCK, frontier.toBytes());
        return new TestRecord<>(null, null, wm);
    }

    private static boolean hasNullMessageHeader(TestRecord<?, ?> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.NULL_MESSAGE.equals(h.key())) {
                return true;
            }
        }
        return false;
    }

    /** Filters the given records to those that are NOT null messages (business records). */
    private static List<TestRecord<String, String>> businessRecords(
            TestOutputTopic<String, String> topic) {
        return businessRecords(topic.readRecordsToList());
    }

    private static List<TestRecord<String, String>> businessRecords(
            List<TestRecord<String, String>> records) {
        List<TestRecord<String, String>> out = new ArrayList<>();
        for (TestRecord<String, String> r : records) {
            if (!hasNullMessageHeader(r)) {
                out.add(r);
            }
        }
        return out;
    }

    private static List<TestRecord<String, String>> nullMessageRecords(
            List<TestRecord<String, String>> records) {
        List<TestRecord<String, String>> out = new ArrayList<>();
        for (TestRecord<String, String> r : records) {
            if (hasNullMessageHeader(r)) {
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
     * causes the decorating {@link ParsleyProcessor} to emit a null message for every delivered input.
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
