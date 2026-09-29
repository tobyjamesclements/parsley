package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the Kafka Streams wiring through {@code TopologyTestDriver}.
 *
 * <p>Covers the header on the wire, byte-exact pass-through of key and value, holding across
 * channels, the identity check at task initialisation, the status a task publishes, and each
 * condition that fails a step.
 */
class TopologyWiringTest {
    private static final UUID IN1_ID = new UUID(100, 1);
    private static final UUID IN2_ID = new UUID(100, 2);
    private static final UUID OUT_ID = new UUID(100, 3);
    private static final Map<String, ResolvedTopic> TOPICS = Map.of(
            "in1", new ResolvedTopic(IN1_ID, 1),
            "in2", new ResolvedTopic(IN2_ID, 1),
            "out", new ResolvedTopic(OUT_ID, 1));
    private static final Channel IN1 = new Channel(IN1_ID, 0);
    private static final Channel IN2 = new Channel(IN2_ID, 0);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private TopologyTestDriver newDriver(Process definition, ScriptedTopicIdentity identity) {
        return newDriver(definition, identity, TOPICS);
    }

    private TopologyTestDriver newDriver(Process definition, ScriptedTopicIdentity identity,
                                         Map<String, ResolvedTopic> topics) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiring-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(
                ProcessTopology.build(definition, topics, identity, Map.of(), Duration.ofMillis(100),
                        ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES), props);
        return driver;
    }

    /**
     * A send carries the delivered message's timestamp unless given one of its own
     * (D15, D111): a message sent long after the one it answers may otherwise carry a
     * timestamp old enough for time-based retention to discard it on the next segment roll.
     */
    @Test
    void anSendInheritsTheDeliveredTimestampUnlessGivenItsOwn() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .send(out, "inherited", delivery.value())
                        .send(out, "own", delivery.value(), 5_000L)
                        .send(out, "own-with-headers", delivery.value(), delivery.headers(), 6_000L)
                        .build())
                .sends(out)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes(), new RecordHeaders(), 1_000L));

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        var first = outTopic.readRecord();
        var second = outTopic.readRecord();
        var third = outTopic.readRecord();
        assertEquals("inherited", new String(first.key()));
        assertEquals(1_000L, first.timestamp(), "a send without a timestamp inherits the delivered one");
        assertEquals("own", new String(second.key()));
        assertEquals(5_000L, second.timestamp(), "a send's own timestamp is the record's");
        assertEquals(6_000L, third.timestamp(), "with headers too");
        assertTrue(outTopic.isEmpty());
    }

    /**
     * A first initialisation asks the identity source about the received topics — the
     * frontier is empty until something is received — and, once answered, nothing runs
     * between deliveries: no punctuation asks again (D115). The frontier half of the
     * question is pinned where a restored frontier exists,
     * {@code ProcessorRevivalTest#everyInitialisationAsksAboutReceivedAndFrontierTopicsAndNothingElseAsks}.
     */
    @Test
    void aFirstInitialisationAsksAboutTheReceivedTopicsAndNoPunctuationAsksAgain() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> in2 = Topic.of("in2", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.none())
                .receives(in2, (delivery, state) -> Effects.none())
                .build();
        ScriptedTopicIdentity identity = new ScriptedTopicIdentity();
        newDriver(definition, identity);
        assertEquals(List.of(Set.of(IN1_ID, IN2_ID)), identity.asked,
                "initialisation asks about the received topics; the frontier is empty on a first start");

        UUID foreignId = new UUID(100, 7);
        var headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY,
                CausesCodec.encode(Causes.of(Map.of(new Channel(foreignId, 0), 5L)))));
        input("in2").pipeInput(new TestRecord<>("k".getBytes(), "b".getBytes(), headers));
        driver.advanceWallClockTime(Duration.ofMillis(500));
        assertEquals(1, identity.asked.size(), "punctuations never ask: nothing is polled between deliveries");
    }

    /**
     * A received topic the identity source reports recreated refuses task initialisation
     * with CHANNEL_IDENTITY_CHANGED (SPEC Assumption 2): records of the new incarnation must
     * never be fed under the old identity, and the refusal surfaces the way every
     * initialisation refusal does — the task fails to initialise, and the stop carries the
     * reason.
     */
    @Test
    void aRecreatedReceivedTopicRefusesTaskInitialisation() {
        Process definition = twoInputRecorder(new ArrayList<>());
        ScriptedTopicIdentity identity = new ScriptedTopicIdentity();
        identity.verdicts = new TopicIdentityVerdicts(Set.of(), Set.of(IN1_ID));
        Throwable thrown = assertThrows(Throwable.class, () -> newDriver(definition, identity),
                "a recreated received topic must refuse initialisation");
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.CHANNEL_IDENTITY_CHANGED),
                () -> "expected CHANNEL_IDENTITY_CHANGED in " + thrown);
    }

    private TestInputTopic<byte[], byte[]> input(String topic) {
        return driver.createInputTopic(topic, new ByteArraySerializer(), new ByteArraySerializer());
    }

    /** Forwarding received headers on a send works. */
    @Test
    void forwardingReceivedHeadersOnASendWorks() throws Exception {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value(), delivery.headers()).build())
                .sends(out)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new org.apache.kafka.common.header.internals.RecordHeader("app.trace", new byte[] {7}));
        headers.add(new org.apache.kafka.common.header.internals.RecordHeader(CausesCodec.HEADER_KEY,
                CausesCodec.encode(Causes.of(Map.of(IN2, 0L)))));
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes(), headers));

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TestRecord<byte[], byte[]> record = outTopic.readRecord();
        assertNotNull(record.headers().lastHeader("app.trace"), "application headers forward");
        Causes stamped = CausesCodec.decode(record.headers().lastHeader(CausesCodec.HEADER_KEY).value());
        assertEquals(Map.of(IN1, 0L, IN2, 0L), stamped.byChannel(),
                "the send's causal metadata is parsley's own stamp, not the forwarded copy");
    }

    /** Undecodable application payload fails the step. */
    @Test
    void undecodableApplicationPayloadFailsTheStep() {
        org.apache.kafka.common.serialization.Serde<String> poison =
                org.apache.kafka.common.serialization.Serdes.serdeFrom(
                        new StringSerializer(),
                        (topic, data) -> {
                            throw new RuntimeException("schema mismatch");
                        });
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), poison);
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.none())
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        var thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        FailClosedException refusal =
                FailClosedException.findIn(thrown);
        assertNotNull(refusal, "the step must fail closed, not skip the message");
        assertEquals(FailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                refusal.reason());
    }

    /** Serializer writing into the reserved header namespace fails the step at the sender. */
    @Test
    void serializerSmugglingAReservedHeaderFailsTheStep() {
        org.apache.kafka.common.serialization.Serializer<String> smuggler = new StringSerializer() {
            @Override
            public byte[] serialize(String topic, org.apache.kafka.common.header.Headers headers, String data) {
                headers.add(new org.apache.kafka.common.header.internals.RecordHeader(
                        "parsley.smuggled", new byte[] {1}));
                return serialize(topic, data);
            }
        };
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(),
                Serdes.serdeFrom(smuggler, new org.apache.kafka.common.serialization.StringDeserializer()));
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value()).build())
                .sends(out)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.RESERVED_HEADER_USED),
                () -> "expected RESERVED_HEADER_USED in " + thrown);
    }

    /** Key value bytes pass through untouched and causes ride a header. */
    @Test
    void keyValueBytesPassThroughUntouchedAndCausesRideAHeader() throws Exception {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value() + "!").build())
                .sends(out)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        byte[] keyBytes = new StringSerializer().serialize("in1", "k1");
        byte[] valueBytes = new StringSerializer().serialize("in1", "v1");
        input("in1").pipeInput(new TestRecord<>(keyBytes, valueBytes));

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TestRecord<byte[], byte[]> record = outTopic.readRecord();

        assertArrayEquals(new StringSerializer().serialize("out", "k1"), record.key());
        assertArrayEquals(new StringSerializer().serialize("out", "v1!"), record.value());

        Causes causes = CausesCodec.decode(record.headers().lastHeader(CausesCodec.HEADER_KEY).value());
        assertEquals(Causes.of(Map.of(IN1, 0L)), causes);
    }

    /** Effect arriving before its cause is held across channels. */
    @Test
    void effectArrivingBeforeItsCauseIsHeldAcrossChannels() {
        List<String> delivered = new ArrayList<>();
        Process definition = twoInputRecorder(delivered);
        newDriver(definition, new ScriptedTopicIdentity());

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 0L)))));
        input("in2").pipeInput(new TestRecord<>("B".getBytes(), "B".getBytes(), headers));
        assertEquals(List.of(), delivered, "the effect must be held until its cause is delivered");

        input("in1").pipeInput(new TestRecord<>("A".getBytes(), "A".getBytes()));
        assertEquals(List.of("A", "B"), delivered);
    }

    /**
     * A cause naming a position no record has reached is held and visible, and nothing but
     * a record at or past that position releases it (D115): the punctuation neither
     * settles nor delivers on its own, however many times it runs. The hand-built header
     * here is exactly what an out-of-contract stamper produces (wire-format constraint 8);
     * once records reach in1@5 the hold goes, because receipt of a position asserts
     * everything below it was fed or never will be.
     */
    @Test
    void aCauseNamingAnUnreceivedPositionIsHeldAndVisibleUntilARecordReachesIt() {
        List<String> delivered = new ArrayList<>();
        Process definition = twoInputRecorder(delivered);
        ScriptedTopicIdentity identity = new ScriptedTopicIdentity();
        newDriver(definition, identity, TOPICS);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 5L)))));
        input("in2").pipeInput(new TestRecord<>("B".getBytes(), "B".getBytes(), headers));
        for (int tick = 0; tick < 5; tick++) {
            driver.advanceWallClockTime(Duration.ofMillis(200));
        }
        assertEquals(List.of(), delivered, "no record has reached in1@5: time settles nothing");

        for (int offset = 0; offset <= 5; offset++) {
            input("in1").pipeInput(new TestRecord<>("k".getBytes(), ("a" + offset).getBytes()));
        }
        assertEquals(List.of("a0", "a1", "a2", "a3", "a4", "a5", "B"), delivered,
                "the record at in1@5 releases the hold, after itself");
    }

    /** Undecodable metadata fails the step. */
    @Test
    void undecodableMetadataFailsTheStep() {
        Process definition = twoInputRecorder(new ArrayList<>());
        newDriver(definition, new ScriptedTopicIdentity());

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, new byte[] {42, 42}));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("g".getBytes(), "g".getBytes(), headers)));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.UNDECODABLE_METADATA),
                () -> "expected UNDECODABLE_METADATA in " + thrown);
    }

    /** Send to undeclared channel fails the step. */
    @Test
    void emissionToUndeclaredChannelFailsTheStep() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> undeclared = Topic.of("out", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(undeclared, "k", "v").build())
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.SEND_TO_UNDECLARED_TOPIC),
                () -> "expected SEND_TO_UNDECLARED_TOPIC in " + thrown);
    }

    /** A look-alike send serializes with the declared channel's serdes. */
    @Test
    void lookAlikeSendSerializesWithTheDeclaredSerdes() {
        org.apache.kafka.common.serialization.Serializer<String> shouting = new StringSerializer() {
            @Override
            public byte[] serialize(String topic, String data) {
                return data == null ? null : data.toUpperCase(java.util.Locale.ROOT)
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> declared = Topic.of("out", Serdes.String(),
                Serdes.serdeFrom(shouting, new org.apache.kafka.common.serialization.StringDeserializer()));
        Topic<String, String> lookAlike = Topic.of("out", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(lookAlike, "k", "v").build())
                .sends(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("V".getBytes(), outTopic.readRecord().value(),
                "the send seam resolves the declared channel by name and its serdes produce the"
                        + " bytes, so a second Topic instance for a declared topic has no serdes"
                        + " to smuggle past sends(...)");
    }

    /** A send through a factory-built equal channel instance is sent, not refused. */
    @Test
    void emissionThroughAFactoryBuiltChannelInstanceIsSent() {
        java.util.function.Supplier<Topic<String, String>> outChannel =
                () -> Topic.of("out", Serdes.String(), Serdes.String());
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(outChannel.get(), delivery.key(), delivery.value()).build())
                .sends(outChannel.get())
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("v".getBytes(), outTopic.readRecord().value(),
                "a Topic.of factory called at both sends(...) and send(...) names the same"
                        + " declared topic; a send on a declared topic must be sent"
                        + " (SPEC Structural 19)");
    }

    /** A self-loop re-sending via the delivered channel instance is sent, not refused. */
    @Test
    void selfLoopResendViaTheDeliveredChannelInstanceIsSent() {
        Topic<String, String> loop = Topic.of("loop", Serdes.String(), Serdes.String());
        List<String> delivered = new ArrayList<>();
        Process definition = Process.named("p")
                .receives(loop.startingAt(Topic.InitialPosition.LATEST), (delivery, state) -> {
                    delivered.add(delivery.value());
                    return "seed".equals(delivery.value())
                            ? Effects.builder().send(delivery.topic(), delivery.key(), "echo").build()
                            : Effects.none();
                })
                .sends(loop)
                .build();
        newDriver(definition, new ScriptedTopicIdentity(), Map.of("loop", new ResolvedTopic(new UUID(100, 9), 1)));

        input("loop").pipeInput(new TestRecord<>("k".getBytes(), "seed".getBytes()));
        assertEquals(List.of("seed", "echo"), delivered,
                "receives(channel.startingAt(...)) and sends(channel) are distinct instances of"
                        + " one declared topic, so a handler re-sending via delivery.topic()"
                        + " must be sent, not refused");
    }

    /** A type-mismatched look-alike send fails closed before any write applies. */
    @Test
    void typeMismatchedLookAlikeSendFailsClosedBeforeAnyWriteApplies() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, Long> declared = Topic.of("out", Serdes.String(), Serdes.Long());
        Topic<String, String> lookAlike = Topic.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(store, "k", "v")
                        .send(lookAlike, "k", "v")
                        .build())
                .sends(declared)
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE),
                () -> "the declared Long serializer cannot serialize the look-alike's String;"
                        + " the stop must carry its own reason, not a bare ClassCastException; got " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "sends are serialized in the planning phase, so a serialization refusal"
                            + " must fire before any state write reaches the store");
        }
    }

    /** A swallowed undeclared-store read still fails the step. */
    @Test
    void swallowedUndeclaredStoreReadStillFailsTheStep() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    try {
                        state.get(lookAlike, "k");
                    } catch (RuntimeException swallowed) {
                        // an application fallback path: the refusal must not be swallowable
                    }
                    return Effects.builder().put(declared, "k", "v").build();
                })
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "the reader latches its refusal and deliver() rethrows after the handler"
                        + " returns, so a catch inside the handler cannot commit the step; got " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(), "no effect of the refused step may apply");
        }
    }

    /**
     * The delivered payload's own deserializers are application code running before the
     * handler: one holding a reader captured on an earlier delivery must not have its
     * latched refusal erased by a frame reset. The step fails even when the deserializer
     * swallows the thrown exception (D87) — previously the reset ran after the
     * deserializers and silently discarded exactly this refusal.
     */
    @Test
    void deserializerLatchedRefusalFailsTheStepEvenWhenSwallowed() {
        java.util.concurrent.atomic.AtomicReference<State> captured =
                new java.util.concurrent.atomic.AtomicReference<>();
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> capturingSerde = Serdes.serdeFrom(
                new StringSerializer(), (topic, data) -> {
                    State reader = captured.get();
                    if (reader != null) {
                        try {
                            reader.get(lookAlike, "k");
                        } catch (RuntimeException swallowed) {
                            // an application fallback path: the refusal must not be swallowable
                        }
                    }
                    return new String(data, java.nio.charset.StandardCharsets.UTF_8);
                });
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), capturingSerde);
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    captured.set(state);
                    return Effects.none();
                })
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "first".getBytes()));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "second".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "a refusal latched by the delivered payload's deserializer must fail the step, not be"
                        + " erased before the handler runs; got " + thrown);
    }

    /**
     * A reader refusal that propagates out of a deserializer unswallowed keeps its own
     * reason: relabeling it as an undecodable payload would point the operator at codecs
     * when the condition is an undeclared-store access (D88).
     */
    @Test
    void unswallowedReaderRefusalInADeserializerKeepsItsReason() {
        java.util.concurrent.atomic.AtomicReference<State> captured =
                new java.util.concurrent.atomic.AtomicReference<>();
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> readingSerde = Serdes.serdeFrom(
                new StringSerializer(), (topic, data) -> {
                    State reader = captured.get();
                    if (reader != null) {
                        reader.get(lookAlike, "k");
                    }
                    return new String(data, java.nio.charset.StandardCharsets.UTF_8);
                });
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), readingSerde);
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    captured.set(state);
                    return Effects.none();
                })
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "first".getBytes()));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "second".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "the reader's refusal carries its own reason; got " + thrown);
        assertFalse(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE),
                () -> "the stop must not be relabeled as a payload-codec failure; got " + thrown);
    }

    /**
     * Application code can also run after the post-handler check: a value serializer
     * invoked during effect planning may hold the reader. Its latched refusal fails the
     * step at the post-apply recheck — the guard the processor promises for every
     * fail-closed event, here pinned rather than asserted in a comment.
     */
    @Test
    void serializerLatchedRefusalDuringPlanningFailsTheStep() {
        java.util.concurrent.atomic.AtomicReference<State> captured =
                new java.util.concurrent.atomic.AtomicReference<>();
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> readingSerializerSerde = Serdes.serdeFrom(
                (topic, data) -> {
                    try {
                        captured.get().get(lookAlike, "k");
                    } catch (RuntimeException swallowed) {
                        // an application fallback path: the refusal must not be swallowable
                    }
                    return data.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }, Serdes.String().deserializer());
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), readingSerializerSerde);
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    captured.set(state);
                    return Effects.builder().send(out, "k", "v").build();
                })
                .sends(out)
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "a refusal latched during planning must fail the step at the post-apply recheck;"
                        + " got " + thrown);
    }

    /**
     * The read seam hides parsley's transport header from application deserializers — the
     * mirror of the write seam serializing before the stamp goes on (D56): a header-aware
     * codec sees the same header set on both sides of the wire, and application logic has
     * no way to observe the causal metadata.
     */
    @Test
    void reservedTransportHeaderIsInvisibleToApplicationDeserializers() {
        List<String> seenKeys = new ArrayList<>();
        org.apache.kafka.common.serialization.Deserializer<String> headerAware =
                new org.apache.kafka.common.serialization.Deserializer<>() {
                    @Override
                    public String deserialize(String topic, byte[] data) {
                        return new String(data, java.nio.charset.StandardCharsets.UTF_8);
                    }

                    @Override
                    public String deserialize(String topic, org.apache.kafka.common.header.Headers headers,
                                              byte[] data) {
                        for (org.apache.kafka.common.header.Header header : headers) {
                            seenKeys.add(header.key());
                        }
                        return deserialize(topic, data);
                    }
                };
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(),
                Serdes.serdeFrom(new StringSerializer(), headerAware));
        List<String> delivered = new ArrayList<>();
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    delivered.add(delivery.value());
                    return Effects.none();
                })
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        var headers = new RecordHeaders();
        headers.add(new RecordHeader("app.trace", new byte[] {7}));
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.none())));
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes(), headers));

        assertEquals(List.of("v"), delivered, "the message delivers");
        assertTrue(seenKeys.contains("app.trace"), "application headers reach the deserializer");
        assertTrue(seenKeys.stream().noneMatch(key -> key.startsWith(CausesCodec.RESERVED_HEADER_PREFIX)),
                () -> "the reserved transport header must be invisible to application deserializers;"
                        + " saw " + seenKeys);
    }

    /**
     * A handler returning null is a seam-contract breach parsley itself detects, and it
     * recurs identically on every restart, so it must carry its own refusal reason —
     * status() reporting a stop with an empty refusalReason would misread as transient.
     */
    @Test
    void nullEffectsFromAHandlerFailClosedWithTheirOwnReason() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> null)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.HANDLER_RETURNED_NULL_EFFECTS),
                () -> "a deliberate refusal that recurs on restart must name its reason; got " + thrown);
    }

    /**
     * A stored value the declared serde can no longer decode is the state-read shape of
     * D13's payload rule: it fails the step with its own reason, and an application catch
     * around the read cannot swallow it — the reader latches the refusal.
     */
    @Test
    void undecodableStoredStateValueFailsClosedEvenWhenSwallowed() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> poisonRead =
                Serdes.serdeFrom(new StringSerializer(), (topic, data) -> {
                    throw new RuntimeException("schema moved on");
                });
        Store<String, String> store = Store.of("app-store", Serdes.String(), poisonRead);
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    if (delivery.value().equals("first")) {
                        return Effects.builder().put(store, "k", "v").build();
                    }
                    try {
                        state.get(store, "k");
                    } catch (RuntimeException swallowed) {
                        // an application fallback path: the refusal must not be swallowable
                    }
                    return Effects.none();
                })
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "first".getBytes()));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "second".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE),
                () -> "a stored value the declared serde cannot decode must fail the step with its"
                        + " reason, latched past any application catch; got " + thrown);
    }

    /**
     * The Serializer contract permits signalling failure by returning null; on a state
     * read that must not surface as the store's bare NPE inside the handler's frame,
     * where an application catch could swallow it and commit the step.
     */
    @Test
    void nullReturningKeySerializerOnAStateReadFailsClosedEvenWhenSwallowed() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> nullKeySerde =
                Serdes.serdeFrom((topic, data) -> null, Serdes.String().deserializer());
        Store<String, String> store = Store.of("app-store", nullKeySerde, Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    try {
                        state.get(store, "k");
                    } catch (RuntimeException swallowed) {
                        // an application fallback path: the refusal must not be swallowable
                    }
                    return Effects.none();
                })
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE),
                () -> "a key serialized to null cannot address a store entry and must fail the"
                        + " step with its reason, latched past any application catch; got " + thrown);
    }

    /**
     * The write-side twin of the null-returning read key above: the Serializer contract
     * permits signalling failure by returning null, and a null store key cannot address an
     * entry, so {@code planWrite} must fail the plan with its own reason (D81's taxonomy)
     * before any write applies — not surface as the store's bare NPE during apply, after a
     * sibling write already landed. The null-KEY-at-Effects-construction refusal is a
     * different site: this key is non-null and the declared serde encodes it to null.
     */
    @Test
    void nullReturningKeySerializerOnAWriteFailsThePlanBeforeAnyWriteApplies() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> nullOnPoison =
                Serdes.serdeFrom((topic, data) -> "poison".equals(data) ? null
                        : data.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        Serdes.String().deserializer());
        Store<String, String> store = Store.of("app-store", nullOnPoison, Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(store, "good", "v")
                        .put(store, "poison", "v")
                        .build())
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE),
                () -> "a write key serialized to null cannot address a store entry and must fail"
                        + " the plan with its reason, not as the store's bare NPE; got " + thrown);
        FailClosedException refusal = FailClosedException.findIn(thrown);
        assertTrue(refusal.getMessage().contains("state write key serialized to null"),
                () -> "the refusal names the write-key shape, not a generic payload failure: "
                        + refusal.getMessage());
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "writes are planned before any is applied, so the declared write ahead of"
                            + " the refused one must not reach the store");
        }
    }

    /**
     * The read-key serializer that throws, as opposed to returning null (pinned two tests
     * up): the reader must wrap the failure with its own reason and latch it before
     * throwing, so a handler that catches and swallows its read's failure cannot commit
     * the step (D87's latch-past-catch promise, D81's taxonomy). Without the wrap the bare
     * RuntimeException lands in the application catch, unlatched, and the step commits
     * with its refused read swallowed.
     */
    @Test
    void throwingKeySerializerOnAStateReadFailsClosedEvenWhenSwallowed() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> throwingKeySerde =
                Serdes.serdeFrom((topic, data) -> {
                    throw new RuntimeException("key schema mismatch");
                }, Serdes.String().deserializer());
        Store<String, String> store = Store.of("app-store", throwingKeySerde, Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    try {
                        state.get(store, "k");
                    } catch (RuntimeException swallowed) {
                        // an application fallback path: the refusal must not be swallowable
                    }
                    return Effects.none();
                })
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE),
                () -> "a read key the declared serde cannot serialize must fail the step with its"
                        + " reason, latched past any application catch; got " + thrown);
        FailClosedException refusal = FailClosedException.findIn(thrown);
        assertTrue(refusal.getMessage().contains("state read key could not be serialized"),
                () -> "the refusal names the throwing-read-key site: " + refusal.getMessage());
    }

    /**
     * A record arriving from a topic this task holds no channel for must be refused
     * naming the process and topic, not feed the engine a null channel whose NPE
     * diagnoses nothing. Staged through the width arm of init's channel map: channels
     * exist only for task partitions below the resolved width, so the driver's task 0
     * against a zero-width ResolvedTopic exercises the same predicate as a task numbered at
     * or above a received topic's real width.
     */
    @Test
    void recordFromATopicWithoutAChannelForThisTaskIsRefusedWithTheDiagnosis() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.none())
                .build();
        newDriver(definition, new ScriptedTopicIdentity(), Map.of("in1", new ResolvedTopic(IN1_ID, 0)));

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(TestChains.chainContains(thrown, IllegalStateException.class, "p fed from undeclared topic in1"),
                () -> "a record with no channel for this task must be refused naming the"
                        + " process and the topic; got " + thrown);
    }

    /** A null store on a state read is refused with a message. */
    @Test
    void nullStoreOnAStateReadIsRefusedWithAMessage() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(null, "k");
                    return Effects.none();
                })
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(TestChains.chainContains(thrown, IllegalArgumentException.class, "store must be non-null"),
                () -> "a null store must be refused per the taxonomy, not surface as a bare NPE"
                        + " from store.name(); got " + thrown);
    }

    /** A null key on a state read is refused with a message. */
    @Test
    void nullKeyOnAStateReadIsRefusedWithAMessage() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(store, null);
                    return Effects.none();
                })
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(TestChains.chainContains(thrown, IllegalArgumentException.class, "state read key must be non-null"),
                () -> "a null read key must be refused like a null write key, not reach the"
                        + " state backend as null bytes; got " + thrown);
    }

    /** A state write ahead of a refused send is not applied. */
    @Test
    void writeAheadOfARefusedSendIsNotApplied() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> undeclared = Topic.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(store, "k", "v")
                        .send(undeclared, "k", "v")
                        .build())
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.SEND_TO_UNDECLARED_TOPIC),
                () -> "expected SEND_TO_UNDECLARED_TOPIC in " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "every effect target is validated before any write is applied, so a refused"
                            + " step must not leave earlier writes relying on the EOS abort alone");
        }
    }

    /** A state write to an undeclared store fails closed before any write applies. */
    @Test
    void stateWriteToAnUndeclaredStoreFailsClosedBeforeAnyWriteApplies() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(declared, "k", "v")
                        .put(lookAlike, "k2", "v2")
                        .build())
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "the store seam matches by identity and refuses with its own reason, so"
                        + " status() can report the refusal; got " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "the declared write ahead of the refused one must not be applied");
        }
    }

    /** A state read from an undeclared store fails closed with its own reason. */
    @Test
    void stateReadFromAnUndeclaredStoreFailsClosedWithItsOwnReason() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(lookAlike, "k");
                    return Effects.none();
                })
                .stores(declared)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "a read through a Store instance other than the declared one would smuggle"
                        + " a differently-typed codec into the application's frame; got " + thrown);
    }

    /** Application state reads see earlier writes and tombstones pass through. */
    @Test
    void applicationStateReadsSeeEarlierWritesAndTombstonesPassThrough() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    String seen = state.get(store, "count");
                    String next = seen == null ? "1" : String.valueOf(Integer.parseInt(seen) + 1);
                    return Effects.builder()
                            .put(store, "count", next)
                            .send(out, delivery.key(), delivery.value() == null ? null : next)
                            .build();
                })
                .sends(out)
                .stores(store)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "x".getBytes()));
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "y".getBytes()));
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), (byte[]) null));

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("1".getBytes(), outTopic.readRecord().value());
        assertArrayEquals("2".getBytes(), outTopic.readRecord().value(), "reads must see earlier committed writes");
        assertNull(outTopic.readRecord().value(), "no value required where the application sent none");
    }

    /** Stamped causes relay across processes and compress. */
    @Test
    void stampedCausesRelayAcrossProcessesAndCompress() throws Exception {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Process upstream = Process.named("up")
                .receives(in1, (delivery, state) -> Effects.builder().send(out, "k", "v").build())
                .sends(out)
                .build();
        newDriver(upstream, new ScriptedTopicIdentity());
        input("in1").pipeInput(new TestRecord<>("a".getBytes(), "a".getBytes()));
        input("in1").pipeInput(new TestRecord<>("b".getBytes(), "b".getBytes()));
        TestOutputTopic<byte[], byte[]> outTopic = driver.createOutputTopic(
                "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TestRecord<byte[], byte[]> first = outTopic.readRecord();
        TestRecord<byte[], byte[]> second = outTopic.readRecord();

        assertEquals(Causes.of(Map.of(IN1, 0L)),
                CausesCodec.decode(first.headers().lastHeader(CausesCodec.HEADER_KEY).value()));
        assertEquals(Causes.of(Map.of(IN1, 1L)),
                CausesCodec.decode(second.headers().lastHeader(CausesCodec.HEADER_KEY).value()),
                "two causes on one channel compress to the single greater position");
    }

    /** Self channel topology is accepted. */
    @Test
    void selfChannelTopologyIsAccepted() {
        Topic<String, String> loop = Topic.of("loop", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(loop, (delivery, state) -> delivery.value().length() < 3
                        ? Effects.builder().send(loop, delivery.key(), delivery.value() + "x").build()
                        : Effects.none())
                .sends(loop)
                .build();
        newDriver(definition, new ScriptedTopicIdentity(), Map.of("loop", new ResolvedTopic(new UUID(100, 9), 1)));

        input("loop").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> out =
                driver.createOutputTopic("loop", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("vx".getBytes(), out.readRecord().value());
    }

    /** Several send channels and several stores wire independently. */
    @Test
    void severalSendChannelsAndSeveralStoresWireIndependently() {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("out", Serdes.String(), Serdes.String());
        Topic<String, String> out2 = Topic.of("in2", Serdes.String(), Serdes.String());
        Store<String, String> storeA = Store.of("store-a", Serdes.String(), Serdes.String());
        Store<String, String> storeB = Store.of("store-b", Serdes.String(), Serdes.String());
        Process definition = Process.named("p")
                .receives(in1, (delivery, state) -> {
                    String a = state.get(storeA, "k");
                    String b = state.get(storeB, "k");
                    return Effects.builder()
                            .put(storeA, "k", a == null ? "a" : a + "a")
                            .put(storeB, "k", b == null ? "b" : b + "b")
                            .send(out, delivery.key(), state.get(storeA, "k") == null ? "first" : "later")
                            .send(out2, delivery.key(), delivery.value())
                            .build();
                })
                .sends(out, out2)
                .stores(storeA, storeB)
                .build();
        newDriver(definition, new ScriptedTopicIdentity());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v1".getBytes()));
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v2".getBytes()));

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TestOutputTopic<byte[], byte[]> out2Topic =
                driver.createOutputTopic("in2", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("first".getBytes(), outTopic.readRecord().value());
        assertArrayEquals("later".getBytes(), outTopic.readRecord().value(), "store-a writes must persist per store");
        assertArrayEquals("v1".getBytes(), out2Topic.readRecord().value());
        assertArrayEquals("v2".getBytes(), out2Topic.readRecord().value());
    }

    private static Process twoInputRecorder(List<String> delivered) {
        Topic<String, String> in1 = Topic.of("in1", Serdes.String(), Serdes.String());
        Topic<String, String> in2 = Topic.of("in2", Serdes.String(), Serdes.String());
        return Process.named("p")
                .receives(in1, (delivery, state) -> {
                    delivered.add(delivery.value());
                    return Effects.none();
                })
                .receives(in2, (delivery, state) -> {
                    delivered.add(delivery.value());
                    return Effects.none();
                })
                .build();
    }

    private static boolean causeChainContains(Throwable thrown, FailClosedException.Reason reason) {
        FailClosedException found = FailClosedException.findIn(thrown);
        return found != null && found.reason() == reason;
    }
}
