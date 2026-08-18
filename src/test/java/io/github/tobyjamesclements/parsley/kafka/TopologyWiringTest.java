package io.github.tobyjamesclements.parsley.kafka;

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

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

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
 * channels, fact ingestion, and each condition that fails a step.
 */
class TopologyWiringTest {
    private static final UUID IN1_ID = new UUID(100, 1);
    private static final UUID IN2_ID = new UUID(100, 2);
    private static final UUID OUT_ID = new UUID(100, 3);
    private static final Map<String, TopicInfo> TOPICS = Map.of(
            "in1", new TopicInfo(IN1_ID, 1),
            "in2", new TopicInfo(IN2_ID, 1),
            "out", new TopicInfo(OUT_ID, 1));
    private static final ChannelId IN1 = new ChannelId(IN1_ID, 0);
    private static final ChannelId IN2 = new ChannelId(IN2_ID, 0);

    static final class FakeFacts implements FactsSource {
        volatile PositionFacts facts = PositionFacts.EMPTY;

        @Override
        public PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                                    Set<ChannelId> frontierChannels) {
            return facts;
        }
    }

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private TopologyTestDriver newDriver(ProcessDefinition definition, FakeFacts facts) {
        return newDriver(definition, facts, TOPICS);
    }

    private TopologyTestDriver newDriver(ProcessDefinition definition, FakeFacts facts, Map<String, TopicInfo> topics) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiring-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(
                ProcessTopology.build(definition, topics, facts, Duration.ofMillis(100)), props);
        return driver;
    }

    private TestInputTopic<byte[], byte[]> input(String topic) {
        return driver.createInputTopic(topic, new ByteArraySerializer(), new ByteArraySerializer());
    }

    /** Forwarding received headers on an emission works. */
    @Test
    void forwardingReceivedHeadersOnAnEmissionWorks() throws Exception {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value(), delivery.headers()).build())
                .sends(out)
                .build();
        newDriver(definition, new FakeFacts());

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
                "the emission's causal metadata is parsley's own stamp, not the forwarded copy");
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
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), poison);
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> Effects.none())
                .build();
        newDriver(definition, new FakeFacts());

        var thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException refusal =
                io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.findIn(thrown);
        assertNotNull(refusal, "the step must fail closed, not skip the message");
        assertEquals(io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
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
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(),
                Serdes.serdeFrom(smuggler, new org.apache.kafka.common.serialization.StringDeserializer()));
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value()).build())
                .sends(out)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.RESERVED_HEADER_USED),
                () -> "expected RESERVED_HEADER_USED in " + thrown);
    }

    /** Key value bytes pass through untouched and causes ride a header. */
    @Test
    void keyValueBytesPassThroughUntouchedAndCausesRideAHeader() throws Exception {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(out, delivery.key(), delivery.value() + "!").build())
                .sends(out)
                .build();
        newDriver(definition, new FakeFacts());

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
        ProcessDefinition definition = twoInputRecorder(delivered);
        newDriver(definition, new FakeFacts());

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 0L)))));
        input("in2").pipeInput(new TestRecord<>("B".getBytes(), "B".getBytes(), headers));
        assertEquals(List.of(), delivered, "the effect must be held until its cause is delivered");

        input("in1").pipeInput(new TestRecord<>("A".getBytes(), "A".getBytes()));
        assertEquals(List.of("A", "B"), delivered);
    }

    /** Punctuator report ingestion frees messages whose cause never arrives. */
    @Test
    void punctuatorReportIngestionFreesMessagesWhoseCauseNeverArrives() {
        List<String> delivered = new ArrayList<>();
        ProcessDefinition definition = twoInputRecorder(delivered);
        FakeFacts facts = new FakeFacts();
        newDriver(definition, facts);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 5L)))));
        input("in2").pipeInput(new TestRecord<>("B".getBytes(), "B".getBytes(), headers));
        driver.advanceWallClockTime(Duration.ofMillis(200));
        assertEquals(List.of(), delivered, "positions 0..5 of in1 are not yet known to be empty");

        facts.facts = new PositionFacts(Map.of(IN1, 6L), Map.of(), Set.of());
        driver.advanceWallClockTime(Duration.ofMillis(200));
        assertEquals(List.of("B"), delivered, "the report, not a message and not time, frees the hold");
    }

    /** Undecodable metadata fails the step. */
    @Test
    void undecodableMetadataFailsTheStep() {
        ProcessDefinition definition = twoInputRecorder(new ArrayList<>());
        newDriver(definition, new FakeFacts());

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, new byte[] {42, 42}));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("g".getBytes(), "g".getBytes(), headers)));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.UNDECODABLE_METADATA),
                () -> "expected UNDECODABLE_METADATA in " + thrown);
    }

    /** Emission to undeclared channel fails the step. */
    @Test
    void emissionToUndeclaredChannelFailsTheStep() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> undeclared = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(undeclared, "k", "v").build())
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL),
                () -> "expected EMISSION_TO_UNDECLARED_CHANNEL in " + thrown);
    }

    /** The composed changelog name is bounded at exactly Kafka's limit. */
    @Test
    void composedChangelogNameIsBoundedAtExactlyKafkasLimit() {
        String applicationId = "app";
        // applicationId + "-" + store + "-changelog" == 249 characters exactly.
        String storeAtLimit = "s".repeat(249 - applicationId.length() - 1 - "-changelog".length());
        assertEquals(249, ProcessTopology.changelogName(applicationId, storeAtLimit).length(),
                "a composite at exactly 249 characters is Kafka-legal and must compose");
        assertThrows(IllegalArgumentException.class,
                () -> ProcessTopology.changelogName(applicationId, storeAtLimit + "s"),
                "one character past Kafka's limit must refuse; this is the kafka-side boundary"
                        + " pin that keeps ProcessTopology's mirrored limit agreeing with"
                        + " KafkaNames' declaration-site limit, which"
                        + " ApiValidationTest#channelTopicAtExactlyTheLengthLimitIsAccepted pins"
                        + " at the same boundary");
    }

    /** A look-alike emission serializes with the declared channel's serdes. */
    @Test
    void lookAlikeEmissionSerializesWithTheDeclaredSerdes() {
        org.apache.kafka.common.serialization.Serializer<String> shouting = new StringSerializer() {
            @Override
            public byte[] serialize(String topic, String data) {
                return data == null ? null : data.toUpperCase(java.util.Locale.ROOT)
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> declared = Channel.of("out", Serdes.String(),
                Serdes.serdeFrom(shouting, new org.apache.kafka.common.serialization.StringDeserializer()));
        Channel<String, String> lookAlike = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(lookAlike, "k", "v").build())
                .sends(declared)
                .build();
        newDriver(definition, new FakeFacts());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("V".getBytes(), outTopic.readRecord().value(),
                "the send seam resolves the declared channel by name and its serdes produce the"
                        + " bytes, so a second Channel instance for a declared topic has no serdes"
                        + " to smuggle past sends(...)");
    }

    /** An emission through a factory-built equal channel instance is sent, not refused. */
    @Test
    void emissionThroughAFactoryBuiltChannelInstanceIsSent() {
        java.util.function.Supplier<Channel<String, String>> outChannel =
                () -> Channel.of("out", Serdes.String(), Serdes.String());
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(outChannel.get(), delivery.key(), delivery.value()).build())
                .sends(outChannel.get())
                .build();
        newDriver(definition, new FakeFacts());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("v".getBytes(), outTopic.readRecord().value(),
                "a Channel.of factory called at both sends(...) and send(...) names the same"
                        + " declared topic; an emission on a declared topic must be sent"
                        + " (SPEC Structural 19)");
    }

    /** A self-loop re-emitting via the delivered channel instance is sent, not refused. */
    @Test
    void selfLoopReEmissionViaTheDeliveredChannelInstanceIsSent() {
        Channel<String, String> loop = Channel.of("loop", Serdes.String(), Serdes.String());
        List<String> delivered = new ArrayList<>();
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(loop.startingAt(Channel.InitialPosition.LATEST), (delivery, state) -> {
                    delivered.add(delivery.value());
                    return "seed".equals(delivery.value())
                            ? Effects.builder().send(delivery.channel(), delivery.key(), "echo").build()
                            : Effects.none();
                })
                .sends(loop)
                .build();
        newDriver(definition, new FakeFacts(), Map.of("loop", new TopicInfo(new UUID(100, 9), 1)));

        input("loop").pipeInput(new TestRecord<>("k".getBytes(), "seed".getBytes()));
        assertEquals(List.of("seed", "echo"), delivered,
                "receives(channel.startingAt(...)) and sends(channel) are distinct instances of"
                        + " one declared topic, so a handler re-emitting via delivery.channel()"
                        + " must be sent, not refused");
    }

    /** A type-mismatched look-alike emission fails closed before any write applies. */
    @Test
    void typeMismatchedLookAlikeEmissionFailsClosedBeforeAnyWriteApplies() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, Long> declared = Channel.of("out", Serdes.String(), Serdes.Long());
        Channel<String, String> lookAlike = Channel.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(store, "k", "v")
                        .send(lookAlike, "k", "v")
                        .build())
                .sends(declared)
                .stores(store)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE),
                () -> "the declared Long serializer cannot serialize the look-alike's String;"
                        + " the stop must carry its own reason, not a bare ClassCastException; got " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "emissions are serialized in the planning phase, so a serialization refusal"
                            + " must fire before any state write reaches the store");
        }
    }

    /** A swallowed undeclared-store read still fails the step. */
    @Test
    void swallowedUndeclaredStoreReadStillFailsTheStep() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
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
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "the reader latches its refusal and deliver() rethrows after the handler"
                        + " returns, so a catch inside the handler cannot commit the step; got " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(), "no effect of the refused step may apply");
        }
    }

    /**
     * A handler returning null is a seam-contract breach parsley itself detects, and it
     * recurs identically on every restart, so it must carry its own refusal reason —
     * status() reporting a stop with an empty refusalReason would misread as transient.
     */
    @Test
    void nullEffectsFromAHandlerFailClosedWithTheirOwnReason() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> null)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.HANDLER_RETURNED_NULL_EFFECTS),
                () -> "a deliberate refusal that recurs on restart must name its reason; got " + thrown);
    }

    /**
     * A stored value the declared serde can no longer decode is the state-read shape of
     * D13's payload rule: it fails the step with its own reason, and an application catch
     * around the read cannot swallow it — the reader latches the refusal.
     */
    @Test
    void undecodableStoredStateValueFailsClosedEvenWhenSwallowed() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        org.apache.kafka.common.serialization.Serde<String> poisonRead =
                Serdes.serdeFrom(new StringSerializer(), (topic, data) -> {
                    throw new RuntimeException("schema moved on");
                });
        Store<String, String> store = Store.of("app-store", Serdes.String(), poisonRead);
        ProcessDefinition definition = ProcessDefinition.named("p")
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
        newDriver(definition, new FakeFacts());

        input("in1").pipeInput(new TestRecord<>("k".getBytes(), "first".getBytes()));
        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "second".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE),
                () -> "a stored value the declared serde cannot decode must fail the step with its"
                        + " reason, latched past any application catch; got " + thrown);
    }

    /** A null store on a state read is refused with a message. */
    @Test
    void nullStoreOnAStateReadIsRefusedWithAMessage() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(null, "k");
                    return Effects.none();
                })
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(chainContainsIllegalArgument(thrown, "store must be non-null"),
                () -> "a null store must be refused per the taxonomy, not surface as a bare NPE"
                        + " from store.name(); got " + thrown);
    }

    /** A null key on a state read is refused with a message. */
    @Test
    void nullKeyOnAStateReadIsRefusedWithAMessage() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(store, null);
                    return Effects.none();
                })
                .stores(store)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(chainContainsIllegalArgument(thrown, "state read key must be non-null"),
                () -> "a null read key must be refused like a null write key, not reach the"
                        + " state backend as null bytes; got " + thrown);
    }

    private static boolean chainContainsIllegalArgument(Throwable thrown, String messagePart) {
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof IllegalArgumentException
                    && cause.getMessage() != null && cause.getMessage().contains(messagePart)) {
                return true;
            }
        }
        return false;
    }

    /** A state write ahead of a refused emission is not applied. */
    @Test
    void stateWriteAheadOfARefusedEmissionIsNotApplied() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> undeclared = Channel.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(store, "k", "v")
                        .send(undeclared, "k", "v")
                        .build())
                .stores(store)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL),
                () -> "expected EMISSION_TO_UNDECLARED_CHANNEL in " + thrown);
        try (var all = driver.<org.apache.kafka.common.utils.Bytes, byte[]>getKeyValueStore("app-store").all()) {
            assertFalse(all.hasNext(),
                    "every effect target is validated before any write is applied, so a refused"
                            + " step must not leave earlier writes relying on the EOS abort alone");
        }
    }

    /** A state write to an undeclared store fails closed before any write applies. */
    @Test
    void stateWriteToAnUndeclaredStoreFailsClosedBeforeAnyWriteApplies() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> Effects.builder()
                        .put(declared, "k", "v")
                        .put(lookAlike, "k2", "v2")
                        .build())
                .stores(declared)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
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
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Store<String, String> declared = Store.of("app-store", Serdes.String(), Serdes.String());
        Store<String, String> lookAlike = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> {
                    state.get(lookAlike, "k");
                    return Effects.none();
                })
                .stores(declared)
                .build();
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE),
                () -> "a read through a Store instance other than the declared one would smuggle"
                        + " a differently-typed codec into the application's frame; got " + thrown);
    }

    /** Application state reads see earlier writes and tombstones pass through. */
    @Test
    void applicationStateReadsSeeEarlierWritesAndTombstonesPassThrough() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        Store<String, String> store = Store.of("app-store", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
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
        newDriver(definition, new FakeFacts());

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
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition upstream = ProcessDefinition.named("up")
                .receives(in1, (delivery, state) -> Effects.builder().send(out, "k", "v").build())
                .sends(out)
                .build();
        newDriver(upstream, new FakeFacts());
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
        Channel<String, String> loop = Channel.of("loop", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(loop, (delivery, state) -> delivery.value().length() < 3
                        ? Effects.builder().send(loop, delivery.key(), delivery.value() + "x").build()
                        : Effects.none())
                .sends(loop)
                .build();
        newDriver(definition, new FakeFacts(), Map.of("loop", new TopicInfo(new UUID(100, 9), 1)));

        input("loop").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes()));
        TestOutputTopic<byte[], byte[]> out =
                driver.createOutputTopic("loop", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("vx".getBytes(), out.readRecord().value());
    }

    /** Several send channels and several stores wire independently. */
    @Test
    void severalSendChannelsAndSeveralStoresWireIndependently() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        Channel<String, String> out2 = Channel.of("in2", Serdes.String(), Serdes.String());
        Store<String, String> storeA = Store.of("store-a", Serdes.String(), Serdes.String());
        Store<String, String> storeB = Store.of("store-b", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
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
        newDriver(definition, new FakeFacts());

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

    private static ProcessDefinition twoInputRecorder(List<String> delivered) {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> in2 = Channel.of("in2", Serdes.String(), Serdes.String());
        return ProcessDefinition.named("p")
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

    private static boolean causeChainContains(Throwable thrown, ParsleyFailClosedException.Reason reason) {
        ParsleyFailClosedException found = ParsleyFailClosedException.findIn(thrown);
        return found != null && found.reason() == reason;
    }
}
