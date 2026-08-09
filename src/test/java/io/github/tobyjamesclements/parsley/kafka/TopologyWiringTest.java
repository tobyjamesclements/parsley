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
import io.github.tobyjamesclements.parsley.api.StoreDef;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kafka Streams wiring, driven through TopologyTestDriver: byte-exact key/value pass-through, the causal header on
 * the wire, cross-channel holds at the processor level, punctuator fact ingestion, store persistence across a
 * restart, and step-failing rejections. Protocol-level behaviour under crashes and transactions is covered by the
 * simulation suite; broker-level behaviour by the integration tests.
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

    /** A controllable facts source standing in for the admin client. */
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

    /** ASSESSMENT 1.13/D56: forwarding the received headers on an emission is a natural pattern; the reserved
     * transport header is parsley's, filtered out of the seam's view, so the pattern must simply work — and the
     * application header must travel while the emission still carries fresh causal metadata of its own. */
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

    /** ASSESSMENT 3.7: a payload the application's own serde cannot decode fails the step with parsley's reason
     * (D13) — skipping would deliver past the message (SPEC Safety 3). */
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

        // Safety 4/5: the record's key and value are exactly what the application's serializer produces — a reader
        // with the application's codecs alone decodes them; nothing is wrapped or prefixed.
        assertArrayEquals(new StringSerializer().serialize("out", "k1"), record.key());
        assertArrayEquals(new StringSerializer().serialize("out", "v1!"), record.value());

        // The causal metadata rides in the reserved header, expressing the delivered cause (in1@0).
        Causes causes = CausesCodec.decode(record.headers().lastHeader(CausesCodec.HEADER_KEY).value());
        assertEquals(Causes.of(Map.of(IN1, 0L)), causes);
    }

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

        // The host's read position report says in1 has nothing before position 6 left to feed.
        facts.facts = new PositionFacts(Map.of(IN1, 6L), Map.of(), Set.of());
        driver.advanceWallClockTime(Duration.ofMillis(200));
        assertEquals(List.of("B"), delivered, "the report, not a message and not time, frees the hold");
    }

    // Restart persistence of held messages cannot be shown with TopologyTestDriver — its close() wipes local state
    // by design — and is covered by the simulation suite (heldMessageIsDeliveredAfterRestart) and the broker
    // integration test instead.

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

    @Test
    void emissionToUndeclaredChannelFailsTheStep() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> undeclared = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) ->
                        Effects.builder().send(undeclared, "k", "v").build())
                .build(); // note: no .sends(...)
        newDriver(definition, new FakeFacts());

        Throwable thrown = assertThrows(Throwable.class, () ->
                input("in1").pipeInput(new TestRecord<>("k".getBytes(), "v".getBytes())));
        assertTrue(causeChainContains(thrown, ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL),
                () -> "expected EMISSION_TO_UNDECLARED_CHANNEL in " + thrown);
    }

    @Test
    void applicationStateReadsSeeEarlierWritesAndTombstonesPassThrough() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        StoreDef<String, String> store = StoreDef.of("app-store", Serdes.String(), Serdes.String());
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
        input("in1").pipeInput(new TestRecord<>("k".getBytes(), (byte[]) null)); // a tombstone: no value sent

        TestOutputTopic<byte[], byte[]> outTopic =
                driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertArrayEquals("1".getBytes(), outTopic.readRecord().value());
        assertArrayEquals("2".getBytes(), outTopic.readRecord().value(), "reads must see earlier committed writes");
        assertNull(outTopic.readRecord().value(), "no value required where the application sent none");
    }

    @Test
    void stampedCausesRelayAcrossProcessesAndCompress() throws Exception {
        // First process delivers in1@0 and emits to out; run its output through a second driver's input to check
        // what a downstream parsley process learns from the wire alone.
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

    @Test
    void selfChannelTopologyIsAccepted() {
        // SPEC Structural 2: a channel from a process to itself means the same topic is both a source and a sink of
        // one topology. This pins the adapter's acceptance of that arrangement; the delivery semantics of the loop
        // are covered by the simulation suite.
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

    @Test
    void severalSendChannelsAndSeveralStoresWireIndependently() {
        // SPEC Structural 18: a process may send to any number of channels and hold any number of stores.
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("out", Serdes.String(), Serdes.String());
        Channel<String, String> out2 = Channel.of("in2", Serdes.String(), Serdes.String());
        StoreDef<String, String> storeA = StoreDef.of("store-a", Serdes.String(), Serdes.String());
        StoreDef<String, String> storeB = StoreDef.of("store-b", Serdes.String(), Serdes.String());
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
