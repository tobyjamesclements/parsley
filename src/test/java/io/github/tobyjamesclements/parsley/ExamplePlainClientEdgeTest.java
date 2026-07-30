package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worked example — <b>the plain-client edge</b> ({@code docs/guide/clients.md}). Topologies
 * rarely begin and end at stages: a plain Kafka producer at the border stamps with a
 * {@link CausalClock}, and its records are then gated downstream exactly like a stage's. A
 * plain consumer needs nothing at all, and may read the stamps for observability.
 *
 * <p>Both halves of the clock's contract appear here: {@code observe} makes what the client
 * read a cause of what it writes, and {@code recordProduced} orders the client's own
 * successive sends across topics.
 */
class ExamplePlainClientEdgeTest {

    private static final Topic<String, String> RECEIPTS =
            Topic.of("receipts", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> FOLLOWUPS =
            Topic.of("followups", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> NOTES =
            Topic.of("notes", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> OBSERVED =
            Topic.of("observed", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> ORDERS =
            Topic.of("orders", Codec.utf8(), Codec.utf8());

    /** A stage that records what it was delivered, in delivery order. */
    private static Stage watcher(Topic<String, String> first, Topic<String, String> second) {
        return Stage.named("watcher")
                .on(first, m -> List.of(OBSERVED.send(m.key(), first.name() + ":" + m.value())))
                .on(second, m -> List.of(OBSERVED.send(m.key(), second.name() + ":" + m.value())))
                .into(OBSERVED)
                .build();
    }

    @TempDir
    Path stateDir;

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(appId).toString());
        return props;
    }

    private static TestInputTopic<byte[], byte[]> in(TopologyTestDriver d, String topic) {
        return d.createInputTopic(topic, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static TestOutputTopic<byte[], byte[]> out(TopologyTestDriver d, String topic) {
        return d.createOutputTopic(topic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * A clock wired to the broker-less topology's synthesized topic identity. Production code
     * passes an {@code Admin} instead ({@code new CausalClock(admin)}); everything else about
     * the client is identical.
     */
    private static CausalClock testClock() {
        return new CausalClock(topic -> new TopicIds.Resolved(
                Stage.testChannel(topic, 0).topicId(), 1));
    }

    /** One record as a plain consumer would hand it to {@code observe}. */
    private static ConsumerRecord<byte[], byte[]> consumed(String topic, long offset, String value) {
        return new ConsumerRecord<>(topic, 0, offset,
                Codec.utf8().encode("k"), Codec.utf8().encode(value));
    }

    /** One acknowledgement as a producer's {@code send(...).get()} would return it. */
    private static RecordMetadata acknowledged(String topic, long offset) {
        return new RecordMetadata(new TopicPartition(topic, 0), offset, 0, 0L, 0, 0);
    }

    /**
     * What a plain producer read becomes a cause of what it writes. The client observed a
     * receipt and then produced a follow-up; a downstream stage consuming both must deliver
     * the receipt first, and holds the follow-up until it has.
     */
    @Test
    void whatAPlainProducerObservedBecomesACauseOfWhatItWrites() {
        CausalClock clock = testClock();
        clock.observe(consumed("receipts", 0, "r0"));

        RecordHeaders headers = new RecordHeaders();
        clock.stamp(headers);
        TestRecord<byte[], byte[]> followup = new TestRecord<>(
                Codec.utf8().encode("k"), Codec.utf8().encode("f0"), headers, 2_000L);

        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(watcher(RECEIPTS, FOLLOWUPS)).testTopology(), props("edge-observe"))) {
            TestOutputTopic<byte[], byte[]> observed = out(app, "observed");

            in(app, "followups").pipeInput(followup);
            assertTrue(observed.isEmpty(),
                    "the follow-up must be held: the receipt it claims is not delivered yet");

            in(app, "receipts").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("r0"));
            assertEquals(List.of("receipts:r0", "followups:f0"),
                    observed.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the observed receipt must be delivered before the record it caused");
        }
    }

    /**
     * The sequential-producer pattern: send, await the acknowledgement, fold it, send again.
     * The second record claims the first, so a consumer of both topics delivers them in the
     * order the client sent them — the ordering a single producer cannot otherwise assert
     * across topics.
     */
    @Test
    void aSequentialProducerOrdersItsOwnSendsAcrossTopics() {
        CausalClock clock = testClock();

        RecordHeaders firstHeaders = new RecordHeaders();
        clock.stamp(firstHeaders);
        TestRecord<byte[], byte[]> first = new TestRecord<>(
                Codec.utf8().encode("k"), Codec.utf8().encode("f0"), firstHeaders, 1_000L);
        clock.recordProduced(acknowledged("followups", 0));

        RecordHeaders secondHeaders = new RecordHeaders();
        clock.stamp(secondHeaders);
        TestRecord<byte[], byte[]> second = new TestRecord<>(
                Codec.utf8().encode("k"), Codec.utf8().encode("n0"), secondHeaders, 2_000L);

        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(watcher(FOLLOWUPS, NOTES)).testTopology(), props("edge-sequential"))) {
            TestOutputTopic<byte[], byte[]> observed = out(app, "observed");

            in(app, "notes").pipeInput(second);
            assertTrue(observed.isEmpty(),
                    "the second send must be held: it claims the first, still undelivered");

            in(app, "followups").pipeInput(first);
            assertEquals(List.of("followups:f0", "notes:n0"),
                    observed.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the client's sends must be delivered in the order it made them");
        }
    }

    /**
     * A plain consumer needs no Parsley awareness: the only trace on the wire is headers,
     * which it may ignore or, for observability, read. It gets no delivery ordering — a
     * consumer that wants that is a stage.
     */
    @Test
    void aPlainConsumerCanReadStampsForObservability() {
        Stage gateway = Stage.named("gateway")
                .on(ORDERS, m -> List.of(RECEIPTS.send(m.key(), "receipt:" + m.value())))
                .into(RECEIPTS)
                .build();

        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(gateway).testTopology(), props("edge-observability"))) {
            in(app, "orders").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("o0"));

            TestRecord<byte[], byte[]> receipt = out(app, "receipts").readRecordsToList().get(0);
            assertEquals("receipt:o0", Codec.utf8().decode(receipt.value()),
                    "the payload is ordinary; Parsley adds no envelope");
            assertNotNull(CausalHeaders.readSender(receipt.headers()),
                    "the sender tag is readable by any consumer");
            assertTrue(CausalHeaders.readSeq(receipt.headers()) >= 0,
                    "the send sequence is readable by any consumer");
        }
    }
}
