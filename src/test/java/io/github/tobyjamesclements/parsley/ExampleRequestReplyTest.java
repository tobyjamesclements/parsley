package io.github.tobyjamesclements.parsley;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worked example — <b>request and reply with effect-before-reply</b>
 * ({@code docs/guide/topologies.md#request-and-reply}). A responder emits its effects and then
 * its reply from one handler; the reply claims the effects (emissions of one delivery are
 * claimed in list order), so a requester consuming both topics has already been delivered
 * everything the reply acknowledges by the time it processes the reply.
 *
 * <p>Read-your-writes across services, expressed as a topology shape rather than a wait loop.
 */
class ExampleRequestReplyTest {

    private static final Topic<String, String> REQUESTS =
            Topic.of("requests", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> EFFECTS =
            Topic.of("effects", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> REPLIES =
            Topic.of("replies", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> OBSERVED =
            Topic.of("observed", Codec.utf8(), Codec.utf8());

    /** The responder: effect first, reply second, both from one delivery. */
    private static Stage responder() {
        return Stage.named("responder")
                .on(REQUESTS, m -> List.of(
                        EFFECTS.send(m.key(), "wrote:" + m.value()),
                        REPLIES.send(m.key(), "ack:" + m.value())))
                .into(EFFECTS, REPLIES)
                .build();
    }

    /** The requester side: whatever it does on a reply may assume the effect is visible. */
    private static Stage requester() {
        return Stage.named("requester")
                .on(EFFECTS, m -> List.of(OBSERVED.send(m.key(), "effect:" + m.value())))
                .on(REPLIES, m -> List.of(OBSERVED.send(m.key(), "reply:" + m.value())))
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
     * The reply's stamp must claim the effect emitted before it. The claim is a <em>sequence</em>
     * claim rather than an offset one: the broker assigns offsets asynchronously, so a stamp
     * names the sender's own just-issued sends by their send sequence, which is known
     * synchronously ({@code docs/design/architecture.md#the-stamp}). That is what lets a hop
     * cost one round trip and still order its own outputs.
     */
    @Test
    void theReplyClaimsTheEffectEmittedBeforeIt() {
        try (TopologyTestDriver service = new TopologyTestDriver(
                Parsley.of(responder()).testTopology(), props("reqrep-service"))) {
            in(service, "requests").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("v"));

            TestRecord<byte[], byte[]> effect = out(service, "effects").readRecordsToList().get(0);
            TestRecord<byte[], byte[]> reply = out(service, "replies").readRecordsToList().get(0);

            var sender = CausalHeaders.readSender(effect.headers());
            long effectSeq = CausalHeaders.readSeq(effect.headers());
            long claimedByReply = CausalHeaders.read(reply.headers())
                    .getSeq(new VectorClock.SeqKey(Stage.testChannel("effects", 0), sender));
            assertTrue(claimedByReply >= effectSeq,
                    "the reply must claim the effect it acknowledges: claimed sequence "
                            + claimedByReply + ", effect sent at sequence " + effectSeq);
        }
    }

    /**
     * The requester delivers the effect before the reply even when the reply reaches it first
     * — the gate holds the reply, and the effect's arrival releases it. Without the claim,
     * an acknowledgement could be processed before the write it acknowledges was visible.
     */
    @Test
    void theRequesterSeesTheEffectBeforeTheReplyEvenWhenTheReplyArrivesFirst() {
        TestRecord<byte[], byte[]> effect;
        TestRecord<byte[], byte[]> reply;
        try (TopologyTestDriver service = new TopologyTestDriver(
                Parsley.of(responder()).testTopology(), props("reqrep-produce"))) {
            in(service, "requests").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("v"));
            effect = out(service, "effects").readRecordsToList().get(0);
            reply = out(service, "replies").readRecordsToList().get(0);
        }

        try (TopologyTestDriver client = new TopologyTestDriver(
                Parsley.of(requester()).testTopology(), props("reqrep-client"))) {
            TestOutputTopic<byte[], byte[]> observed = out(client, "observed");

            in(client, "replies").pipeInput(reply);
            assertTrue(observed.isEmpty(),
                    "the reply must be held: the effect it claims is not delivered here yet");

            in(client, "effects").pipeInput(effect);
            assertEquals(List.of("effect:wrote:v", "reply:ack:v"),
                    observed.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the effect must be delivered before the reply that acknowledges it");
        }
    }
}
