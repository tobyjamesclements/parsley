package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hold-diagnosis surface: {@link CausalStreams#explainHolds()} and the age-triggered
 * warning. Each test builds a real hold by feeding a stamped record ahead of its cause, then
 * asserts the runtime explains it correctly — the missing cause, the local watermarks, and a
 * diagnosis whose remedy is never to skip the record.
 */
class ExplainHoldsTest {

    private static final Topic<String, String> CAUSES =
            Topic.of("causes", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> EFFECTS =
            Topic.of("effects", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> REPLIES =
            Topic.of("replies", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> OUT = Topic.of("out", Codec.utf8(), Codec.utf8());

    private static final Duration TICK = Duration.ofMillis(500);

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

    /** A stage consuming both topics; what it emits is irrelevant to the diagnosis. */
    private static Stage consumer(Duration warnAfter) {
        return Stage.named("consumer")
                .on(CAUSES, m -> List.of(OUT.send(m.key(), m.value())))
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .holdWarningAfter(warnAfter)
                .into(OUT)
                .build();
    }

    /**
     * Produces one effect record stamped with a claim on {@code causes@0}, the way a real
     * upstream stage would: it delivers the cause and emits, so the emission claims it.
     */
    private TestRecord<byte[], byte[]> effectClaimingCause(String value) {
        Stage upstream = Stage.named("upstream")
                .on(CAUSES, m -> List.of(EFFECTS.send(m.key(), value)))
                .into(EFFECTS)
                .build();
        try (TopologyTestDriver d = new TopologyTestDriver(
                Parsley.of(upstream).testTopology(), props("explain-upstream"))) {
            in(d, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            return d.createOutputTopic("effects",
                    new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(0);
        }
    }

    /**
     * A record held on an offset claim explains itself: the held coordinate, the missing
     * cause's coordinate, and the local watermarks that show the gap. The cause has not been
     * fetched here at all, which is the ordinary lagging-channel case.
     */
    @Test
    void aHeldRecordNamesTheCauseItIsWaitingFor() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(consumer(Duration.ofSeconds(30))).testTopology(),
                props("explain-offset"))) {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-offset");
            assertEquals(1, holds.size(), "the held effect must be explained: " + holds);
            HeldRecord held = holds.get(0);
            assertEquals("effects", held.topic(), "the hold is on the effects channel");
            assertEquals(0L, held.offset(), "the held record's own offset");
            assertEquals(1, held.queueDepth(), "one record waits on this channel");

            assertEquals(1, held.unmet().size(), "one unmet cause: " + held.unmet());
            HeldRecord.Unmet unmet = held.unmet().get(0);
            assertEquals(HeldRecord.Diagnosis.NOT_FETCHED, unmet.diagnosis(),
                    "the cause has not been fetched here at all");
            assertEquals("causes", unmet.topic(), "the missing cause's topic, by name");
            assertEquals(0L, unmet.claimedOffset(), "the claimed offset");
            assertEquals(-1L, unmet.localFrontier(), "nothing delivered on that channel yet");
            assertNull(unmet.sender(), "an offset claim carries no sender");
        }
    }

    /** Delivering the missing cause releases the hold, and the explanation empties with it. */
    @Test
    void deliveringTheCauseClearsTheExplanation() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(consumer(Duration.ofSeconds(30))).testTopology(),
                props("explain-cleared"))) {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);
            assertFalse(Holds.forApplication("explain-cleared").isEmpty(),
                    "the effect must be held before its cause arrives");

            in(app, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            app.advanceWallClockTime(TICK);
            assertEquals(List.of(), Holds.forApplication("explain-cleared"),
                    "nothing is held once the cause is delivered");
        }
    }

    /**
     * A record held on a sequence claim names the sender and the sequence, not an offset:
     * offsets are assigned asynchronously, so a sender's claims on its own just-issued sends
     * are in sequence space. Never having delivered that sender on that channel is the
     * signature the late-joiner caveat warns about.
     */
    @Test
    void aSequenceClaimNamesItsSenderAndSequence() {
        Stage responder = Stage.named("responder")
                .on(CAUSES, m -> List.of(
                        EFFECTS.send(m.key(), "effect"),
                        REPLIES.send(m.key(), "reply")))
                .into(EFFECTS, REPLIES)
                .build();
        TestRecord<byte[], byte[]> reply;
        try (TopologyTestDriver service = new TopologyTestDriver(
                Parsley.of(responder).testTopology(), props("explain-seq-upstream"))) {
            in(service, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            reply = service.createOutputTopic("replies",
                    new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(0);
        }

        Stage watcher = Stage.named("watcher")
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(REPLIES, m -> List.of(OUT.send(m.key(), m.value())))
                .into(OUT)
                .build();
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(watcher).testTopology(), props("explain-seq"))) {
            in(app, "replies").pipeInput(reply);
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-seq");
            assertEquals(1, holds.size(), "the reply must be held: " + holds);
            HeldRecord.Unmet unmet = holds.get(0).unmet().get(0);
            assertEquals(HeldRecord.Diagnosis.SENDER_UNSEEN, unmet.diagnosis(),
                    "no record from that sender has been delivered on effects");
            assertEquals("effects", unmet.topic(), "the channel the claim names");
            assertNotNull(unmet.sender(), "a sequence claim names its sender");
            assertTrue(unmet.claimedSequence() >= 0, "and the sequence it claims");
            assertEquals(-1L, unmet.deliveredSequence(),
                    "nothing from that sender delivered here yet");
            assertEquals(-1L, unmet.claimedOffset(), "a sequence claim carries no offset");
        }
    }

    /** Explanations are payload-free: coordinates and claims only, never record bytes. */
    @Test
    void explanationsCarryNoPayload() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("SUPER-SECRET-PAYLOAD");
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(consumer(Duration.ofSeconds(30))).testTopology(),
                props("explain-nopayload"))) {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            String rendered = Holds.forApplication("explain-nopayload").toString()
                    + Holds.forApplication("explain-nopayload").get(0).summary();
            assertFalse(rendered.contains("SUPER-SECRET-PAYLOAD"),
                    "no payload may reach the diagnosis surface: " + rendered);
        }
    }

    /** The summary is a one-line diagnosis: coordinates, the remedy, and where to read more. */
    @Test
    void theSummaryCarriesTheCodeRemedyAndReference() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(consumer(Duration.ofSeconds(30))).testTopology(),
                props("explain-summary"))) {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            String summary = Holds.forApplication("explain-summary").get(0).summary();
            assertTrue(summary.contains("vc-hold-not-fetched"),
                    "the stable code must appear: " + summary);
            assertTrue(summary.contains("effects:0@0") && summary.contains("causes:0"),
                    "both coordinates must appear: " + summary);
            assertTrue(summary.contains("check lag"), "the remedy must appear: " + summary);
            assertTrue(summary.contains("docs/guide/diagnosing-holds.md"),
                    "the documentation anchor must appear: " + summary);
        }
    }

    /** Every diagnosis carries a stable code and an anchor, and none of them says "skip it". */
    @Test
    void everyDiagnosisCarriesAStableCodeAndAnchor() {
        for (HeldRecord.Diagnosis d : HeldRecord.Diagnosis.values()) {
            assertTrue(d.code().startsWith("vc-hold-"), "stable code prefix: " + d.code());
            assertTrue(d.reference().startsWith("docs/"), "documentation anchor: " + d.reference());
            assertFalse(d.remedy().contains("skip") || d.remedy().contains("timeout"),
                    "no remedy may suggest skipping or timing out: " + d.remedy());
        }
    }

    /**
     * The warning fires once per head, only after the threshold, and never when disabled: a
     * warning per punctuator tick would bury the diagnosis it exists to surface.
     */
    @Test
    void theWarningFiresOncePerHeadAndRespectsTheThreshold() {
        Duration threshold = Duration.ofSeconds(30);
        assertFalse(Stage.shouldWarn(threshold, 29_000, null, 5L),
                "below the threshold, nothing is warned");
        assertTrue(Stage.shouldWarn(threshold, 30_000, null, 5L),
                "at the threshold, an unwarned head warns");
        assertFalse(Stage.shouldWarn(threshold, 60_000, 5L, 5L),
                "the same head must not warn twice");
        assertTrue(Stage.shouldWarn(threshold, 60_000, 5L, 6L),
                "a new head on the channel is a new hold and warns again");
        assertFalse(Stage.shouldWarn(Duration.ZERO, 60_000, null, 5L),
                "a zero threshold disables the warning");
        assertFalse(Stage.shouldWarn(Duration.ofSeconds(-1), 60_000, null, 5L),
                "a negative threshold disables the warning");
    }

    /** The public accessor reports this application's holds, and no other application's. */
    @Test
    void theAccessorIsScopedToItsApplication() {
        HeldRecord record = new HeldRecord("stage", "0_0", "effects", 0, 7L, 1, 1_000L,
                List.of(new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED, "causes", 0,
                        3L, -1L, 0L, null, -1L, -1L)));
        Holds.publish("app-one", "0_0", List.of(record));
        try {
            assertEquals(List.of(record), new CausalStreams(null, null, "app-one").explainHolds(),
                    "the accessor must report its own application's holds");
            assertEquals(List.of(), new CausalStreams(null, null, "app-two").explainHolds(),
                    "another application's holds must not leak in");
        } finally {
            Holds.clear("app-one", "0_0");
        }
        assertEquals(List.of(), new CausalStreams(null, null, "app-one").explainHolds(),
                "a closed task's snapshot must not linger");
    }

    /** Longest-held first: the oldest hold is the one gating everything behind it. */
    @Test
    void holdsAreReportedLongestHeldFirst() {
        HeldRecord young = new HeldRecord("s", "0_0", "a", 0, 1L, 1, 1_000L,
                List.of(new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED, "c", 0, 1L, -1L,
                        0L, null, -1L, -1L)));
        HeldRecord old = new HeldRecord("s", "0_1", "b", 1, 2L, 1, 9_000L,
                List.of(new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED, "c", 1, 1L, -1L,
                        0L, null, -1L, -1L)));
        Holds.publish("app-order", "0_0", List.of(young));
        Holds.publish("app-order", "0_1", List.of(old));
        try {
            assertEquals(List.of(old, young), Holds.forApplication("app-order"),
                    "the longest-held record must come first");
        } finally {
            Holds.clear("app-order", "0_0");
            Holds.clear("app-order", "0_1");
        }
    }
}
