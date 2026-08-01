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
import java.time.Instant;
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
 * asserts the runtime explains it correctly: the missing cause, the local watermarks, and a
 * diagnosis whose remedy is never to skip the record.
 */
class ExplainHoldsTest {

    private static final Topic<String, String> CAUSES =
            Topic.of("causes", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> EFFECTS =
            Topic.of("effects", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> REPLIES =
            Topic.of("replies", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> SIGNALS =
            Topic.of("signals", Codec.utf8(), Codec.utf8());
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

    /** A stage consuming both topics. What it emits is irrelevant to the diagnosis. */
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
                Parsley.named("explain-upstream", upstream).testTopology(), props("explain-upstream"))) {
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
                Parsley.named("explain-offset", consumer(Duration.ofSeconds(30)))
                        .testTopology(),
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
                Parsley.named("explain-cleared", consumer(Duration.ofSeconds(30)))
                        .testTopology(),
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
                Parsley.named("explain-seq-upstream", responder).testTopology(), props("explain-seq-upstream"))) {
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
                Parsley.named("explain-seq", watcher).testTopology(), props("explain-seq"))) {
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
                Parsley.named("explain-nopayload", consumer(Duration.ofSeconds(30)))
                        .testTopology(),
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
                Parsley.named("explain-summary", consumer(Duration.ofSeconds(30)))
                        .testTopology(),
                props("explain-summary"))) {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            String summary = Holds.forApplication("explain-summary").get(0).summary();
            assertTrue(summary.contains("vc-hold-not-fetched"),
                    "the stable code must appear: " + summary);
            assertTrue(summary.contains("effects:0@0") && summary.contains("causes:0"),
                    "both coordinates must appear: " + summary);
            assertTrue(summary.contains("check lag"), "the remedy must appear: " + summary);
            assertTrue(summary.contains("HeldRecord.Diagnosis#NOT_FETCHED"),
                    "the diagnosis reference must appear: " + summary);
        }
    }

    /** Every diagnosis carries a stable code and a reference, and none of them says "skip it". */
    @Test
    void everyDiagnosisCarriesAStableCodeAndAnchor() {
        for (HeldRecord.Diagnosis d : HeldRecord.Diagnosis.values()) {
            assertTrue(d.code().startsWith("vc-hold-"), "stable code prefix: " + d.code());
            assertTrue(d.reference().equals("HeldRecord.Diagnosis#" + d.name()),
                    "each diagnosis references its own constant: " + d.reference());
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

    /**
     * A cause that has already been fetched here but is itself held is diagnosed as such, not
     * as one still in flight. The local consumer position is the dividing line: everything
     * strictly below it arrived here or was consumer-skipped, so a claim below it names a
     * record that is waiting on its own channel's head, a different investigation entirely
     * from a lagging upstream.
     */
    @Test
    void aCauseAlreadyFetchedHereIsDiagnosedAsHeldUpstream() {
        // A causes record that is itself gated, on a claim to a channel nothing will feed.
        Stage gatedCause = Stage.named("gatedcause")
                .on(REPLIES, m -> List.of(CAUSES.send(m.key(), "c0")))
                .into(CAUSES)
                .build();
        TestRecord<byte[], byte[]> cause;
        try (TopologyTestDriver d = new TopologyTestDriver(
                Parsley.named("upstream-gatedcause", gatedCause).testTopology(), props("upstream-gatedcause"))) {
            in(d, "replies").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("r0"));
            cause = d.createOutputTopic("causes",
                            new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(0);
        }
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");

        Stage watcher = Stage.named("watcher")
                .on(CAUSES, m -> List.of(OUT.send(m.key(), m.value())))
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(REPLIES, m -> List.of(OUT.send(m.key(), m.value())))
                .into(OUT)
                .build();
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-upstream-held", watcher).testTopology(), props("explain-upstream-held"))) {
            // causes@0 arrives and holds: it claims replies@0, which never comes.
            in(app, "causes").pipeInput(cause);
            // effects@0 claims causes@0 — fetched here, and held.
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-upstream-held");
            assertEquals(2, holds.size(), "both records must be held: " + holds);
            HeldRecord onEffects = holds.stream().filter(h -> h.topic().equals("effects"))
                    .findFirst().orElseThrow();
            HeldRecord.Unmet unmet = onEffects.unmet().get(0);
            assertEquals(HeldRecord.Diagnosis.HELD_UPSTREAM, unmet.diagnosis(),
                    "causes@0 has been fetched here, so the remedy is to follow that channel's"
                            + " head, not to check the upstream's lag: " + unmet);
            assertEquals(1L, unmet.localPosition(),
                    "the position must sit past the claimed offset, which is what says the"
                            + " record arrived: " + unmet);

            HeldRecord onCauses = holds.stream().filter(h -> h.topic().equals("causes"))
                    .findFirst().orElseThrow();
            assertEquals(HeldRecord.Diagnosis.NOT_FETCHED, onCauses.unmet().get(0).diagnosis(),
                    "the cause at the end of the chain is the one genuinely not fetched: "
                            + onCauses);
        }
    }

    /**
     * A sequence claim ahead of a sender this task has delivered is diagnosed as that sender
     * being behind, not as one never seen. The two point at different things, the second at
     * the late-joiner caveat and the first at ordinary lag. Sequence zero is a real delivery,
     * so the mark must be read as present rather than as nothing.
     */
    @Test
    void aClaimBehindItsSendersDeliveredSequenceIsDiagnosedAsSenderBehind() {
        Stage responder = Stage.named("responder")
                .on(CAUSES, m -> List.of(
                        EFFECTS.send(m.key(), "effect"),
                        REPLIES.send(m.key(), "reply")))
                .into(EFFECTS, REPLIES)
                .build();
        List<TestRecord<byte[], byte[]>> effects;
        List<TestRecord<byte[], byte[]>> replies;
        try (TopologyTestDriver service = new TopologyTestDriver(
                Parsley.named("behind-upstream", responder).testTopology(), props("behind-upstream"))) {
            in(service, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            in(service, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c1"));
            effects = service.createOutputTopic("effects",
                    new ByteArrayDeserializer(), new ByteArrayDeserializer()).readRecordsToList();
            replies = service.createOutputTopic("replies",
                    new ByteArrayDeserializer(), new ByteArrayDeserializer()).readRecordsToList();
        }

        Stage watcher = Stage.named("watcher")
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(REPLIES, m -> List.of(OUT.send(m.key(), m.value())))
                .into(OUT)
                .build();
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-behind", watcher).testTopology(), props("explain-behind"))) {
            in(app, "effects").pipeInput(effects.get(0)); // delivers; marks the sender at 0
            in(app, "replies").pipeInput(replies.get(1)); // claims that sender's sequence 1
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-behind");
            assertEquals(1, holds.size(), "the second reply must be held: " + holds);
            assertEquals(1, holds.get(0).unmet().size(),
                    "only the claim on the other channel is unmet: " + holds.get(0).unmet());
            HeldRecord.Unmet unmet = holds.get(0).unmet().get(0);
            assertEquals(HeldRecord.Diagnosis.SENDER_BEHIND, unmet.diagnosis(),
                    "that sender has been delivered here, just not far enough: " + unmet);
            assertEquals("effects", unmet.topic(), "the channel the claim names");
            assertEquals(1L, unmet.claimedSequence(), "the sequence the claim names");
            assertEquals(0L, unmet.deliveredSequence(),
                    "sequence zero is a delivery, not the absence of one");
        }
    }

    /**
     * Only the claims a head is actually waiting for are reported. A stamp carries the emitter's
     * whole causal past, most of which the reader has already delivered. Reporting a claim
     * that is exactly met sends the operator after a cause that is already here, which is the
     * one mistake a diagnosis surface must not make.
     */
    @Test
    void aClaimMetExactlyAtTheFrontierIsNotReportedAsUnmet() {
        Stage upstream = Stage.named("upstream")
                .on(CAUSES, m -> List.of(EFFECTS.send(m.key(), "e")))
                .on(REPLIES, m -> List.of(EFFECTS.send(m.key(), "e")))
                .into(EFFECTS)
                .build();
        TestRecord<byte[], byte[]> effect;
        try (TopologyTestDriver d = new TopologyTestDriver(
                Parsley.named("upstream-twoclaims", upstream).testTopology(), props("upstream-twoclaims"))) {
            in(d, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            in(d, "replies").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("r0"));
            // The second emission claims both causes@0 and replies@0.
            effect = d.createOutputTopic("effects",
                            new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(1);
        }

        Stage watcher = Stage.named("watcher")
                .on(CAUSES, m -> List.of(OUT.send(m.key(), m.value())))
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(REPLIES, m -> List.of(OUT.send(m.key(), m.value())))
                .into(OUT)
                .build();
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-metclaim", watcher).testTopology(), props("explain-metclaim"))) {
            in(app, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-metclaim");
            assertEquals(1, holds.size(), "the effect must be held on its replies claim: " + holds);
            assertEquals(1, holds.get(0).unmet().size(),
                    "causes@0 is delivered, so the frontier meets that claim exactly and only"
                            + " the replies claim is outstanding: " + holds.get(0).unmet());
            assertEquals("replies", holds.get(0).unmet().get(0).topic(),
                    "and it must be the replies claim that is reported: " + holds.get(0).unmet());
        }
    }

    /** The same rule for sequence claims: a claim exactly at the delivered sequence is met. */
    @Test
    void aSequenceClaimMetExactlyIsNotReportedAsUnmet() {
        Stage responder = Stage.named("responder")
                .on(CAUSES, m -> List.of(
                        EFFECTS.send(m.key(), "effect"),
                        SIGNALS.send(m.key(), "signal"),
                        REPLIES.send(m.key(), "reply")))
                .into(EFFECTS, SIGNALS, REPLIES)
                .build();
        TestRecord<byte[], byte[]> effect;
        TestRecord<byte[], byte[]> reply;
        try (TopologyTestDriver service = new TopologyTestDriver(
                Parsley.named("metseq-upstream", responder).testTopology(), props("metseq-upstream"))) {
            in(service, "causes").pipeInput(Codec.utf8().encode("k"), Codec.utf8().encode("c0"));
            effect = service.createOutputTopic("effects",
                            new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(0);
            // The reply claims the sender's sequence 0 on both effects and signals.
            reply = service.createOutputTopic("replies",
                            new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readRecordsToList().get(0);
        }

        Stage watcher = Stage.named("watcher")
                .on(EFFECTS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(SIGNALS, m -> List.of(OUT.send(m.key(), m.value())))
                .on(REPLIES, m -> List.of(OUT.send(m.key(), m.value())))
                .into(OUT)
                .build();
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-metseq", watcher).testTopology(), props("explain-metseq"))) {
            in(app, "effects").pipeInput(effect); // delivers; marks the sender at sequence 0
            in(app, "replies").pipeInput(reply);
            app.advanceWallClockTime(TICK);

            List<HeldRecord> holds = Holds.forApplication("explain-metseq");
            assertEquals(1, holds.size(), "the reply must be held on its signals claim: " + holds);
            assertEquals(1, holds.get(0).unmet().size(),
                    "the effects claim names sequence 0, which this task has delivered from that"
                            + " sender, so only the signals claim is outstanding: "
                            + holds.get(0).unmet());
            assertEquals("signals", holds.get(0).unmet().get(0).topic(),
                    "and it must be the signals claim that is reported: " + holds.get(0).unmet());
        }
    }

    /**
     * A hold ages from when its record became the head, by the wall clock the punctuator reads.
     * It is the number an operator watches to tell an ordinary wait from a stuck one, and the
     * gauge, this snapshot, and the log warning all read the same mark so they cannot disagree.
     */
    @Test
    void aHoldAgesFromWhenItsRecordBecameTheHead() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-age", consumer(Duration.ofSeconds(30)))
                        .testTopology(),
                props("explain-age"), Instant.ofEpochMilli(0L))) {
            in(app, "effects").pipeInput(effect);

            app.advanceWallClockTime(TICK);
            assertEquals(0L, Holds.forApplication("explain-age").get(0).heldMs(),
                    "the first sample that observes a head is where its age starts");

            app.advanceWallClockTime(TICK);
            assertEquals(TICK.toMillis(), Holds.forApplication("explain-age").get(0).heldMs(),
                    "and from there it ages by the punctuator's wall clock");
        }
    }

    /**
     * A closing task drops its published snapshot. Tasks migrate, and a snapshot left behind
     * describes a task this instance no longer runs. It would be reported to every reader of
     * {@link CausalStreams#explainHolds()} forever, naming a hold nobody here can act on.
     */
    @Test
    void aClosingTaskDropsItsPublishedHolds() {
        TestRecord<byte[], byte[]> effect = effectClaimingCause("e0");
        TopologyTestDriver app = new TopologyTestDriver(
                Parsley.named("explain-closed", consumer(Duration.ofSeconds(30)))
                        .testTopology(), props("explain-closed"));
        try {
            in(app, "effects").pipeInput(effect);
            app.advanceWallClockTime(TICK);
            assertFalse(Holds.forApplication("explain-closed").isEmpty(),
                    "the hold must be published before the task closes");
        } finally {
            app.close();
        }
        assertEquals(List.of(), Holds.forApplication("explain-closed"),
                "the closed task's snapshot must not linger on the instance");
    }

    /** One task closing must not take the instance's other tasks' holds down with it. */
    @Test
    void clearingOneTasksHoldsLeavesTheOthers() {
        HeldRecord one = new HeldRecord("s", "0_0", "a", 0, 1L, 1, 1_000L,
                List.of(new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED, "c", 0, 1L, -1L,
                        0L, null, -1L, -1L)));
        HeldRecord two = new HeldRecord("s", "0_1", "b", 1, 2L, 1, 2_000L,
                List.of(new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED, "c", 1, 1L, -1L,
                        0L, null, -1L, -1L)));
        Holds.publish("app-partial", "0_0", List.of(one));
        Holds.publish("app-partial", "0_1", List.of(two));
        try {
            Holds.clear("app-partial", "0_0");
            assertEquals(List.of(two), Holds.forApplication("app-partial"),
                    "the task that is still running must keep reporting its hold");
        } finally {
            Holds.clear("app-partial", "0_1");
        }
        assertEquals(List.of(), Holds.forApplication("app-partial"),
                "and the last task closing must leave nothing behind");
    }

    /**
     * Each claim kind renders in its own vocabulary, and a hold with several causes says so.
     * An offset claim names an offset measured against the local frontier. A sequence claim
     * names a sender and a sequence measured against what that sender has delivered here.
     * Rendering one as the other prints {@code -1} where the operator expects the number the
     * gate is actually comparing.
     */
    @Test
    void eachClaimKindRendersInItsOwnVocabulary() {
        HeldRecord.Unmet offsetClaim = new HeldRecord.Unmet(HeldRecord.Diagnosis.NOT_FETCHED,
                "causes", 0, 3L, -1L, 0L, null, -1L, -1L);
        HeldRecord.Unmet sequenceClaim = new HeldRecord.Unmet(HeldRecord.Diagnosis.SENDER_BEHIND,
                "effects", 0, -1L, 5L, 6L, "sender-a", 4L, 1L);

        assertTrue(offsetClaim.describe().contains("offset 3"),
                "an offset claim names the offset claimed: " + offsetClaim.describe());
        assertTrue(offsetClaim.describe().contains("local frontier -1, position 0"),
                "and the local watermarks it is measured against: " + offsetClaim.describe());
        assertTrue(sequenceClaim.describe().contains("sequence 4 from sender sender-a"),
                "a sequence claim names the sender and sequence: " + sequenceClaim.describe());
        assertTrue(sequenceClaim.describe().contains("highest delivered sequence 1"),
                "and the delivered sequence it is measured against: " + sequenceClaim.describe());

        HeldRecord single = new HeldRecord("s", "0_0", "effects", 0, 7L, 1, 1_000L,
                List.of(offsetClaim));
        assertFalse(single.summary().contains("more unmet"),
                "one unmet cause is the whole story: " + single.summary());
        HeldRecord several = new HeldRecord("s", "0_0", "effects", 0, 7L, 2, 1_000L,
                List.of(offsetClaim, sequenceClaim));
        assertTrue(several.summary().contains("(+1 more unmet)"),
                "further causes are counted exactly, so the line stays one line: "
                        + several.summary());
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
