package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adapter smoke tests under TopologyTestDriver, driven the way a user would: typed topics,
 * pure handlers and folds, the broker-less {@code Parsley.testTopology()} wiring. Protocol
 * correctness under concurrency, crashes, and EOS lives in the simulator suite — TTD is
 * single-threaded and transactionless, so these tests only assert the adapter's plumbing.
 */
class StageTest {

    private static final Topic<String, String> T1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> T2 = Topic.of("t2", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> T3 = Topic.of("t3", Codec.utf8(), Codec.utf8());

    /** The broker-less resolver the production wiring tests pair with a custom offsets view. */
    private static final TopicIds TEST_IDS =
            topic -> new TopicIds.Resolved(Stage.testChannel(topic, 0).topicId(), 1);

    private static final BrokerOffsets NO_OFFSETS = new BrokerOffsets() {
        @Override
        public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
            return Map.of();
        }

        @Override
        public EarliestOffsets earliestOffsets(Set<Channel> channels) {
            return new EarliestOffsets(Map.of(), Set.of());
        }
    };

    @TempDir
    Path stateDir;

    @AfterEach
    void clearCapturedPositions() {
        Positions.forCurrentThread().clear();
    }

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    private static String messageChain(Throwable t) {
        StringBuilder chain = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            chain.append(c.getMessage()).append('\n');
        }
        return chain.toString();
    }

    private TopologyTestDriver statelessDriver() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .on(T2, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .into(T3)
                .build();
        return new TopologyTestDriver(Parsley.of(stage).testTopology(), props("parsley-ttd"));
    }

    /** An unstamped record delivers immediately and its output carries a stamp claiming it. */
    @Test
    void deliversAndStampsOutputs() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput("k", "a", 1000L);
            var out = t3.readRecordsToList();
            assertEquals(1, out.size(), "one delivery, one emission");
            assertEquals("out:a", out.get(0).value());

            VectorClock stamp = CausalHeaders.read(out.get(0).headers());
            assertNotNull(stamp, "output must carry a dependency clock");
            assertEquals(0L, stamp.get(Stage.testChannel("t1", 0)),
                    "stamp must claim the delivered input");
        }
    }

    /** A record whose dependency has not been delivered is held, then released in order. */
    @Test
    void holdsUntilDependencyDelivered() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            // A t2 record claiming t1@0 arrives before t1@0 itself: the gate must hold it.
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 0));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));
            assertTrue(t3.readRecordsToList().isEmpty(), "held record must not produce output");

            // Its cause arrives: both deliver, in causal order.
            t1.pipeInput("k", "a", 1000L);
            var out = t3.readRecordsToList();
            assertEquals(List.of("out:a", "out:b"),
                    out.stream().map(TestRecord::value).toList(),
                    "cause delivers before effect");

            // The second output's stamp claims both inputs.
            VectorClock stamp = CausalHeaders.read(out.get(1).headers());
            assertEquals(0L, stamp.get(Stage.testChannel("t1", 0)));
            assertEquals(0L, stamp.get(Stage.testChannel("t2", 0)));
        }
    }

    /** A fold accumulates per-key state across deliveries, and a null step state deletes it. */
    @Test
    void foldAccumulatesAndDeletesPerKeyState() {
        Stage counts = Stage.named("counts")
                .state(Codec.int64(), () -> 0L)
                .on(T1, (Long n, Message<String, String> m) -> "reset".equals(m.value())
                        ? Step.of(null)
                        : Step.of(n + 1, T3.send(m.key(), "n=" + (n + 1))))
                .into(T3)
                .build();
        try (TopologyTestDriver driver =
                     new TopologyTestDriver(Parsley.of(counts).testTopology(), props("parsley-fold-ttd"))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput("a", "x", 1000L);
            t1.pipeInput("a", "y", 1001L);
            t1.pipeInput("b", "z", 1002L);
            assertEquals(List.of("n=1", "n=2", "n=1"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "state must accumulate per key, independently across keys");

            // A null step state deletes the key's state; the next fold starts from initial.
            t1.pipeInput("a", "reset", 1003L);
            t1.pipeInput("a", "x", 1004L);
            assertEquals(List.of("n=1"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "after deletion the fold must see the initial state again");
        }
    }

    /** An undecodable clock header fails the task rather than reading as empty. */
    @Test
    void corruptClockFailsClosed() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            var headers = new RecordHeaders();
            headers.add(CausalHeaders.CLOCK, new byte[] {9, 9, 9});
            boolean threw = false;
            try {
                t1.pipeInput(new TestRecord<>("k", "x", headers, 1000L));
            } catch (RuntimeException expected) {
                threw = true;
            }
            assertTrue(threw, "a corrupt clock header must fail the task, never read as empty");
        }
    }

    /** The partition function matches the producer default: murmur2 keyed, zero for null. */
    @Test
    void partitionFunctionMatchesProducerDefault() {
        assertEquals(0, Stage.partitionFor(null, 4), "null keys must pin to partition zero");
        boolean sawNonZero = false;
        for (int i = 0; i < 8; i++) {
            byte[] key = ("k" + i).getBytes();
            int expected = Utils.toPositive(Utils.murmur2(key)) % 4;
            assertEquals(expected, Stage.partitionFor(key, 4),
                    "keyed records must land where the default producer partitioner would");
            sawNonZero |= expected != 0;
        }
        assertTrue(sawNonZero, "the sample must exercise a non-zero partition");
    }

    /** A null key folds under its own state, distinct from an empty key's state. */
    @Test
    void foldKeepsNullAndEmptyKeyStateApart() {
        Stage counts = Stage.named("counts")
                .state(Codec.int64(), () -> 0L)
                .on(T1, (Long n, Message<String, String> m) ->
                        Step.of(n + 1, T3.send(m.key(), "n=" + (n + 1))))
                .into(T3)
                .build();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(counts).testTopology(), props("parsley-nullkey-ttd"))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(null, "x", 1000L);
            t1.pipeInput(null, "y", 1001L);
            t1.pipeInput("", "z", 1002L);
            assertEquals(List.of("n=1", "n=2", "n=1"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the null key must accumulate its own state, separate from the empty key");
        }
    }

    /** A stateful stage mixes fold and handler sources; handlers leave the state untouched. */
    @Test
    void statefulStageMixesFoldAndHandlerSources() {
        Stage mix = Stage.named("mix")
                .state(Codec.int64(), () -> 0L)
                .on(T1, (Long n, Message<String, String> m) ->
                        Step.of(n + 1, T3.send(m.key(), "n=" + (n + 1))))
                .on(T2, m -> List.of(T3.send(m.key(), "h:" + m.value())))
                .truncationInterval(Duration.ofMinutes(10))
                .into(T3)
                .build();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(mix).testTopology(), props("parsley-mixed-ttd"))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput("k", "a", 1000L);
            t2.pipeInput("k", "x", 1001L);
            t1.pipeInput("k", "b", 1002L);
            assertEquals(List.of("n=1", "h:x", "n=2"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the handler source must interleave without disturbing the fold's state");
        }
    }

    /** Production wiring under a weaker guarantee fails the task at init, not at first loss. */
    @Test
    void refusesToRunWithoutExactlyOnce() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .into(T3)
                .build();
        Topology t = new Topology();
        stage.addTo(t, TEST_IDS, (ids, sinks) -> NO_OFFSETS, false);

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> new TopologyTestDriver(t, props("parsley-eos-guard")).close(),
                "init under the default at_least_once guarantee must fail closed");
        assertTrue(messageChain(e).contains("processing.guarantee"),
                "the failure must name the missing guarantee, got: " + messageChain(e));
    }

    /** Production wiring without captured consumer positions fails at the first record. */
    @Test
    void refusesToRunWithoutCapturedPositions() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .into(T3)
                .build();
        Topology t = new Topology();
        stage.addTo(t, TEST_IDS, (ids, sinks) -> NO_OFFSETS, false);
        Properties props = props("parsley-pos-guard");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        try (TopologyTestDriver driver = new TopologyTestDriver(t, props)) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            Positions.forCurrentThread().clear();
            RuntimeException e = assertThrows(RuntimeException.class,
                    () -> t1.pipeInput("k", "v", 1000L),
                    "a record with no captured positions must fail closed");
            assertTrue(messageChain(e).contains("captured consumer positions"),
                    "the failure must name the missing capture, got: " + messageChain(e));
        }
    }

    /** The wall-clock punctuator sweeps captured positions and releases held records. */
    @Test
    void punctuatedSweepReleasesHeldRecord() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            // Held: claims t1@0, which never arrives as a record.
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 0));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));
            assertTrue(t3.readRecordsToList().isEmpty(), "the claim is unmet, so the record holds");

            // The consumer's position moved past t1@0 (marker or aborted batch): the sweep
            // driven by the punctuator must account for the skipped run and release.
            Positions.forCurrentThread().put(new TopicPartition("t1", 0), 1L);
            driver.advanceWallClockTime(Duration.ofMillis(500));
            assertEquals(List.of("out:b"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the punctuated sweep must release the held record");
        }
    }

    /** The sweep at the end of each process call releases in the same poll, punctuator aside. */
    @Test
    void processEndSweepReleasesHeldRecord() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput("k", "a", 1000L);
            assertEquals(1, t3.readRecordsToList().size(), "the plain record delivers");

            // Held: claims t1@5, far past the delivered frontier.
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 5));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));
            assertTrue(t3.readRecordsToList().isEmpty(), "the claim is unmet, so the record holds");

            // The position is past the claim before the next record; without the punctuator
            // firing, the end-of-process sweep alone must pick it up and release.
            Positions.forCurrentThread().put(new TopicPartition("t1", 0), 6L);
            t1.pipeInput("k", "c", 3000L);
            assertEquals(List.of("out:c", "out:b"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the process-end sweep must release the held record in the same call");
        }
    }

    /** The truncation punctuator drops claims retention has made globally stable. */
    @Test
    void truncationSweepDropsStableUpstreamClaims() {
        Channel up = Stage.testChannel("up", 0);
        Channel up2 = Stage.testChannel("up2", 0);
        BrokerOffsets logStarts = new BrokerOffsets() {
            @Override
            public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
                return Map.of();
            }

            @Override
            public EarliestOffsets earliestOffsets(Set<Channel> channels) {
                // Retention deleted everything below offset 8 on both upstream channels.
                return new EarliestOffsets(Map.of(up, 8L, up2, 8L), Set.of());
            }
        };
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .into(T3)
                .truncationInterval(Duration.ofSeconds(1))
                .build();
        Topology t = new Topology();
        stage.addTo(t, TEST_IDS, (ids, sinks) -> logStarts, true);

        try (TopologyTestDriver driver = new TopologyTestDriver(t, props("parsley-trunc-ttd"))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            // The inbound record carries custody of two non-consumed upstream coordinates.
            VectorClock carried = VectorClock.of(up, 7);
            carried.advanceTo(up2, 8);
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, carried);
            t1.pipeInput(new TestRecord<>("k", "a", headers, 1000L));

            VectorClock before = CausalHeaders.read(t3.readRecordsToList().get(0).headers());
            assertEquals(7L, before.get(up), "custody of up@7 must be claimed before truncation");
            assertEquals(8L, before.get(up2), "custody of up2@8 must be claimed before truncation");

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            t1.pipeInput("k", "b", 2000L);
            VectorClock after = CausalHeaders.read(t3.readRecordsToList().get(0).headers());
            assertEquals(VectorClock.NOTHING, after.get(up),
                    "up@7 sits below log start 8 and must be truncated away");
            assertEquals(8L, after.get(up2),
                    "up2@8 is exactly the log start and must survive (stability is logStart - 1)");
            assertEquals(1L, after.get(Stage.testChannel("t1", 0)),
                    "consumed-channel claims must be untouched by truncation");
        }
    }
}
