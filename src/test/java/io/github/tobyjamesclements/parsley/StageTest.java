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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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

            // The consumer's position moved past t1@0 (marker or aborted batch, no record
            // ever returned): the sweep driven by the punctuator must account for the
            // skipped run and release.
            Positions.forCurrentThread().put(new TopicPartition("t1", 0),
                    new Positions.PollView(1L, -1L));
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

            // The position is past the claim (markers behind the returned record at t1@1);
            // without the punctuator firing, the end-of-process sweep alone must pick it up
            // and release once that record has been fed.
            Positions.forCurrentThread().put(new TopicPartition("t1", 0),
                    new Positions.PollView(6L, 1L));
            t1.pipeInput("k", "c", 3000L);
            assertEquals(List.of("out:c", "out:b"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the process-end sweep must release the held record in the same call");
        }
    }

    /**
     * A poll's post-poll position runs ahead of returned records the task has not yet
     * processed; the sweep must withhold that position until the whole batch is fed. Applied
     * early, the frontier jumps the buffered records and they are dropped as replays — lost,
     * with their offsets committed.
     */
    @Test
    void positionSweepWaitsForBufferedRecordsOfTheBatch() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            // One poll returned t1@0..2 and left the position at 3 before any process call.
            Positions.forCurrentThread().put(new TopicPartition("t1", 0),
                    new Positions.PollView(3L, 2L));
            // The wall-clock punctuator can fire between the poll and the first process call;
            // its sweep must withhold the position, not advance the frontier to 2.
            driver.advanceWallClockTime(Duration.ofMillis(500));

            t1.pipeInput("k", "a", 1000L);
            t1.pipeInput("k", "b", 1001L);
            t1.pipeInput("k", "c", 1002L);
            assertEquals(List.of("out:a", "out:b", "out:c"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "every record of the polled batch must deliver; none may be dropped as a replay");
        }
    }

    /** The value of the parsley metric {@code name}; the task-level one when {@code topic} is null. */
    private static double parsleyMetric(TopologyTestDriver driver, String name, String topic) {
        List<Double> matches = driver.metrics().entrySet().stream()
                .filter(e -> e.getKey().group().equals(StageMetrics.GROUP))
                .filter(e -> e.getKey().name().equals(name))
                .filter(e -> topic == null ? !e.getKey().tags().containsKey("topic")
                        : topic.equals(e.getKey().tags().get("topic")))
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .toList();
        assertEquals(1, matches.size(),
                "expected exactly one metric named " + name + " for topic " + topic);
        return matches.get(0);
    }

    /** The punctuator samples hold depth, and delivered batches accumulate into the total. */
    @Test
    void metricsReportHoldDepthAndDeliveries() {
        try (TopologyTestDriver driver = statelessDriver()) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());

            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 0));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));
            driver.advanceWallClockTime(Duration.ofMillis(500));
            assertEquals(1.0, parsleyMetric(driver, "records-held", null),
                    "the held record must show on the depth gauge");
            assertEquals(0.0, parsleyMetric(driver, "records-delivered-total", null),
                    "nothing has passed the gate yet");

            t1.pipeInput("k", "a", 1000L);
            assertEquals(2.0, parsleyMetric(driver, "records-delivered-total", null),
                    "the cause and the released record must both count as delivered");
            driver.advanceWallClockTime(Duration.ofMillis(500));
            assertEquals(0.0, parsleyMetric(driver, "records-held", null),
                    "the depth gauge must drop back to zero after release");
        }
    }

    /** Per-topic gauges record only at the DEBUG metrics recording level, tagged by topic. */
    @Test
    void perTopicGaugesRecordAtDebugLevel() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .on(T2, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .into(T3)
                .build();
        Properties props = props("parsley-ttd-debug");
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        try (TopologyTestDriver driver =
                     new TopologyTestDriver(Parsley.of(stage).testTopology(), props)) {
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());

            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 0));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));
            driver.advanceWallClockTime(Duration.ofMillis(500));
            assertEquals(1.0, parsleyMetric(driver, "records-held", "t2"),
                    "the holding channel's per-topic gauge must record");
            assertEquals(0.0, parsleyMetric(driver, "records-held", "t1"),
                    "the idle channel's per-topic gauge must read zero");
        }
    }

    /** A failing truncation sweep skips its cycle and counts on the skip total. */
    @Test
    void truncationSweepFailureCounts() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of())
                .truncationInterval(Duration.ofSeconds(1))
                .build();
        BrokerOffsets failing = new BrokerOffsets() {
            @Override
            public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
                return Map.of();
            }

            @Override
            public EarliestOffsets earliestOffsets(Set<Channel> channels) {
                throw new IllegalStateException("log-start resolution failed");
            }
        };
        Topology t = new Topology();
        stage.addTo(t, TEST_IDS, (ids, sinks) -> failing, true);
        try (TopologyTestDriver driver = new TopologyTestDriver(t, props("parsley-ttd-sweep"))) {
            driver.advanceWallClockTime(Duration.ofSeconds(1));
            assertEquals(1.0, parsleyMetric(driver, "truncation-sweeps-skipped-total", null),
                    "the failed sweep must count once and must not fail the task");
        }
    }

    /**
     * A tick is emitted at the interval as a stamped record on the stage's own tick topic,
     * loops back through the gate, and is delivered to the tick logic — whose emissions land
     * on the declared sink. The tick record's stamp claims the task's delivered history.
     */
    @Test
    void tickEmitsStampedRecordAndDeliversToLogic() {
        Stage stage = Stage.named("ticker")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .ticks(Duration.ofSeconds(1),
                        tick -> List.of(T3.send("tick", "fired@" + tick.timestamp())))
                .into(T3)
                .build();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(stage).testTopology(), props("parsley-tick-ttd"),
                Instant.ofEpochMilli(0L))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<byte[], byte[]> tickTopic = driver.createOutputTopic(
                    "parsley-ticker-ticks",
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer());

            t1.pipeInput("k", "a", 1000L);
            assertEquals(List.of("out:a"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the data record must deliver before any tick fires");

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            var ticks = tickTopic.readRecordsToList();
            assertEquals(1, ticks.size(), "one interval, one tick record");
            assertEquals(0, Stage.tickPartition(ticks.get(0).key()),
                    "the tick key must carry the emitting task's own partition");
            VectorClock stamp = CausalHeaders.read(ticks.get(0).headers());
            assertNotNull(stamp, "the tick must carry a dependency clock");
            assertEquals(0L, stamp.get(Stage.testChannel("t1", 0)),
                    "the tick's stamp must claim the task's delivered history");
            assertNotNull(CausalHeaders.readSender(ticks.get(0).headers()),
                    "the tick must carry the task's sender tag");

            assertEquals(List.of("fired@1000"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the looped-back tick must be delivered to the tick logic");
            assertEquals(1.0, parsleyMetric(driver, "ticks-emitted-total", null),
                    "the emitted tick must count on the task's tick total");
        }
    }

    /**
     * A stateful tick fold steps the reserved per-partition tick slot: distinct from the
     * null key's slot, accumulated across ticks, deleted by a null step state, and resolved
     * to the initial value again afterwards.
     */
    @Test
    void tickFoldStepsTickStateApartFromKeyState() {
        Stage stage = Stage.named("tickfold")
                .state(Codec.int64(), () -> 0L)
                .on(T1, (Long n, Message<String, String> m) ->
                        Step.of(n + 1, T3.send(m.key(), "n=" + (n + 1))))
                .ticks(Duration.ofSeconds(1), (Long n, Tick tick) -> (n + 1) == 2
                        ? Step.of(null, T3.send("tick", "t=2"))
                        : Step.of(n + 1, T3.send("tick", "t=" + (n + 1))))
                .into(T3)
                .build();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(stage).testTopology(), props("parsley-tickfold-ttd"),
                Instant.ofEpochMilli(0L))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            // The null-key record folds under the null-key slot; if the tick slot collided
            // with it, the first tick would see 1 and step straight to the delete branch.
            t1.pipeInput(null, "x", 1000L);
            driver.advanceWallClockTime(Duration.ofSeconds(1)); // t=1 (from initial)
            driver.advanceWallClockTime(Duration.ofSeconds(1)); // t=2, slot deleted
            driver.advanceWallClockTime(Duration.ofSeconds(1)); // t=1 (initial again)
            t1.pipeInput(null, "y", 1001L);

            assertEquals(List.of("n=1", "t=1", "t=2", "t=1", "n=2"),
                    t3.readRecordsToList().stream().map(TestRecord::value).toList(),
                    "the tick slot must accumulate, delete, and reset apart from the null key's slot");
        }
    }

    /**
     * A tick's stamp merges claims advertised by held records, so a tick emitted while a
     * record is held on an unresolved dependency is itself gated — delay-only — and released
     * in causal order once the dependency arrives.
     */
    @Test
    void tickHeldBehindAdvertisedClaimReleasesInOrder() {
        Stage stage = Stage.named("gated")
                .on(T1, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .on(T2, m -> List.of(T3.send(m.key(), "out:" + m.value())))
                .ticks(Duration.ofSeconds(1),
                        tick -> List.of(T3.send("tick", "tick@" + tick.timestamp())))
                .into(T3)
                .build();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(stage).testTopology(), props("parsley-tickgate-ttd"),
                Instant.ofEpochMilli(0L))) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<byte[], byte[]> tickTopic = driver.createOutputTopic(
                    "parsley-gated-ticks",
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer());

            t1.pipeInput("k", "a", 1000L);
            assertEquals(1, t3.readRecordsToList().size(), "the plain record delivers");

            // Held on t2: claims t1@1, which has not arrived.
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, VectorClock.of(Stage.testChannel("t1", 0), 1));
            t2.pipeInput(new TestRecord<>("k", "b", headers, 2000L));

            driver.advanceWallClockTime(Duration.ofSeconds(1));
            assertEquals(1, tickTopic.readRecordsToList().size(),
                    "the tick is emitted regardless of the held record");
            assertTrue(t3.readRecordsToList().isEmpty(),
                    "the tick inherits the advertised t1@1 claim and must itself be gated");

            // The dependency arrives: the cause, the held record, and the tick all release.
            t1.pipeInput("k", "c", 1001L);
            var out = t3.readRecordsToList().stream().map(TestRecord::value).toList();
            assertEquals(3, out.size(), "the cause, the held record, and the tick must all deliver");
            assertEquals("out:c", out.get(0), "the cause must deliver first");
            assertEquals(Set.of("out:b", "tick@1000"), Set.copyOf(out.subList(1, 3)),
                    "the held record and the gated tick must both release after their cause");
        }
    }

    /** Ticks are declared once, with a positive interval, in either builder phase. */
    @Test
    void ticksDeclaredOnceWithPositiveInterval() {
        TickHandler none = tick -> List.of();
        assertThrows(IllegalStateException.class,
                () -> Stage.named("s").on(T1, m -> List.of())
                        .ticks(Duration.ofSeconds(1), none).ticks(Duration.ofSeconds(1), none),
                "a second stateless tick declaration must be rejected");
        assertThrows(IllegalStateException.class,
                () -> Stage.named("s").state(Codec.int64(), () -> 0L)
                        .on(T1, (Long n, Message<String, String> m) -> Step.of(n))
                        .ticks(Duration.ofSeconds(1), (Long n, Tick t) -> Step.of(n))
                        .ticks(Duration.ofSeconds(1), (Long n, Tick t) -> Step.of(n)),
                "a second stateful tick declaration must be rejected");
        assertThrows(IllegalArgumentException.class,
                () -> Stage.named("s").on(T1, m -> List.of()).ticks(Duration.ZERO, none),
                "a zero interval must be rejected");
        assertThrows(IllegalArgumentException.class,
                () -> Stage.named("s").on(T1, m -> List.of()).ticks(Duration.ofSeconds(-1), none),
                "a negative interval must be rejected");
    }

    /** Assembly rejects a tick topic whose partition count differs from the widest source. */
    @Test
    void tickTopicPartitionsMustMatchWidestSource() {
        Stage stage = Stage.named("mismatch")
                .on(T1, m -> List.of())
                .ticks(Duration.ofSeconds(1), tick -> List.of())
                .build();
        TopicIds ids = topic -> new TopicIds.Resolved(
                Stage.testChannel(topic, 0).topicId(), topic.startsWith("parsley-") ? 1 : 2);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> stage.addTo(new Topology(), ids, (i, s) -> NO_OFFSETS, true),
                "a tick topic narrower than the widest source must fail assembly");
        assertTrue(e.getMessage().contains("partitions"),
                "the failure must name the partition mismatch, got: " + e.getMessage());
    }

    /** The tick sink partitioner reads the emitting task's partition back from the key. */
    @Test
    void tickPartitionDecodesTheEmittingTask() {
        assertEquals(3, Stage.tickPartition(ByteBuffer.allocate(4).putInt(3).array()),
                "the four-byte big-endian key must decode to the emitting partition");
        assertEquals(0, Stage.tickPartition(ByteBuffer.allocate(4).putInt(0).array()),
                "partition zero must round-trip");
        assertThrows(IllegalStateException.class, () -> Stage.tickPartition(null),
                "a missing tick key must fail closed");
        assertThrows(IllegalStateException.class, () -> Stage.tickPartition(new byte[] {1, 2}),
                "a malformed tick key must fail closed");
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
