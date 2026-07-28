package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
 * Adapter smoke tests under TopologyTestDriver, driven the way a user would: typed topics,
 * pure handlers and folds, the broker-less {@code Parsley.testTopology()} wiring. Protocol
 * correctness under concurrency, crashes, and EOS lives in the simulator suite — TTD is
 * single-threaded and transactionless, so these tests only assert the adapter's plumbing.
 */
class StageTest {

    private static final Topic<String, String> T1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> T2 = Topic.of("t2", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> T3 = Topic.of("t3", Codec.utf8(), Codec.utf8());

    @TempDir
    Path stateDir;

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
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
}
