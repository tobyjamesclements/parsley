package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the 80%-of-budget metadata warning (D53): the threshold sits at exactly 80%
 * of the configured budget, the warning fires once per process, and the punctuator's
 * frontier observation actually consults the latch.
 *
 * <p>The threshold and the latch are pinned through {@link ParsleyProcessor.BudgetAlarm},
 * the seam {@code observeFrontier} delegates to; the wiring leg drives the real punctuator
 * through {@code TopologyTestDriver} and reads the warning off {@code System.err}, where
 * slf4j-simple writes it.
 */
class BudgetWarningTest {
    private static final UUID IN1_ID = new UUID(300, 1);
    private static final UUID IN2_ID = new UUID(300, 2);
    private static final Map<String, TopicInfo> TOPICS = Map.of(
            "in1", new TopicInfo(IN1_ID, 1),
            "in2", new TopicInfo(IN2_ID, 1));

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * Pins the threshold at exactly 80% of the budget, in both directions: a regression
     * moving it up (say to 99%, warning only when the wall is already imminent) fails the
     * at-the-boundary assert, and one moving it down fails the one-byte-below assert. The
     * below-threshold consultation also pins that consulting without warning does not set
     * the latch.
     */
    @Test
    void warningThresholdSitsAtExactlyEightyPercentOfTheBudget() {
        ParsleyProcessor.BudgetAlarm alarm = new ParsleyProcessor.BudgetAlarm();
        assertFalse(alarm.shouldWarn(799, 1000),
                "one byte below 80% of the budget must not warn: the surface promises 80%, and an"
                        + " earlier warning would cry wolf on healthy frontiers");
        assertTrue(alarm.shouldWarn(800, 1000),
                "exactly 80% of the budget must warn (D53): a threshold moved above 80% would leave"
                        + " the operator without the promised headroom before the fail-closed wall");
    }

    /**
     * Pins the latch (D53's "warns once"): after the first warning, later consultations
     * above the threshold stay silent, so a frontier sitting above 80% does not repeat the
     * warning every facts round for the rest of the process's life.
     */
    @Test
    void warningFiresExactlyOnceAcrossRepeatedConsultations() {
        ParsleyProcessor.BudgetAlarm alarm = new ParsleyProcessor.BudgetAlarm();
        assertTrue(alarm.shouldWarn(800, 1000), "the first crossing warns");
        assertFalse(alarm.shouldWarn(900, 1000),
                "a second consultation above the threshold must not warn again: D53 promises one"
                        + " warning per process, not one per facts round");
        assertFalse(alarm.shouldWarn(999, 1000),
                "the latch holds however close to the wall the frontier grows; the budget itself"
                        + " fails closed with its own diagnosis when reached");
    }

    /**
     * Pins the wiring the unit seam cannot see: the wall-clock punctuator's frontier
     * observation consults the alarm and emits the warning. A regression deleting the
     * consultation from {@code observeFrontier} leaves this red with zero warnings; one
     * dropping the latch leaves it red with one warning per punctuation. Two delivered
     * channels put the frontier at 54 encoded bytes, inside the 64-byte budget's
     * [80%, 100%) band, so the warning fires without the budget's fail-closed refusal.
     *
     * <p>Two environmental hazards this capture leans on: JUnit parallel execution would
     * break the exact-count assertion, because {@code System.err} is process-global and a
     * concurrent test's output — or its own stream swap — would land in or around the
     * capture. And slf4j-simple's {@code cacheOutputStream=true} would latch the original
     * {@code System.err} at first use, so the swapped-in capture would read nothing; the
     * suite relies on the default, which reads {@code System.err} at each write.
     */
    @Test
    void punctuatorEmitsTheBudgetWarningThroughTheAlarmExactlyOnce() {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> in2 = Channel.of("in2", Serdes.String(), Serdes.String());
        ProcessDefinition definition = ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> Effects.none())
                .receives(in2, (delivery, state) -> Effects.none())
                .build();
        FactsSource facts = (Set<ChannelId> received, Set<ChannelId> frontier) -> PositionFacts.EMPTY;
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "budget-warning-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(
                ProcessTopology.build(definition, TOPICS, facts, Duration.ofMillis(100),
                        Runnable::run, 64), props);

        TestInputTopic<byte[], byte[]> in1Topic =
                driver.createInputTopic("in1", new ByteArraySerializer(), new ByteArraySerializer());
        TestInputTopic<byte[], byte[]> in2Topic =
                driver.createInputTopic("in2", new ByteArraySerializer(), new ByteArraySerializer());
        in1Topic.pipeInput(new TestRecord<>("k".getBytes(), "a".getBytes()));
        in2Topic.pipeInput(new TestRecord<>("k".getBytes(), "b".getBytes()));

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream realErr = System.err;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            driver.advanceWallClockTime(Duration.ofMillis(150));
            assertEquals(1, warningCount(captured),
                    "the punctuator's frontier observation must emit the 80% warning when the"
                            + " frontier (54 bytes) stands at or above 80% of the 64-byte budget");
            driver.advanceWallClockTime(Duration.ofMillis(150));
            driver.advanceWallClockTime(Duration.ofMillis(150));
            driver.advanceWallClockTime(Duration.ofMillis(150));
            assertEquals(1, warningCount(captured),
                    "later punctuations over the same still-above-threshold frontier must not"
                            + " repeat the warning: D53 promises it once per process");
        } finally {
            System.setErr(realErr);
        }
    }

    /** Counts the D53 warning lines in the captured log output. */
    private static int warningCount(ByteArrayOutputStream captured) {
        String output = captured.toString(StandardCharsets.UTF_8);
        String marker = "80% of the 64-byte budget";
        int count = 0;
        for (int at = output.indexOf(marker); at >= 0; at = output.indexOf(marker, at + marker.length())) {
            count++;
        }
        return count;
    }
}
