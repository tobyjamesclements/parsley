package io.parsley.it;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationReason;
import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.serialisation.DefaultVectorClockSerialiser;
import io.parsley.streams.CausalProcessorSupplier;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CausalOrderingEndToEndTest {

    @TempDir
    Path tempDir;

    private static final DefaultVectorClockSerialiser SERIALISER = new DefaultVectorClockSerialiser();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String CLOCK_HEADER = "parsley-vector-clock";

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> output;
    private final List<CausalViolationReason> violations = new ArrayList<>();

    private TopologyTestDriver buildDriver(String stateDir, String appId) {
        StreamsBuilder builder = new StreamsBuilder();
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                (_, reason) -> violations.add(reason),
                SERIALISER);
        builder.stream(INPUT, Consumed.with(STRING_SERDE, STRING_SERDE))
               .process(supplier)
               .to(OUTPUT, Produced.with(STRING_SERDE, STRING_SERDE));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        return new TopologyTestDriver(builder.build(), props);
    }

    @BeforeEach
    void setUp() {
        driver = buildDriver(
                tempDir.resolve(UUID.randomUUID().toString()).toString(),
                "it-" + UUID.randomUUID());
        input  = driver.createInputTopic(INPUT,  STRING_SERDE.serializer(),   STRING_SERDE.serializer());
        output = driver.createOutputTopic(OUTPUT, STRING_SERDE.deserializer(), STRING_SERDE.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    /**
     * Three records piped in order R1, R2, R3.
     * R2 has a forward dependency on R3's source position, so it must be held until R3 is processed.
     * Expected output order: R1, R3, R2 — causal dependency satisfied before R2 is released.
     */
    @Test
    void outOfOrderRecordHeldUntilDependencySatisfied() {
        Partition p = new Partition(INPUT, 0);

        // R1 (offset=0): no dependencies — forwarded immediately
        input.pipeInput(new TestRecord<>("R1", "v1", clockHeaders(noopClock())));
        assertEquals(1, output.getQueueSize());

        // R2 (offset=1): needs offset=2 from input-0 — buffered
        input.pipeInput(new TestRecord<>("R2", "v2", clockHeaders(clock(Map.of(p, 2L)))));
        assertEquals(1, output.getQueueSize());

        // R3 (offset=2): no dependencies — forwarded, frontier advances to {input-0=2}, R2 drains
        input.pipeInput(new TestRecord<>("R3", "v3", clockHeaders(noopClock())));
        assertEquals(3, output.getQueueSize());

        List<TestRecord<String, String>> records = output.readRecordsToList();
        assertEquals("R1", records.get(0).key());
        assertEquals("R3", records.get(1).key());
        assertEquals("R2", records.get(2).key());
    }

    @Test
    void multipleBufferedRecordsReleasedInCausalOrder() {
        Partition p = new Partition(INPUT, 0);

        // offset=0: needs offset=2 — buffered
        input.pipeInput(new TestRecord<>("A", "v", clockHeaders(clock(Map.of(p, 2L)))));
        // offset=1: needs offset=2 — buffered
        input.pipeInput(new TestRecord<>("B", "v", clockHeaders(clock(Map.of(p, 2L)))));
        assertEquals(0, output.getQueueSize());

        // offset=2: no dependencies — forwarded, advances frontier, drains both A and B
        input.pipeInput(new TestRecord<>("C", "v", clockHeaders(noopClock())));

        List<TestRecord<String, String>> records = output.readRecordsToList();
        assertEquals(3, records.size());
        assertEquals("C", records.get(0).key());
        // A and B both satisfied — order between them may vary but they follow C
        assertEquals(2, records.subList(1, 3).stream()
                .filter(r -> r.key().equals("A") || r.key().equals("B")).count());
    }

    /**
     * Three-hop dependency chain piped in dependency order.
     * R_X (topic-x) needs topic-y/0 to be in frontier.
     * R_Y (topic-y) needs topic-z/0 to be in frontier.
     * R_Z (topic-z) has no dependency.
     *
     * When R_Z arrives it advances frontier. drainBuffer() must loop twice:
     *   iteration 1: R_Y is satisfied (needs topic-z/0), drained, frontier gains topic-y/0
     *   iteration 2: R_X is now satisfied (needs topic-y/0), drained
     * This exercises the while(progress) loop in CausalProcessor.drainBuffer().
     */
    @Test
    void cascadingTransitiveDrainExercisesWhileLoop() {
        String topicX = "topic-x";
        String topicY = "topic-y";
        String topicZ = "topic-z";

        StreamsBuilder builder = new StreamsBuilder();
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                (rec, reason) -> violations.add(reason),
                SERIALISER);
        builder.stream(topicX, Consumed.with(STRING_SERDE, STRING_SERDE))
                .merge(builder.stream(topicY, Consumed.with(STRING_SERDE, STRING_SERDE)))
                .merge(builder.stream(topicZ, Consumed.with(STRING_SERDE, STRING_SERDE)))
                .process(supplier)
                .to(OUTPUT, Produced.with(STRING_SERDE, STRING_SERDE));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cascade-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve(UUID.randomUUID().toString()).toString());

        try (TopologyTestDriver cascadeDriver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> inX = cascadeDriver.createInputTopic(topicX, STRING_SERDE.serializer(), STRING_SERDE.serializer());
            TestInputTopic<String, String> inY = cascadeDriver.createInputTopic(topicY, STRING_SERDE.serializer(), STRING_SERDE.serializer());
            TestInputTopic<String, String> inZ = cascadeDriver.createInputTopic(topicZ, STRING_SERDE.serializer(), STRING_SERDE.serializer());
            TestOutputTopic<String, String> out = cascadeDriver.createOutputTopic(OUTPUT, STRING_SERDE.deserializer(), STRING_SERDE.deserializer());

            // R_X needs frontier to have seen topic-y/0 at offset 0
            inX.pipeInput(new TestRecord<>("R_X", "v", clockHeaders(clock(Map.of(new Partition(topicY, 0), 0L)))));
            assertEquals(0, out.getQueueSize());

            // R_Y needs frontier to have seen topic-z/0 at offset 0
            inY.pipeInput(new TestRecord<>("R_Y", "v", clockHeaders(clock(Map.of(new Partition(topicZ, 0), 0L)))));
            assertEquals(0, out.getQueueSize());

            // R_Z has no dependency — forwarded immediately; cascading drain releases R_Y then R_X
            inZ.pipeInput(new TestRecord<>("R_Z", "v", clockHeaders(noopClock())));

            List<TestRecord<String, String>> records = out.readRecordsToList();
            assertEquals(3, records.size());
            assertEquals("R_Z", records.get(0).key());
            assertEquals("R_Y", records.get(1).key());
            assertEquals("R_X", records.get(2).key());
            assertTrue(violations.isEmpty());
        }
    }

    @Test
    void multiSourcePartitionFrontierTrackedIndependently() {
        String inputA = "input-a";
        String inputB = "input-b";

        StreamsBuilder builder = new StreamsBuilder();
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                (rec, reason) -> violations.add(reason),
                SERIALISER);
        builder.stream(inputA, Consumed.with(STRING_SERDE, STRING_SERDE))
                .merge(builder.stream(inputB, Consumed.with(STRING_SERDE, STRING_SERDE)))
                .process(supplier)
                .to(OUTPUT, Produced.with(STRING_SERDE, STRING_SERDE));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mp-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve(UUID.randomUUID().toString()).toString());

        try (TopologyTestDriver mpDriver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> topicA = mpDriver.createInputTopic(
                    inputA, STRING_SERDE.serializer(), STRING_SERDE.serializer());
            TestInputTopic<String, String> topicB = mpDriver.createInputTopic(
                    inputB, STRING_SERDE.serializer(), STRING_SERDE.serializer());
            TestOutputTopic<String, String> mpOutput = mpDriver.createOutputTopic(
                    OUTPUT, STRING_SERDE.deserializer(), STRING_SERDE.deserializer());

            // B needs Partition(inputA, 0) at offset 0; frontier is empty so it's buffered
            topicB.pipeInput(new TestRecord<>("B", "v", clockHeaders(clock(Map.of(new Partition(inputA, 0), 0L)))));
            assertEquals(0, mpOutput.getQueueSize());

            // A has no dependency — forwarded immediately, advances frontier to {inputA/0=0}
            // drainBuffer then releases B since {inputA/0=0} dominates {inputA/0=0}
            topicA.pipeInput(new TestRecord<>("A", "v", clockHeaders(noopClock())));

            List<TestRecord<String, String>> records = mpOutput.readRecordsToList();
            assertEquals(2, records.size());
            assertEquals("A", records.get(0).key());
            assertEquals("B", records.get(1).key());
        }
    }

    @Test
    void noViolationsForWellOrderedRecords() {
        input.pipeInput(new TestRecord<>("k1", "v", clockHeaders(noopClock())));
        input.pipeInput(new TestRecord<>("k2", "v", clockHeaders(noopClock())));
        input.pipeInput(new TestRecord<>("k3", "v", clockHeaders(noopClock())));

        assertEquals(3, output.getQueueSize());
        assertTrue(violations.isEmpty());
    }

    private static RecordHeaders clockHeaders(VectorClock clock) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CLOCK_HEADER, SERIALISER.serialise(clock)));
        return headers;
    }

    private static VectorClock noopClock() {
        return Map::of;
    }

    private static VectorClock clock(Map<Partition, Long> positions) {
        return () -> positions;
    }
}
