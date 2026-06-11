package io.parsley.it;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationReason;
import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.internal.ImmutableVectorClock;
import io.parsley.serialisation.DefaultVectorClockSerialiser;
import io.parsley.streams.CausalProcessorSupplier;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.state.StoreBuilder;

import static org.junit.jupiter.api.Assertions.*;

class CausalProcessorTest {

    @TempDir
    Path tempDir;

    private static final DefaultVectorClockSerialiser SERIALISER = new DefaultVectorClockSerialiser();
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String CLOCK_HEADER = "parsley-vector-clock";

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> output;
    private final List<CausalViolationReason> violations = new ArrayList<>();

    @BeforeEach
    void setUp() {
        driver = buildDriver(BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))));
        input = driver.createInputTopic(INPUT, Serdes.String().serializer(), Serdes.String().serializer());
        output = driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        violations.clear();
    }

    @Test
    void noClockHeaderForwardedImmediatelyWithMissingHeaderViolation() {
        input.pipeInput(new TestRecord<>("k", "v"));

        assertEquals(1, output.getQueueSize());
        assertEquals("k", output.readRecord().key());
        assertEquals(1, violations.size());
        assertEquals(CausalViolationReason.MISSING_HEADER, violations.get(0));
    }

    @Test
    void satisfiedClockForwardedImmediately() {
        input.pipeInput(new TestRecord<>("k", "v", clockHeaders(ImmutableVectorClock.empty())));

        assertEquals(1, output.getQueueSize());
        assertTrue(violations.isEmpty());
    }

    @Test
    void unsatisfiedClockIsBufferedNotForwarded() {
        Partition p = new Partition(INPUT, 0);
        input.pipeInput(new TestRecord<>("k", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 5L)))));

        assertEquals(0, output.getQueueSize());
        assertTrue(violations.isEmpty());
    }

    @Test
    void bufferedRecordDrainedAfterFrontierAdvances() {
        Partition p = new Partition(INPUT, 0);

        // A needs frontier to have seen offset=1 from input-0; piped first so it gets offset=0
        input.pipeInput(new TestRecord<>("A", "v1", clockHeaders(new ImmutableVectorClock(Map.of(p, 1L)))));
        assertEquals(0, output.getQueueSize());

        // B has empty clock (always satisfied); gets offset=1, advances frontier to {input-0=1}, drains A
        input.pipeInput(new TestRecord<>("B", "v2", clockHeaders(ImmutableVectorClock.empty())));

        List<TestRecord<String, String>> records = output.readRecordsToList();
        assertEquals(2, records.size());
        assertEquals("B", records.get(0).key());
        assertEquals("A", records.get(1).key());
    }

    @Test
    void ignorePolicyEvictsBufferedRecordsOnPunctuator() {
        Partition p = new Partition(INPUT, 0);
        input.pipeInput(new TestRecord<>("k", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 99L)))));
        assertEquals(0, output.getQueueSize());

        driver.advanceWallClockTime(Duration.ofSeconds(31));

        assertEquals(1, output.getQueueSize());
        assertEquals(1, violations.size());
        assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(0));
    }

    @Test
    void dropPolicyEvictsWithoutForwarding() {
        driver.close();
        violations.clear();
        driver = buildDriver(BufferingPolicy.drop(BufferLimit.ofDuration(Duration.ofSeconds(30))));
        input = driver.createInputTopic(INPUT, Serdes.String().serializer(), Serdes.String().serializer());
        output = driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

        Partition p = new Partition(INPUT, 0);
        input.pipeInput(new TestRecord<>("k", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 99L)))));

        driver.advanceWallClockTime(Duration.ofSeconds(31));

        assertEquals(0, output.getQueueSize());
        assertEquals(1, violations.size());
        assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(0));
    }

    @Test
    void unresolvableClockForwardedWithUnresolvableClockViolation() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CLOCK_HEADER, new byte[]{0x00, 0x01})); // too short for readInt()
        input.pipeInput(new TestRecord<>("k", "v", headers));

        assertEquals(1, output.getQueueSize());
        assertEquals(1, violations.size());
        assertEquals(CausalViolationReason.UNRESOLVABLE_CLOCK, violations.get(0));
    }

    @Test
    void deadLetterPolicyEvictsToSinkWithoutForwarding() {
        List<ConsumerRecord<String, String>> deadLetterList = new ArrayList<>();
        var policy = new BufferingPolicy.DeadLetter(BufferLimit.ofDuration(Duration.ofSeconds(30)), "dead-topic");

        try (TopologyTestDriver dlDriver = buildDriverWithDeadLetter(policy, deadLetterList::add)) {
            TestInputTopic<String, String> dlInput = dlDriver.createInputTopic(
                    INPUT, Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> dlOutput = dlDriver.createOutputTopic(
                    OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

            Partition p = new Partition(INPUT, 0);
            dlInput.pipeInput(new TestRecord<>("k", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 99L)))));
            assertEquals(0, dlOutput.getQueueSize());

            dlDriver.advanceWallClockTime(Duration.ofSeconds(31));

            assertEquals(0, dlOutput.getQueueSize());
            assertEquals(1, deadLetterList.size());
            assertEquals(1, violations.size());
            assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(0));
        }
    }

    @Test
    void deadLetterPolicyInThreeArgConstructorThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new CausalProcessorSupplier<String, String>(
                        BufferingPolicy.deadLetter(BufferLimit.ofDuration(Duration.ofSeconds(30)), "dead"),
                        (rec, reason) -> {},
                        SERIALISER));
    }

    @Test
    void supplierStoresNamesMatchProcessorConstants() {
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                (rec, reason) -> {},
                SERIALISER);
        Set<String> names = supplier.stores().stream()
                .map(StoreBuilder::name)
                .collect(Collectors.toSet());
        assertTrue(names.contains("parsley-frontier"));
    }

    @Test
    void sizeLimitEvictsWhenBufferReachesLimit() {
        driver.close();
        violations.clear();
        driver = buildDriver(BufferingPolicy.ignore(BufferLimit.ofSize(1)));
        input = driver.createInputTopic(INPUT, Serdes.String().serializer(), Serdes.String().serializer());
        output = driver.createOutputTopic(OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());

        Partition p = new Partition(INPUT, 0);
        input.pipeInput(new TestRecord<>("A", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 99L)))));
        input.pipeInput(new TestRecord<>("B", "v", clockHeaders(new ImmutableVectorClock(Map.of(p, 99L)))));

        assertEquals(2, output.getQueueSize(), "Each record must be evicted immediately when SizeLimit=1");
        assertEquals(2, violations.size());
        violations.forEach(r -> assertEquals(CausalViolationReason.LIMIT_REACHED, r));
    }

    @Test
    void bytesLimitThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () ->
                buildDriver(BufferingPolicy.ignore(BufferLimit.ofBytes(1024))));
    }

    @Test
    void frontierAdvancementLimitThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () ->
                buildDriver(BufferingPolicy.ignore(BufferLimit.ofFrontierAdvancement(10))));
    }

    @Test
    void frontierPersistedToStateStoreAfterProcessing() {
        Partition p = new Partition(INPUT, 0);
        input.pipeInput(new TestRecord<>("k", "v", clockHeaders(ImmutableVectorClock.empty())));

        // Frontier store key is "f"; store name is "parsley-frontier"
        byte[] stored = driver.<String, byte[]>getKeyValueStore("parsley-frontier").get("f");
        assertNotNull(stored, "Frontier must be written to state store after processing");
        VectorClock persisted = SERIALISER.deserialise(stored);
        assertEquals(1, persisted.positions().size());
        assertEquals(0L, persisted.positions().get(p));
    }

    private TopologyTestDriver buildDriver(BufferingPolicy policy) {
        StreamsBuilder builder = new StreamsBuilder();
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                policy,
                (rec, reason) -> violations.add(reason),
                SERIALISER);
        KStream<String, String> stream = builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.String()));
        stream.process(supplier).to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve(UUID.randomUUID().toString()).toString());
        return new TopologyTestDriver(builder.build(), props);
    }

    private TopologyTestDriver buildDriverWithDeadLetter(
            BufferingPolicy.DeadLetter policy,
            Consumer<ConsumerRecord<String, String>> deadLetterSink) {
        StreamsBuilder builder = new StreamsBuilder();
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                policy,
                (rec, reason) -> violations.add(reason),
                SERIALISER,
                deadLetterSink);
        KStream<String, String> stream = builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.String()));
        stream.process(supplier).to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-dl-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve(UUID.randomUUID().toString()).toString());
        return new TopologyTestDriver(builder.build(), props);
    }

    private static RecordHeaders clockHeaders(ImmutableVectorClock clock) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CLOCK_HEADER, SERIALISER.serialise(clock)));
        return headers;
    }
}
