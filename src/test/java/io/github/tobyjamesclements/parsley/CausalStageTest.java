package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * per-source Streams processors, the broker-less {@code testTopology()} wiring. Protocol correctness
 * under concurrency, crashes, and EOS lives in the simulator suite — TTD is single-threaded
 * and transactionless, so these tests only assert the adapter's plumbing.
 */
class CausalStageTest {

    private static final CausalTopic<String, String> T1 =
            CausalTopic.of("t1", Serdes.String(), Serdes.String());
    private static final CausalTopic<String, String> T2 =
            CausalTopic.of("t2", Serdes.String(), Serdes.String());
    private static final CausalTopic<String, String> T3 =
            CausalTopic.of("t3", Serdes.String(), Serdes.String());

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> t1;
    private TestInputTopic<String, String> t2;
    private TestOutputTopic<String, String> t3;

    /** An ordinary Streams processor prefixing values and forwarding to the named sink. */
    static ProcessorSupplier<String, String, String, String> prefixTo(String prefix, String sink) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> context;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(Record<String, String> r) {
                context.forward(r.withValue(prefix + r.value()), sink);
            }
        };
    }

    @BeforeEach
    void setUp() {
        CausalStage stage = CausalStage.builder("stage")
                .source(T1, prefixTo("out:", "t3"))
                .source(T2, prefixTo("out:", "t3"))
                .sink(T3)
                .build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsley-ttd");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(stage.testTopology(), props);
        t1 = driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
        t2 = driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
        t3 = driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    /** An unstamped record delivers immediately and its output carries a stamp claiming it. */
    @Test
    void deliversAndStampsOutputs() {
        t1.pipeInput("k", "a", 1000L);
        var out = t3.readRecordsToList();
        assertEquals(1, out.size(), "one delivery, one emit");
        assertEquals("out:a", out.get(0).value());

        VectorClock stamp = CausalHeaders.read(out.get(0).headers());
        assertNotNull(stamp, "output must carry a dependency clock");
        assertEquals(0L, stamp.get(CausalStage.testChannel("t1", 0)),
                "stamp must claim the delivered input");
    }

    /** A record whose dependency has not been delivered is held, then released in order. */
    @Test
    void holdsUntilDependencyDelivered() {
        // A t2 record claiming t1@0 arrives before t1@0 itself: the gate must hold it.
        var headers = new RecordHeaders();
        CausalHeaders.write(headers, VectorClock.of(CausalStage.testChannel("t1", 0), 0));
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
        assertEquals(0L, stamp.get(CausalStage.testChannel("t1", 0)));
        assertEquals(0L, stamp.get(CausalStage.testChannel("t2", 0)));
    }

    /** An undecodable clock header fails the task rather than reading as empty. */
    @Test
    void corruptClockFailsClosed() {
        var headers = new RecordHeaders();
        headers.add(CausalHeaders.CLOCK, new byte[] {9, 9, 9});
        var record = new TestRecord<>("k", "x", headers, 1000L);
        boolean threw = false;
        try {
            t1.pipeInput(record);
        } catch (RuntimeException expected) {
            threw = true;
        }
        assertTrue(threw, "a corrupt clock header must fail the task, never read as empty");
    }
}
