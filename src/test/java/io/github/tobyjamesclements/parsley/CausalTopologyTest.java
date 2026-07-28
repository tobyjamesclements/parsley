package io.github.tobyjamesclements.parsley;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-stage composition under TopologyTestDriver: two stages in one topology connected by
 * a shared {@link CausalTopic}, records flowing end to end with custody carried transitively —
 * the final output's stamp must still claim the original input's coordinate, two hops
 * upstream.
 */
class CausalTopologyTest {

    private static final CausalTopic<String, String> T1 =
            CausalTopic.of("t1", Serdes.String(), Serdes.String());
    private static final CausalTopic<String, String> MID =
            CausalTopic.of("mid", Serdes.String(), Serdes.String());
    private static final CausalTopic<String, String> T3 =
            CausalTopic.of("t3", Serdes.String(), Serdes.String());

    @TempDir
    Path stateDir;

    /** An ordinary Streams processor suffixing values and forwarding to the named sink. */
    static ProcessorSupplier<String, String, String, String> suffixTo(String suffix, String sink) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> context;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(Record<String, String> r) {
                context.forward(r.withValue(r.value() + suffix), sink);
            }
        };
    }

    /** Records traverse both stages; the final stamp claims the original t1 coordinate. */
    @Test
    void pipelineCarriesCustodyAcrossStages() {
        CausalStage first = CausalStage.builder("first")
                .source(T1, suffixTo(":a", "mid"))
                .sink(MID)
                .build();
        CausalStage second = CausalStage.builder("second")
                .source(MID, suffixTo(":b", "t3"))
                .sink(T3)
                .build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsley-multi-ttd");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        try (TopologyTestDriver driver =
                     new TopologyTestDriver(CausalTopology.of(first, second).testTopology(), props)) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> t3 =
                    driver.createOutputTopic("t3", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput("k", "v0", 1000L);
            t1.pipeInput("k", "v1", 1001L);

            var out = t3.readRecordsToList();
            assertEquals(List.of("v0:a:b", "v1:a:b"),
                    out.stream().map(r -> r.value()).toList(),
                    "records must traverse both stages in order");

            // Transitive custody: the second stage never consumed t1, yet its output's stamp
            // must claim the original t1 coordinate (folded from the first stage's stamp).
            VectorClock stamp = CausalHeaders.read(out.get(1).headers());
            assertNotNull(stamp);
            assertEquals(1L, stamp.get(CausalStage.testChannel("t1", 0)),
                    "custody of the t1 ancestry must survive the blind hop");
            assertTrue(stamp.get(CausalStage.testChannel("mid", 0)) >= 0,
                    "the direct cause on mid must be claimed");
        }
    }

    /** Composition validation: duplicate names and shared source topics fail loudly. */
    @Test
    void compositionValidatesNamesAndSources() {
        ProcessorSupplier<String, String, String, String> drop = () -> r -> {};

        CausalStage a = CausalStage.builder("same").source(T1, drop).build();
        CausalStage b = CausalStage.builder("same").source(MID, drop).build();
        assertThrows(IllegalArgumentException.class, () -> CausalTopology.of(a, b),
                "stages sharing a name must be rejected");

        CausalStage c = CausalStage.builder("c").source(T1, drop).build();
        CausalStage d = CausalStage.builder("d").source(T1, drop).build();
        assertThrows(IllegalArgumentException.class, () -> CausalTopology.of(c, d),
                "two stages sourcing one topic must be rejected (one source node per topic)");
    }

    /** A forward to an undeclared sink fails loudly at the stamping site. */
    @Test
    void emitToUndeclaredSinkFailsClosed() {
        CausalStage stage = CausalStage.builder("stage")
                .source(T1, suffixTo("", "t3"))
                .sink(MID) // t3 deliberately not declared
                .build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsley-undeclared-ttd");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        try (TopologyTestDriver driver = new TopologyTestDriver(stage.testTopology(), props)) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            boolean threw = false;
            try {
                t1.pipeInput("k", "v", 1000L);
            } catch (RuntimeException expected) {
                threw = true;
            }
            assertTrue(threw, "emitting to an undeclared sink must fail the task");
        }
    }
}
