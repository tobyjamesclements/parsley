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
 * an ordinary topic, records flowing end to end with custody carried transitively — the
 * final output's stamp must still claim the original input's coordinate, two hops upstream.
 */
class CausalTopologyTest {

    @TempDir
    Path stateDir;

    private static Processor<String, String, String, String> appending(String tag, String sink) {
        return new Processor<>() {
            private ProcessorContext<String, String> context;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(Record<String, String> r) {
                context.forward(r.withValue(r.value() + ":" + tag), sink);
            }
        };
    }

    /** Records traverse both stages; the final stamp claims the original t1 coordinate. */
    @Test
    void pipelineCarriesCustodyAcrossStages() {
        CausalStage<String, String, String, String> first =
                CausalStage.<String, String, String, String>builder()
                        .name("first")
                        .source("t1", Serdes.String(), Serdes.String())
                        .processor(() -> appending("a", "mid"))
                        .sink("mid", Serdes.String(), Serdes.String())
                        .build();
        CausalStage<String, String, String, String> second =
                CausalStage.<String, String, String, String>builder()
                        .name("second")
                        .source("mid", Serdes.String(), Serdes.String())
                        .processor(() -> appending("b", "t3"))
                        .sink("t3", Serdes.String(), Serdes.String())
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
        CausalStage<String, String, String, String> a =
                CausalStage.<String, String, String, String>builder()
                        .source("t1", Serdes.String(), Serdes.String())
                        .processor(() -> appending("a", "t2"))
                        .sink("t2", Serdes.String(), Serdes.String())
                        .build();
        CausalStage<String, String, String, String> b =
                CausalStage.<String, String, String, String>builder()
                        .source("t2", Serdes.String(), Serdes.String())
                        .processor(() -> appending("b", "t3"))
                        .sink("t3", Serdes.String(), Serdes.String())
                        .build();
        assertThrows(IllegalArgumentException.class, () -> CausalTopology.of(a, b),
                "unnamed stages share the default name and must be rejected");

        CausalStage<String, String, String, String> c =
                CausalStage.<String, String, String, String>builder()
                        .name("c")
                        .source("t1", Serdes.String(), Serdes.String())
                        .processor(() -> appending("c", "t4"))
                        .sink("t4", Serdes.String(), Serdes.String())
                        .build();
        CausalStage<String, String, String, String> d =
                CausalStage.<String, String, String, String>builder()
                        .name("d")
                        .source("t1", Serdes.String(), Serdes.String())
                        .processor(() -> appending("d", "t5"))
                        .sink("t5", Serdes.String(), Serdes.String())
                        .build();
        assertThrows(IllegalArgumentException.class, () -> CausalTopology.of(c, d),
                "two stages sourcing one topic must be rejected (one source node per topic)");
    }
}
