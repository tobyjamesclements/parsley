package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
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
 * Composition through the {@link Parsley} front door under TopologyTestDriver: two stages in
 * one topology connected by a shared {@link Topic}, records flowing end to end with custody
 * carried transitively — the final output's stamp must still claim the original input's
 * coordinate, two hops upstream.
 */
class ParsleyTest {

    private static final Topic<String, String> T1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> MID = Topic.of("mid", Codec.utf8(), Codec.utf8());
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

    /** Records traverse both stages; the final stamp claims the original t1 coordinate. */
    @Test
    void pipelineCarriesCustodyAcrossStages() {
        Stage first = Stage.named("first")
                .on(T1, m -> List.of(MID.send(m.key(), m.value() + ":a")))
                .into(MID)
                .build();
        Stage second = Stage.named("second")
                .on(MID, m -> List.of(T3.send(m.key(), m.value() + ":b")))
                .into(T3)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(first, second).testTopology(), props("parsley-multi-ttd"))) {
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
            assertEquals(1L, stamp.get(Stage.testChannel("t1", 0)),
                    "custody of the t1 ancestry must survive the blind hop");
            assertTrue(stamp.get(Stage.testChannel("mid", 0)) >= 0,
                    "the direct cause on mid must be claimed");
        }
    }

    /** Composition validation: duplicate names and shared source topics fail loudly. */
    @Test
    void compositionValidatesNamesAndSources() {
        Handler<String, String> drop = m -> List.of();

        Stage a = Stage.named("same").on(T1, drop).build();
        Stage b = Stage.named("same").on(MID, drop).build();
        assertThrows(IllegalArgumentException.class, () -> Parsley.of(a, b),
                "stages sharing a name must be rejected");

        Stage c = Stage.named("c").on(T1, drop).build();
        Stage d = Stage.named("d").on(T1, drop).build();
        assertThrows(IllegalArgumentException.class, () -> Parsley.of(c, d),
                "two stages sourcing one topic must be rejected (one source node per topic)");
    }

    /** A stage's tick topic is one of its sources, so composition guards it like any other. */
    @Test
    void tickTopicParticipatesInSourceDisjointness() {
        Handler<String, String> drop = m -> List.of();
        java.time.Duration second = java.time.Duration.ofSeconds(1);

        Stage ticking = Stage.named("a").on(T1, drop).ticks(second, tick -> List.of()).build();
        Stage poacher = Stage.named("b")
                .on(Topic.of("vc-a-ticks", Codec.utf8(), Codec.utf8()), drop)
                .build();
        assertThrows(IllegalArgumentException.class, () -> Parsley.of(ticking, poacher),
                "a stage sourcing another stage's tick topic must be rejected");

        Stage other = Stage.named("z").on(MID, drop).ticks(second, tick -> List.of()).build();
        assertNotNull(Parsley.of(ticking, other),
                "two ticking stages have distinct tick topics and must compose");
    }

    /** An emission to an undeclared sink fails loudly at the stamping site. */
    @Test
    void emissionToUndeclaredSinkFailsClosed() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .into(MID) // t3 deliberately not declared
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(
                Parsley.of(stage).testTopology(), props("parsley-undeclared-ttd"))) {
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

    /** Each testTopology() call assembles a fresh topology; drivers do not share state. */
    @Test
    void testTopologyIsFreshPerCall() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .into(T3)
                .build();
        Parsley parsley = Parsley.of(stage);

        try (TopologyTestDriver ignored = new TopologyTestDriver(
                parsley.testTopology(), props("parsley-fresh-a"))) {
            // A second driver over a second topology from the same composition must construct:
            // Kafka Streams consumes a Topology, so reuse would fail here.
            try (TopologyTestDriver second = new TopologyTestDriver(
                    parsley.testTopology(), props("parsley-fresh-b"))) {
                assertNotNull(second, "a second driver over a fresh topology must construct");
            }
        }
    }
}
