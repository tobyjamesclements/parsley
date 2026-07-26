package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.cause;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.message;

/**
 * A {@code parsley.*} key fails {@link CausalStreams} construction loudly. Parsley has no
 * configuration keys, so such a key wires nothing, and a deployment carrying one must learn at
 * startup — from a message naming the key and stating that the surface is empty — rather than
 * running with an operator who believes the key took effect. The sample key here is a
 * {@code parsley.coordination.*} one: the causal protocol carries no topology-epoch coordination
 * (joins need zero coordination), so nothing in that namespace can ever be honoured.
 */
class CausalStreamsParsleyKeyRejectionTest {

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(
            Map.of("c1", org.apache.kafka.common.Uuid.randomUuid()));

    /**
     * Constructing a {@link CausalStreams} over properties carrying a {@code parsley.coordination.*}
     * key throws {@code IllegalStateException} before any Kafka Streams instance is built, and the
     * message names both the offending key and the empty configuration surface.
     */
    @Test
    void aParsleyCoordinationKeyFailsConstructionLoudly() {
        CausalTopology topology = new CausalStreamsBuilder().topicAdmin(ADMIN)
                .stream("c1", Serdes.String(), Serdes.String())
                .process(PassthroughProcessor::new)
                .to("c2-sink", "c2", Serdes.String(), Serdes.String())
                .build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsley-key-rejection-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));
        props.put("parsley.coordination.epoch-events-topic", "epoch-events");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> new CausalStreams(topology, props),
                "a parsley.coordination.* key must fail CausalStreams construction");
        assertTrue(message(thrown).contains("parsley.coordination.epoch-events-topic"),
                "the failure must name the offending key: " + message(thrown));
        assertTrue(message(thrown).contains("no configuration keys"),
                "the failure must state that the configuration surface is empty, not read as an "
                        + "unknown-key typo: " + message(thrown));
    }

    /** A delegate that forwards its input unchanged — the topology shape is all this test needs. */
    private static final class PassthroughProcessor
            implements org.apache.kafka.streams.processor.api.Processor<String, String, String, String> {
        private org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

        @Override
        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(org.apache.kafka.streams.processor.api.Record<String, String> record) {
            context.forward(record);
        }
    }
}
