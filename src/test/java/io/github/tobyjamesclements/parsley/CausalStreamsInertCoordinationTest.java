package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code parsley.coordination.*} keys are inert at the {@link CausalStreams} runtime: they are
 * accepted (a configured deployment still constructs and would start), wire no coordination, and the
 * epoch-transition entry point reports the removal instead of pretending an epoch exists. The keys
 * are deleted outright in the next release (T3.3); this test pins the one-release inert behaviour.
 */
class CausalStreamsInertCoordinationTest {

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(
            Map.of("t1", org.apache.kafka.common.Uuid.randomUuid()));

    /**
     * A topology configured with every {@code parsley.coordination.*} key still constructs — the keys
     * must not fail startup while they remain accepted — and {@link
     * CausalStreams#requestEpochTransition()} always throws, naming the removal rather than "not
     * configured" (the caller did configure it; the truthful message is that the subsystem is gone).
     */
    @Test
    void coordinationConfigConstructsInertAndEpochTransitionReportsTheRemoval() {
        CausalTopology topology = new CausalStreamsBuilder().topicAdmin(ADMIN)
                .stream("t1", Serdes.String(), Serdes.String())
                .process(PassthroughProcessor::new)
                .to("out-sink", "out", Serdes.String(), Serdes.String())
                .build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "inert-coordination-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));
        props.put("parsley.coordination.epoch-events-topic", "epoch-events");
        props.put("parsley.coordination.domain-topics", "t1,out");
        props.put("parsley.coordination.member-apps", "inert-coordination-test");

        try (CausalStreams streams = new CausalStreams(topology, props)) {
            IllegalStateException thrown = assertThrows(IllegalStateException.class,
                    streams::requestEpochTransition,
                    "requestEpochTransition must throw: there is no epoch subsystem to transition");
            assertTrue(thrown.getMessage().contains("removed"),
                    "the message must name the removal, not claim coordination is unconfigured: "
                            + thrown.getMessage());
        }
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
