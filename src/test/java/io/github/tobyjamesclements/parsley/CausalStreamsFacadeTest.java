package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for the {@link CausalStreams} facade passthroughs ({@code metrics()},
 * {@code setStateListener}): an operator working through the facade must be able to reach the
 * underlying instance's metrics — Parsley registers its own per-task sensors there — and observe
 * state transitions, without needing a handle on the wrapped {@code KafkaStreams} (which the
 * facade deliberately does not expose).
 */
class CausalStreamsFacadeTest {

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(
            Map.of("c1", Uuid.randomUuid(), "c2", Uuid.randomUuid()));

    /** A single-stage passthrough topology over the {@link TestTopicAdmin} double. */
    private static CausalTopology topology() {
        return new CausalStreamsBuilder().topicAdmin(ADMIN)
                .stream("c1", Serdes.String(), Serdes.String())
                .process(PassthroughProcessor::new)
                .to("c2-sink", "c2", Serdes.String(), Serdes.String())
                .build();
    }

    private static Properties props(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));
        return props;
    }

    /**
     * {@code metrics()} reaches the underlying instance's registry: even before {@code start()},
     * the wrapped {@code KafkaStreams} registers client-level metrics, so the view is non-empty.
     */
    @Test
    void metricsExposesTheUnderlyingRegistry() {
        try (CausalStreams streams = new CausalStreams(topology(), props("facade-metrics-test"))) {
            Map<MetricName, ? extends Metric> metrics = streams.metrics();
            assertFalse(metrics.isEmpty(),
                    "metrics() must expose the underlying KafkaStreams metrics registry; an empty "
                            + "view means the passthrough is not wired to the wrapped instance");
        }
    }

    /**
     * {@code setStateListener} is accepted on a not-yet-started instance — the documented time to
     * register it, mirroring the uncaught-exception-handler passthrough.
     */
    @Test
    void setStateListenerIsAcceptedBeforeStart() {
        try (CausalStreams streams = new CausalStreams(topology(), props("facade-listener-test"))) {
            assertDoesNotThrow(() -> streams.setStateListener((newState, oldState) -> { }),
                    "setStateListener must delegate to the wrapped instance before start()");
        }
    }

    /** A delegate that forwards its input unchanged — the topology shape is all these tests need. */
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
