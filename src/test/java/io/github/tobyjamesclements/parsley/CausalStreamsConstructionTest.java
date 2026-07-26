package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link CausalStreams} construction against its two JVM-wide registrations
 * ({@link ParsleyOwnOutputRegistry} and {@link ParsleyTopicIdentityWatch}). The registrations must
 * precede the underlying {@code KafkaStreams} construction (the producer interceptor resolves them
 * from config at client build time), so a construction that fails <em>after</em> them hands the
 * caller no instance to {@code close()} — the constructor itself must roll them back, or every
 * failed construction leaks a registry entry for the JVM's lifetime.
 */
class CausalStreamsConstructionTest {

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

    /** A state directory per instance, so concurrent instances cannot collide on one path. */
    @RegisterExtension
    static final TestStateDirectories STATE_DIRS = new TestStateDirectories("construction-test-");

    /** A Streams configuration valid up to the point {@code new KafkaStreams} parses it. */
    private static Properties validProps(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRS.create().toAbsolutePath().toString());
        return props;
    }

    /**
     * A constructor that fails inside {@code new KafkaStreams} (here: an unresolvable
     * {@code metric.reporters} class, parsed only at client build time — after Parsley's topology
     * assembly and validation passed) leaves both JVM-wide registries exactly as it found them.
     */
    @Test
    void failedConstructionRollsBackItsJvmWideRegistrations() {
        Properties props = validProps("construction-rollback-test");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, "does.not.Exist");
        int registriesBefore = ParsleyOwnOutputRegistry.REGISTRIES.size();
        int watchesBefore = ParsleyTopicIdentityWatch.WATCHES.size();

        assertThrows(RuntimeException.class, () -> new CausalStreams(topology(), props),
                "an unresolvable metric.reporters class must fail KafkaStreams construction");

        assertEquals(registriesBefore, ParsleyOwnOutputRegistry.REGISTRIES.size(),
                "a failed construction must unregister its own-output registry — the caller has no "
                        + "instance to close(), so a surviving entry leaks for the JVM's lifetime");
        assertEquals(watchesBefore, ParsleyTopicIdentityWatch.WATCHES.size(),
                "a failed construction must unregister its topic-identity watch — the caller has no "
                        + "instance to close(), so a surviving entry leaks for the JVM's lifetime");
    }

    /**
     * The control for the rollback test: a construction that succeeds registers exactly one entry
     * in each registry, and {@code close()} removes both.
     */
    @Test
    void successfulConstructionRegistersOnceAndCloseUnregisters() {
        int registriesBefore = ParsleyOwnOutputRegistry.REGISTRIES.size();
        int watchesBefore = ParsleyTopicIdentityWatch.WATCHES.size();

        CausalStreams streams = new CausalStreams(topology(), validProps("construction-success-test"));
        assertEquals(registriesBefore + 1, ParsleyOwnOutputRegistry.REGISTRIES.size(),
                "construction must register exactly one own-output registry");
        assertEquals(watchesBefore + 1, ParsleyTopicIdentityWatch.WATCHES.size(),
                "construction must register exactly one topic-identity watch");

        streams.close();
        assertEquals(registriesBefore, ParsleyOwnOutputRegistry.REGISTRIES.size(),
                "close() must unregister the own-output registry");
        assertEquals(watchesBefore, ParsleyTopicIdentityWatch.WATCHES.size(),
                "close() must unregister the topic-identity watch");
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
