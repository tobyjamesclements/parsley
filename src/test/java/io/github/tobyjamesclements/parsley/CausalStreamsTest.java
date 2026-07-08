package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link CausalStreams#provisionDeadLetterTopic()} — the dead-letter topic provisioning
 * {@link CausalStreams#start()} runs before starting the underlying {@code KafkaStreams} instance —
 * against a stub {@link ParsleyTopicAdmin}, without a real broker. Called directly (it is
 * package-private for exactly this reason) so these tests never trigger the real
 * {@code kafkaStreams.start()} broker-connection attempt.
 */
class CausalStreamsTest {

    // Each KafkaStreams instance locks a process-id file under its state directory for as long as it is
    // open; a fresh temp directory per test avoids colliding with another (open or improperly-closed)
    // instance using the same application.id's default state directory.
    private static Properties props(String applicationId) throws IOException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        // Real KafkaStreams (unlike TopologyTestDriver) resolves this hostname while constructing its
        // own internal admin client, even though nothing here ever actually connects — "localhost"
        // resolves trivially and the port is never dialled by these tests.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("causal-streams-test-").toAbsolutePath().toString());
        return props;
    }

    private static CausalTopology topology() {
        ProcessorSupplier<String, String, String, String> noOp = () -> new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) {}
        };
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream("t1", Serdes.String(), Serdes.String())
                .process(noOp)
                .to("out-sink", "out", Serdes.String(), Serdes.String());
        return builder.topicAdmin(TestTopicAdmin.of(Map.of("t1", Uuid.randomUuid()))).build();
    }

    private static CausalStreams streamsWith(Properties props, ParsleyTopicAdmin admin) {
        return new CausalStreams(topology(), props, ParsleyMembershipStrategy.blockUntilDrained(), configs -> admin);
    }

    /**
     * Absent {@code parsley.deadletter.topic}, the topic name defaults to {@code {application.id}-deadletter}
     * with 1 partition (the default) — the same computation {@link CausalTopology#assemble} uses for
     * every stage's dead-letter sink, so provisioning always targets what they actually write to.
     *
     * Asserts the stub recorded exactly that topic name and partition count.
     */
    @Test
    void provisionsTheApplicationIdDerivedTopicByDefault() throws IOException {
        RecordingAdmin admin = new RecordingAdmin(null);

        try (CausalStreams streams = streamsWith(props("my-app"), admin)) {
            streams.provisionDeadLetterTopic();
        }

        assertEquals(List.of("my-app-deadletter"), admin.createdTopics, "the applicationId-derived default must be used");
        assertEquals(List.of(1), admin.createdPartitions, "the default partition count is 1");
    }

    /**
     * An explicit {@code parsley.deadletter.topic}/{@code parsley.deadletter.partitions} override is used
     * instead of the applicationId-derived default.
     *
     * Asserts the stub recorded the configured topic name and partition count.
     */
    @Test
    void provisionsTheConfiguredTopicNameAndPartitionsWhenOverridden() throws IOException {
        RecordingAdmin admin = new RecordingAdmin(null);
        Properties props = props("my-app");
        props.put(ParsleyConfig.DEADLETTER_TOPIC, "custom-dlq");
        props.put(ParsleyConfig.DEADLETTER_PARTITIONS, "3");

        try (CausalStreams streams = streamsWith(props, admin)) {
            streams.provisionDeadLetterTopic();
        }

        assertEquals(List.of("custom-dlq"), admin.createdTopics, "the configured topic name must override the default");
        assertEquals(List.of(3), admin.createdPartitions, "the configured partition count must override the default");
    }

    /**
     * A concurrent creation race — another instance of the same application creating the same topic
     * first — surfaces as an {@link ExecutionException} wrapping {@link TopicExistsException} (matching
     * the real {@code Admin.createTopics(...).all().get()} contract), which must be tolerated rather
     * than failing {@code start()}.
     *
     * Asserts {@code provisionDeadLetterTopic} completes without throwing.
     */
    @Test
    void toleratesTopicExistsExceptionFromAConcurrentCreation() throws IOException {
        RecordingAdmin admin = new RecordingAdmin(new ExecutionException(new TopicExistsException("already exists")));

        try (CausalStreams streams = streamsWith(props("my-app"), admin)) {
            streams.provisionDeadLetterTopic(); // must not throw
        }
    }

    /**
     * Any other failure (e.g. the broker is unreachable) must not be swallowed — only the specific
     * already-exists race is tolerated.
     *
     * Asserts {@code provisionDeadLetterTopic} rethrows as {@link IllegalStateException}.
     */
    @Test
    void doesNotToleratedAnyOtherFailure() throws IOException {
        RecordingAdmin admin = new RecordingAdmin(new RuntimeException("broker unreachable"));

        try (CausalStreams streams = streamsWith(props("my-app"), admin)) {
            assertThrows(IllegalStateException.class, streams::provisionDeadLetterTopic,
                    "only a topic-already-exists race is tolerated; every other failure must propagate");
        }
    }

    /** Records every {@code createTopic} call, optionally throwing {@code failure} instead of succeeding. */
    private static final class RecordingAdmin implements ParsleyTopicAdmin {

        private final @org.jspecify.annotations.Nullable Exception failure;
        final List<String> createdTopics = new ArrayList<>();
        final List<Integer> createdPartitions = new ArrayList<>();

        RecordingAdmin(@org.jspecify.annotations.Nullable Exception failure) {
            this.failure = failure;
        }

        @Override
        public Map<String, Uuid> topicIds(List<String> topics) {
            return Map.of();
        }

        @Override
        public Map<String, Integer> partitionCounts(List<String> topics) {
            return Map.of();
        }

        @Override
        public void createTopic(String name, int partitions) throws Exception {
            if (failure != null) {
                throw failure;
            }
            createdTopics.add(name);
            createdPartitions.add(partitions);
        }

        @Override
        public Map<String, String> cleanupPolicies(List<String> topics) {
            return Map.of();
        }

        @Override
        public void close() {
        }
    }
}
