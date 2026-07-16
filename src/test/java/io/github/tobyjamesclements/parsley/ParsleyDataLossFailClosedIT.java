package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the causal-safety response to a data-loss forward jump. Every causal source is declared with
 * {@code AutoOffsetReset.none()} (see {@link CausalTopology}), so when a committed offset has fallen out of
 * range — retention or {@code deleteRecords} outran a lagging consumer — Kafka Streams fails the task fast
 * at fetch rather than silently resetting forward over the lost records. Without {@code none()} the
 * consumer would jump to the new log-start and {@link ParsleyFrontier#bridge} would fold the skipped real
 * records as if they were transaction markers, releasing dependents before their causes.
 *
 * <p>The scenario: commit offset 2 for the application's group, then delete records below offset 5 so the
 * log-start advances past 2, leaving the committed offset out of range. Because a committed offset already
 * exists, pre-start seeding leaves it untouched (only a genuine first start is seeded), so the out-of-range
 * offset reaches the {@code none()} consumer and fails the instance into {@code ERROR}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyDataLossFailClosedIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "in";
    private static final String OUT = "out";

    /**
     * A committed offset below the (retention-advanced) log-start fails the instance closed under
     * {@code none()}, instead of silently jumping forward over the lost records.
     */
    @Test
    void anOutOfRangeCommittedOffsetFailsTheInstanceClosed() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, OUT);
        String applicationId = "data-loss-it-" + UUID.randomUUID();

        // Ten records on IN (offsets 0..9), each with an empty causal-dependencies header.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(IN, "k", "v" + i);
                record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
                producer.send(record).get();
            }
        }

        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            // Commit offset 2 for the application's group (the group is empty — nothing has started).
            admin.alterConsumerGroupOffsets(applicationId,
                    Map.of(new TopicPartition(IN, 0), new OffsetAndMetadata(2L))).all().get();
            // Delete records below offset 5, advancing the log-start past the committed offset 2 — the
            // committed offset is now out of range, exactly as retention outrunning a lagging consumer.
            admin.deleteRecords(Map.of(new TopicPartition(IN, 0), RecordsToDelete.beforeOffset(5L))).all().get();
        }

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(IN, Serdes.String(), Serdes.String())
                .process(passthrough())
                .to(OUT, Serdes.String(), Serdes.String())
                .build();

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        try (CausalStreams streams = new CausalStreams(topology, streamsConfig(bootstrap, applicationId))) {
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.compareAndSet(null, throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();

            await().atMost(Duration.ofSeconds(60)).until(() -> streams.state() == KafkaStreams.State.ERROR);
            assertSame(KafkaStreams.State.ERROR, streams.state(),
                    "an out-of-range committed offset must fail the instance closed under none(), never "
                            + "silently reset forward over the lost records");
            Throwable failure = uncaught.get();
            assertNotNull(failure, "the instance must fail with an uncaught exception, not stall silently");
            assertTrue(failedOnOutOfRangeUnderNone(failure),
                    "the failure must be the out-of-range / no-valid-reset error under none(), proving it "
                            + "fails closed for the right reason rather than any incidental startup error: "
                            + failure);
        }
    }

    /** Whether {@code failure}'s cause chain is the out-of-range-under-none() failure, not some other error. */
    private static boolean failedOnOutOfRangeUnderNone(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof OffsetOutOfRangeException) {
                return true;
            }
            String message = t.getMessage();
            if (message != null && message.contains("No valid committed offset")) {
                return true;
            }
        }
        return false;
    }

    private static ProcessorSupplier<String, String, String, String> passthrough() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record);
            }
        };
    }

    private static Properties streamsConfig(String bootstrap, String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }
}
