package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round trip against a real broker: a {@link CausalProducer} stamps causal dependencies,
 * and a {@link CausalConsumer} delivers the records in causal order with an advancing frontier.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalRoundTripIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String TOPIC = "events";

    /**
     * Five records forming a causal chain (each depending on the previous one) are produced and
     * then consumed; the consumer must deliver them in causal order with an advancing frontier.
     *
     * <p>Each delivered record still carries the producer's original causal-dependencies header,
     * accessible via {@link CausalDependencies#fromRecord}, so downstream services can propagate
     * causal context to their own clients.
     *
     * Asserts that all five values arrive in order, the frontier is non-empty and covers partition 0,
     * and every record decodes to its producer's original dependencies.
     */
    @Test
    void producedRecordsAreDeliveredInCausalOrderWithAdvancingFrontier() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopic(bootstrap, TOPIC);

        Uuid topicId = CausalPosition.deriveUuid(TOPIC);

        try (CausalProducer<String, String> producer = CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
             CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                     List.of(TOPIC),
                     CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap))
                     .topicAdmin(new MockAdminClient())
                     .build()) {

            // Each record depends on the previous one, forming a causal chain.
            for (int i = 0; i < 5; i++) {
                CausalDependencies deps = i == 0
                        ? CausalDependencies.empty()
                        : CausalDependencies.builder().require(new CausalPosition(topicId, 0, (long) (i - 1))).build();
                producer.send(new ProducerRecord<>(TOPIC, "k", "v" + i), deps);
            }

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(received::add);
                return received.size() >= 5;
            });

            assertEquals(List.of("v0", "v1", "v2", "v3", "v4"),
                    received.stream().map(ConsumerRecord::value).toList());
            assertFalse(consumer.frontier().positions().isEmpty(), "frontier must have advanced");
            assertTrue(consumer.frontier().positions().stream().anyMatch(p -> p.partition() == 0),
                    "frontier covers partition 0");

            // Each delivered record still carries the producer's causal-dependencies header, extractable
            // via the public API — this is the causal context a service would forward to a client.
            for (int i = 0; i < 5; i++) {
                CausalDependencies expected = i == 0
                        ? CausalDependencies.empty()
                        : CausalDependencies.builder().require(new CausalPosition(topicId, 0, (long) (i - 1))).build();
                assertEquals(Optional.of(expected), CausalDependencies.fromRecord(received.get(i)),
                        "record " + i + " should carry its producer's dependencies");
            }
        }
    }

    /**
     * A {@link CausalConsumer} built with a custom store name and a user-provided violation handler
     * delivers records that lack the causal-dependencies header (under {@code forwardUnsafe} policy)
     * and invokes the handler rather than the built-in no-op.
     *
     * <p>A plain {@link org.apache.kafka.clients.producer.KafkaProducer} sends a record with no
     * Parsley header, triggering a {@link CausalViolationReason#MISSING_HEADER} violation. The
     * custom store name exercises the frontier store rename path.
     *
     * Asserts that the record is delivered, the violation handler is invoked with reason
     * {@code MISSING_HEADER}, and the frontier under the custom store name is non-empty.
     */
    @Test
    void fullFactoryHonoursTheViolationHandlerAndCustomStoreName() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "no-deps-events";
        createTopic(bootstrap, topic);

        // The handler runs on the Streams thread, so capture must be thread-safe.
        List<CausalViolation> violations = new CopyOnWriteArrayList<>();

        try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                List.of(topic),
                CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))),
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                streamsConfig(bootstrap))
                .onViolation(violations::add)                      // the override that was previously hidden
                .storeName("custom-store")                         // custom state-store namespace
                .build()) {

            // A plain producer sends a record with NO causal dependencies header → a MISSING_HEADER violation.
            // Under forwardUnsafe policy, violation records are still delivered.
            try (KafkaProducer<String, String> raw = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                raw.send(new ProducerRecord<>(topic, "k", "no-deps")).get();
            }

            // The record is still delivered (forwardUnsafe policy forwards violation records)...
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return !received.isEmpty();
            });
            assertEquals(List.of("no-deps"), received.stream().map(ConsumerRecord::value).toList());

            // ...and the user's violation handler — not a hidden no-op — was invoked, despite the
            // custom store namespace driving the (custom-store-frontier) frontier store.
            await().atMost(Duration.ofSeconds(10)).until(() -> !violations.isEmpty());
            assertEquals(CausalViolationReason.MISSING_HEADER, violations.get(0).reason());
            assertFalse(consumer.frontier().positions().isEmpty(), "frontier under the custom store name advanced");
        }
    }

    /**
     * A {@link CausalConsumer} built with a {@link CausalBufferPolicy#deadLetter dead-letter} policy
     * routes a violating record to the configured {@link CausalConsumers.Builder#deadLetterSink sink}
     * instead of ever delivering it via {@link CausalConsumer#poll}.
     *
     * <p>A plain {@link org.apache.kafka.clients.producer.KafkaProducer} sends a record with no
     * Parsley header, triggering a {@link CausalViolationReason#MISSING_HEADER} violation, which the
     * dead-letter policy routes to the sink.
     *
     * Asserts that the sink receives the record — deserialized to its typed key/value, at its
     * original source topic/partition/offset, carrying the {@code parsley-dlq-*} headers — and that
     * {@code poll()} never returns it.
     */
    @Test
    void deadLetterPolicyRoutesViolatingRecordsToTheSinkInsteadOfDelivering() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "dlq-events";
        createTopic(bootstrap, topic);

        List<ConsumerRecord<String, String>> deadLettered = new CopyOnWriteArrayList<>();

        try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                List.of(topic),
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))),
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                streamsConfig(bootstrap))
                .deadLetterSink(deadLettered::add)
                .build()) {

            // A plain producer sends a record with NO causal dependencies header → a MISSING_HEADER
            // violation. Under a dead-letter policy, the record is routed to the sink, never delivered.
            try (KafkaProducer<String, String> raw = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                raw.send(new ProducerRecord<>(topic, "k", "no-deps")).get();
            }

            await().atMost(Duration.ofSeconds(60)).until(() -> !deadLettered.isEmpty());
            ConsumerRecord<String, String> dlq = deadLettered.get(0);
            assertEquals("no-deps", dlq.value());
            assertEquals("k", dlq.key());
            assertEquals(topic, dlq.topic());
            assertEquals(0, dlq.partition());
            assertEquals(0L, dlq.offset());
            assertEquals(CausalViolationReason.MISSING_HEADER.name(),
                    new String(dlq.headers().lastHeader(CausalViolation.DLQ_REASON_HEADER).value()),
                    "the dead-letter reason header identifies the violation");
            assertTrue(dlq.headers().lastHeader(CausalViolation.DLQ_REQUIRED_DEPENDENCIES_HEADER) != null,
                    "the dead-letter required-dependencies header is present");
            // Internal _parsley_* source-coordinate headers must be stripped before the sink sees them.
            dlq.headers().forEach(h -> assertFalse(h.key().startsWith("_parsley_"),
                    "internal header leaked to the dead-letter sink: " + h.key()));

            // The record was never delivered via poll().
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            }
            assertTrue(received.isEmpty(), "a dead-lettered record must never be delivered via poll()");
        }
    }

    /**
     * A record evicted by a {@link CausalViolationReason#LIMIT_REACHED} eviction reaches the
     * dead-letter sink still carrying the <em>producer's original, unstamped</em> causal
     * dependencies — not the delivery-time frontier.
     *
     * <p>This is the property that distinguishes the dead-letter path from {@link CausalConsumer#poll}:
     * dead-lettered records never pass through the outbox delegate that overwrites
     * {@code parsley-causal-dependencies} with the frontier, so the consumer only strips internal
     * {@code _parsley_*} headers ({@code stripInternalHeaders}) rather than restoring the original
     * ({@code restoreOriginalDependencies}). The sibling {@code MISSING_HEADER} test cannot exercise
     * this because there is no producer clock to preserve.
     *
     * <p>A {@link CausalProducer} sends a record depending on an offset that never arrives, so it
     * buffers until the short duration limit fires and routes it to the sink.
     *
     * Asserts that {@link CausalDependencies#fromRecord} on the dead-lettered record equals the
     * exact dependencies the producer stamped.
     */
    @Test
    void deadLetteredRecordPreservesTheProducersOriginalDependencies() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "dlq-limit-events";
        createTopic(bootstrap, topic);

        // Depend on a coordinate in an upstream topic the consumer never reads, so the dependency
        // can never be satisfied — the record buffers and is evicted as LIMIT_REACHED once the
        // duration limit fires. It must be a *different* topic: a dependency on the record's own
        // source coordinate is stripped as a self-reference (ParsleyEngine.withoutSelfReference).
        Uuid upstreamTopicId = CausalPosition.deriveUuid("never-produced-upstream");
        CausalDependencies producerDeps = CausalDependencies.builder()
                .require(new CausalPosition(upstreamTopicId, 0, 5L)).build();

        List<ConsumerRecord<String, String>> deadLettered = new CopyOnWriteArrayList<>();

        try (CausalProducer<String, String> producer = CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
             CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                     List.of(topic),
                     CausalBufferPolicy.deadLetter(CausalBufferLimit.ofDuration(Duration.ofSeconds(2))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap))
                     .topicAdmin(new MockAdminClient())
                     .deadLetterSink(deadLettered::add)
                     .build()) {

            producer.send(new ProducerRecord<>(topic, "k", "buffered-then-evicted"), producerDeps).get();

            await().atMost(Duration.ofSeconds(60)).until(() -> !deadLettered.isEmpty());
            ConsumerRecord<String, String> dlq = deadLettered.get(0);
            assertEquals("buffered-then-evicted", dlq.value());
            assertEquals(CausalViolationReason.LIMIT_REACHED.name(),
                    new String(dlq.headers().lastHeader(CausalViolation.DLQ_REASON_HEADER).value()),
                    "the record was evicted by the buffer limit");
            assertEquals(Optional.of(producerDeps), CausalDependencies.fromRecord(dlq),
                    "the dead-lettered record must carry the producer's original dependencies, unstamped");
        }
    }

    private static Map<String, Object> streamsConfig(String bootstrap) {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "rt-app-" + UUID.randomUUID(),
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }
}
