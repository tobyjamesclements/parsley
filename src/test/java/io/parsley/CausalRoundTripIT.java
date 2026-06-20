package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
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
        Uuid topicId = createTopic(bootstrap, TOPIC);

        try (CausalProducer<String, String> producer = CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
             CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                     List.of(TOPIC),
                     CausalBufferLimit.ofDuration(Duration.ofSeconds(5)),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap))
                     .addCausalTopic(new CausalTopic(TOPIC, topicId))
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

            // Every record was trivially or causally satisfied — none were ever forcibly evicted.
            for (ConsumerRecord<String, String> record : received) {
                assertEquals(Optional.of(CausalResult.SATISFIED), CausalResult.fromRecord(record),
                        "record must be stamped SATISFIED");
            }
        }
    }

    /**
     * A record with no causal-dependencies header (sent by a plain, non-Parsley producer) is
     * delivered like any other record — trivially satisfied, never buffered.
     *
     * <p>A plain {@link org.apache.kafka.clients.producer.KafkaProducer} sends a record with no
     * Parsley header, which the engine treats as {@link CausalDependencies#empty()}.
     *
     * Asserts that the record is delivered, stamped {@code CausalResult.SATISFIED}.
     */
    @Test
    void recordWithMissingDependencyHeaderIsDeliveredAsSatisfied() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "no-deps-events";
        Uuid topicId = createTopic(bootstrap, topic);

        try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                List.of(topic),
                CausalBufferLimit.ofDuration(Duration.ofSeconds(5)),
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                streamsConfig(bootstrap))
                .addCausalTopic(new CausalTopic(topic, topicId))
                .build()) {

            try (KafkaProducer<String, String> raw = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                raw.send(new ProducerRecord<>(topic, "k", "no-deps")).get();
            }

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return !received.isEmpty();
            });
            assertEquals(List.of("no-deps"), received.stream().map(ConsumerRecord::value).toList());
            assertEquals(Optional.of(CausalResult.SATISFIED), CausalResult.fromRecord(received.get(0)),
                    "a record with no dependency claim is trivially satisfied");
        }
    }

    /**
     * A record whose dependency is never satisfied is buffered until the configured
     * {@link CausalBufferLimit} fires, then forcibly evicted — but, per the always-forward model,
     * it is still delivered via {@link CausalConsumer#poll}, just stamped
     * {@code CausalResult.EVICTED} instead of {@code SATISFIED}. A custom state-store name is
     * honoured for the frontier store backing it.
     *
     * <p>A {@link CausalProducer} sends a record depending on an offset that never arrives, so it
     * buffers until the short duration limit fires.
     *
     * Asserts that the record is still delivered via {@code poll()}, stamped {@code EVICTED}, and
     * that the frontier under the custom store name advanced.
     */
    @Test
    void customStoreNameIsHonouredOnEviction() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "limit-events";
        Uuid topicId = createTopic(bootstrap, topic);

        // Deliberately never created on the broker — must never match a real registration.
        Uuid upstreamTopicId = Uuid.randomUuid();
        CausalDependencies producerDeps = CausalDependencies.builder()
                .require(new CausalPosition(upstreamTopicId, 0, 5L)).build();

        try (CausalProducer<String, String> producer = CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
             CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                     List.of(topic),
                     CausalBufferLimit.ofDuration(Duration.ofSeconds(2)),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap))
                     .addCausalTopic(new CausalTopic(topic, topicId))
                     .storeName("custom-store")
                     .build()) {

            producer.send(new ProducerRecord<>(topic, "k", "buffered-then-evicted"), producerDeps).get();

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return !received.isEmpty();
            });

            assertEquals(List.of("buffered-then-evicted"), received.stream().map(ConsumerRecord::value).toList(),
                    "an evicted record must still be delivered, never dropped");
            assertEquals(Optional.of(CausalResult.EVICTED), CausalResult.fromRecord(received.get(0)),
                    "an evicted record must be stamped EVICTED");
            assertFalse(consumer.frontier().positions().isEmpty(), "frontier under the custom store name advanced");
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

    private static Uuid createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            CreateTopicsResult result = admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1)));
            result.all().get();
            return result.topicId(topic).get();
        }
    }
}
