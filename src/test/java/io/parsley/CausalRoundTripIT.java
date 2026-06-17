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
 * End-to-end round trip against a real broker: a {@link CausalProducer} stamps vector clocks,
 * and a {@link CausalConsumer} delivers the records in causal order with an advancing frontier.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalRoundTripIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String TOPIC = "events";

    @Test
    void producedRecordsAreDeliveredInCausalOrderWithAdvancingFrontier() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopic(bootstrap, TOPIC);

        Uuid topicId = CausalPosition.nameUuid(TOPIC);

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
                        : CausalDependencies.empty().advance(topicId, 0, (long) (i - 1));
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

            // Each delivered record still carries the producer's vector-clock header, extractable
            // via the public API — this is the causal context a service would forward to a client.
            for (int i = 0; i < 5; i++) {
                CausalDependencies expected = i == 0
                        ? CausalDependencies.empty()
                        : CausalDependencies.empty().advance(topicId, 0, (long) (i - 1));
                assertEquals(Optional.of(expected), CausalDependencies.fromRecord(received.get(i)),
                        "record " + i + " should carry its producer's clock");
            }
        }
    }

    @Test
    void fullFactoryHonoursTheViolationHandlerAndCustomStoreName() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String topic = "no-clock-events";
        createTopic(bootstrap, topic);

        // The handler runs on the Streams thread, so capture must be thread-safe.
        List<CausalViolation> violations = new CopyOnWriteArrayList<>();

        try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                List.of(topic),
                CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))),
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                streamsConfig(bootstrap))
                .onViolation(violations::add)                      // the override that was previously hidden
                .storeName("custom-store")                         // custom state-store namespace
                .build()) {

            // A plain producer sends a record with NO causal clock header → a MISSING_HEADER violation.
            try (KafkaProducer<String, String> raw = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                raw.send(new ProducerRecord<>(topic, "k", "no-clock")).get();
            }

            // The record is still delivered (missing-header records are forwarded)...
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return !received.isEmpty();
            });
            assertEquals(List.of("no-clock"), received.stream().map(ConsumerRecord::value).toList());

            // ...and the user's violation handler — not a hidden no-op — was invoked, despite the
            // custom store namespace driving the (custom-store-frontier) frontier store.
            await().atMost(Duration.ofSeconds(10)).until(() -> !violations.isEmpty());
            assertEquals(CausalViolationReason.MISSING_HEADER, violations.get(0).reason());
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

    private static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }
}
