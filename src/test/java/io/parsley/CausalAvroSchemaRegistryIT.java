package io.parsley;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.parsley.avro.Order;
import io.parsley.avro.Price;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round trip with <strong>Avro values backed by a real Confluent Schema Registry</strong>
 * over multiple input topics, each carrying its own schema. A {@link CausalProducer} serialises
 * {@link Order} and {@link Price} records with {@link KafkaAvroSerializer} (registering their schemas
 * against the running registry), and a {@link CausalConsumer} — whose Streams default value serde is
 * a {@link SpecificAvroSerde} pointed at the same registry — delivers them, reconstructing the
 * concrete Avro types.
 *
 * <p>This exercises the full {@code ParsleyConsumer} serde lifecycle against a real, stateful
 * Schema-Registry serde, including the {@code keySerde.close()/valueSerde.close()} performed on
 * {@link CausalConsumer#close()}.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalAvroSchemaRegistryIT {

    // Confluent Platform 8.0.x ships a Kafka 4.x broker, compatible with the kafka-streams 4.3 client.
    private static final String CP_VERSION = "8.0.0";
    private static final String ORDERS = "orders";
    private static final String PRICES = "prices";

    private final Network network = Network.newNetwork();

    @Container
    private final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CP_VERSION))
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    // An extra advertised listener other containers (the registry) reach Kafka on.
                    .withListener("kafka:19092");

    @Container
    private final GenericContainer<?> schemaRegistry =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + CP_VERSION))
                    .withNetwork(network)
                    .withExposedPorts(8081)
                    .dependsOn(kafka)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092");

    /**
     * An {@link Order} and a {@link Price} — each Avro-serialised under its own Schema Registry
     * subject — round-trip through a {@link CausalConsumer} that uses a {@link SpecificAvroSerde}
     * as its default value serde.
     *
     * <p>Neither record declares a causal dependency, so both are admitted immediately. The
     * concrete Avro types are reconstructed by the registry using {@code specific.avro.reader=true}.
     *
     * Asserts that the received {@code Order} and {@code Price} are field-for-field equal to the
     * originals, and that the frontier covers partition 0 for both topics.
     */
    @Test
    void avroRecordsAcrossTopicsRoundTripThroughTheCausalConsumer() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String registryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        createTopics(bootstrap, ORDERS, PRICES);

        Order order = new Order("o-1", "ACME", 5);
        Price price = new Price("ACME", 42.5);

        try (CausalProducer<String, SpecificRecord> producer = CausalProducers.<String, SpecificRecord>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                "schema.registry.url", registryUrl)).build();
             CausalConsumer<String, SpecificRecord> consumer = CausalConsumers.<String, SpecificRecord>builder(
                     List.of(ORDERS, PRICES),
                     CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(10))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "avro-rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap, registryUrl))
                     .topicAdmin(new MockAdminClient(ParsleyTopicAdmin.ofBootstrap(bootstrap)))
                     .build()) {

            // Neither record has a causal dependency — both are admitted immediately.
            producer.send(new ProducerRecord<>(PRICES, "ACME", price), CausalDependencies.empty()).get();
            producer.send(new ProducerRecord<>(ORDERS, "o-1", order), CausalDependencies.empty()).get();

            List<ConsumerRecord<String, SpecificRecord>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return received.size() >= 2;
            });

            // The concrete Avro types are reconstructed from the registry and equal field-by-field.
            Order receivedOrder = (Order) firstOfType(received, Order.class);
            Price receivedPrice = (Price) firstOfType(received, Price.class);
            assertEquals(order, receivedOrder, "the Order round-trips through Avro + Schema Registry");
            assertEquals(price, receivedPrice, "the Price round-trips through Avro + Schema Registry");

            // The frontier advanced over both topic-partitions.
            assertTrue(consumer.frontier().positions().stream()
                    .anyMatch(p -> p.partition() == 0),
                    "frontier covers orders-0");
            assertTrue(consumer.frontier().positions().stream()
                    .anyMatch(p -> p.partition() == 0),
                    "frontier covers prices-0");
        }
    }

    /**
     * An Avro {@link Order} buffered while waiting for a {@link Price} dependency is released in
     * causal order once the Price arrives, and is deserialized under the {@code orders-value}
     * Schema Registry subject rather than the buffer-store or changelog subject.
     *
     * <p>A drop policy with a short eviction window is used: if causal drain is broken the Order
     * is evicted, the await fails, and the {@code onViolation} callback surfaces the cause.
     *
     * Asserts that Price is delivered first, the Order is delivered second with field-for-field
     * equality to the original, and the buffered Order's header decodes to the producer's original
     * dependencies — not the delivery-time frontier.
     */
    @Test
    void bufferedAvroOrderIsReleasedByArrivingPriceAndDeserializesWithCorrectSchemaSubject() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String registryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        createTopics(bootstrap, ORDERS, PRICES);

        Uuid pricesId = CausalPosition.deriveUuid(PRICES);

        Order order = new Order("o-buf", "ACME", 10);
        Price price = new Price("ACME", 99.0);

        // drop policy: if the Order is evicted instead of drained causally, it is lost and the
        // await below fails — protecting against regressions where eviction masks a broken drain.
        AtomicReference<CausalViolation> eviction = new AtomicReference<>();

        try (CausalProducer<String, SpecificRecord> producer = CausalProducers.<String, SpecificRecord>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                "schema.registry.url", registryUrl)).build();
             CausalConsumer<String, SpecificRecord> consumer = CausalConsumers.<String, SpecificRecord>builder(
                     List.of(ORDERS, PRICES),
                     // Short eviction window with drop: if causal drain is broken the Order is lost,
                     // the await fails, and onViolation surfaces a clear cause.
                     CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "avro-buf-" + UUID.randomUUID()),
                     streamsConfig(bootstrap, registryUrl))
                     .onViolation(eviction::set)
                     .topicAdmin(new MockAdminClient(ParsleyTopicAdmin.ofBootstrap(bootstrap)))
                     .build()) {

            // Order declares it has seen prices-0@0 — it will be buffered until Price arrives.
            producer.send(new ProducerRecord<>(ORDERS, "o-buf", order),
                    CausalDependencies.builder().require(new CausalPosition(pricesId, 0, 0)).build()).get();

            // Poll briefly to confirm the Order is buffered and not yet delivered.
            List<ConsumerRecord<String, SpecificRecord>> received = new ArrayList<>();
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return received.isEmpty();
            });
            assertTrue(received.isEmpty(), "Order must be buffered before Price arrives");

            // Price has no causal dependency — admitted immediately, advancing frontier to prices-0@0.
            // This satisfies the Order's dependency and triggers its natural release.
            producer.send(new ProducerRecord<>(PRICES, "ACME", price),
                    CausalDependencies.empty()).get();

            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                CausalViolation v = eviction.get();
                if (v != null) throw new AssertionError(
                        "Order was evicted instead of drained causally: " + v);
                return received.size() >= 2;
            });

            // Causal order: Price (the dependency) must arrive before the Order it unblocked.
            assertEquals(PRICES, received.get(0).topic(), "Price must be delivered first (it unblocked Order)");
            assertEquals(ORDERS, received.get(1).topic(), "Order must be delivered second");

            // The buffered Order's Avro payload must deserialize correctly. If SRC_TOPIC were set to
            // "prices" or "outbox" instead of "orders", the deserializer would pick the wrong Schema
            // Registry subject and return the wrong type or throw.
            Price  receivedPrice = (Price)  received.get(0).value();
            Order  receivedOrder = (Order)  received.get(1).value();
            assertEquals(price, receivedPrice, "Price round-trips through Avro buffer path");
            assertEquals(order, receivedOrder, "Order round-trips through Avro buffer path");

            // The buffered Order's original producer dependencies must be restored (not replaced by frontier).
            assertEquals(Optional.of(CausalDependencies.builder().require(new CausalPosition(pricesId, 0, 0)).build()),
                    CausalDependencies.fromRecord(received.get(1)),
                    "Order must carry the producer's original dependencies, not the delivery-time frontier");
        }
    }

    private static SpecificRecord firstOfType(List<ConsumerRecord<String, SpecificRecord>> records, Class<?> type) {
        return records.stream()
                .map(ConsumerRecord::value)
                .filter(type::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no delivered record of type " + type.getSimpleName()));
    }

    private static Map<String, Object> streamsConfig(String bootstrap, String registryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-rt-app-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        // Reaches the SpecificAvroSerde when Streams configures the default serdes (and the buffer codec).
        config.put("schema.registry.url", registryUrl);
        config.put("specific.avro.reader", true);
        return config;
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(Set.copyOf(newTopics)).all().get();
        }
    }
}