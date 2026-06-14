package io.parsley.it;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalConsumer;
import io.parsley.CausalProducer;
import io.parsley.VectorClock;
import io.parsley.avro.Order;
import io.parsley.avro.Price;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

    @Test
    void avroRecordsAcrossTopicsRoundTripThroughTheCausalConsumer() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String registryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        createTopics(bootstrap, ORDERS, PRICES);

        TopicPartition ordersTp = new TopicPartition(ORDERS, 0);
        TopicPartition pricesTp = new TopicPartition(PRICES, 0);

        Order order = new Order("o-1", "ACME", 5);
        Price price = new Price("ACME", 42.5);

        try (CausalProducer<String, SpecificRecord> producer = CausalProducer.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                "schema.registry.url", registryUrl));
             CausalConsumer<String, SpecificRecord> consumer = CausalConsumer.create(
                     List.of(ORDERS, PRICES),
                     BufferingPolicy.forwardUnsafe(BufferLimit.ofDuration(Duration.ofSeconds(5))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "avro-rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap, registryUrl))) {

            // Each record's clock simply marks its own position, so both self-satisfy and are admitted.
            producer.send(new ProducerRecord<>(PRICES, "ACME", price), VectorClock.empty().advance(pricesTp, 0)).get();
            producer.send(new ProducerRecord<>(ORDERS, "o-1", order), VectorClock.empty().advance(ordersTp, 0)).get();

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
            assertTrue(consumer.frontier().positions().containsKey(ordersTp), "frontier covers orders-0");
            assertTrue(consumer.frontier().positions().containsKey(pricesTp), "frontier covers prices-0");
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