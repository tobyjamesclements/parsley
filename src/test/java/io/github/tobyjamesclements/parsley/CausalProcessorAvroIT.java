package io.github.tobyjamesclements.parsley;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.tobyjamesclements.parsley.avro.Order;
import io.github.tobyjamesclements.parsley.avro.Price;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end Avro round trip against a <strong>real Confluent Schema Registry</strong> through the
 * causal processor. An {@link Order} produced (stamped via the {@link CausalDependencies} edge API)
 * before the {@link Price} it depends on is held in the changelog-backed buffer — Avro-serialised
 * under its own {@code orders-value} subject against the live registry — and drains in causal order
 * once the Price arrives, deserialised back to an equal {@code Order}.
 *
 * <p>This is the processor-based replacement for the deleted standalone-consumer Avro IT; the
 * per-source-topic subject resolution is also covered by {@code CausalProcessorsAvroTopologyTest}
 * against an in-process mock registry.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalProcessorAvroIT {

    private static final String CP_VERSION = "8.0.0";
    private static final String PRICES = "prices";
    private static final String ORDERS = "orders";
    private static final String OUT = "causal-out";

    private final Network network = Network.newNetwork();

    @Container
    private final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CP_VERSION))
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
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
     * An Avro {@code Order} stamped (via {@code from(prices@0)}) and produced before the {@code Price}
     * it depends on is buffered and released in causal order once the Price advances the frontier. The
     * held Order round-trips through the buffer under its source-topic subject against the real
     * registry, arriving field-for-field equal to the original.
     *
     * Asserts the output topic delivers the Price first and the Order second, each equal to the
     * original Avro record.
     */
    @Test
    void aBufferedAvroOrderDrainsInCausalOrderThroughTheRealRegistry() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String registryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        createTopics(bootstrap, PRICES, ORDERS, OUT);

        Order order = new Order("o-buf", "ACME", 10);
        Price price = new Price("ACME", 99.0);

        try (KafkaStreams streams = new KafkaStreams(
                topology(registryUrl), streamsConfig(bootstrap, registryUrl))) {
            streams.start();

            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                CausalTopics topics = CausalTopics.of(admin);
                // The Order's dependencies, as derived after consuming prices@0.
                ConsumerRecord<String, SpecificRecord> priceConsumed =
                        new ConsumerRecord<>(PRICES, 0, 0L, "ACME", price);
                CausalDependencies orderDeps = CausalDependencies.using(topics).observe(priceConsumed);

                try (KafkaProducer<String, SpecificRecord> producer =
                             new KafkaProducer<>(producerConfig(bootstrap, registryUrl))) {
                    // Produce the dependent Order FIRST — it must be buffered (Avro bytes in the store).
                    producer.send(orderDeps.stamp(new ProducerRecord<>(ORDERS, "o-buf", order))).get();
                    // Then the Price it depends on, which unblocks it.
                    producer.send(CausalDependencies.empty()
                            .stamp(new ProducerRecord<>(PRICES, "ACME", price))).get();
                }
            }

            try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, SpecificRecord>(
                    consumerConfig(bootstrap, registryUrl))) {
                consumer.subscribe(List.of(OUT));
                List<SpecificRecord> delivered = pollValues(consumer, 2);
                assertEquals(price, delivered.get(0),
                        "the Price (the dependency) must be delivered first, equal to the original");
                assertEquals(order, delivered.get(1),
                        "the buffered Order must drain second, round-tripped through Avro equal to the original");
            }
        }
    }

    private static Topology topology(String registryUrl) {
        Serde<SpecificRecord> avro = avroValueSerde(registryUrl);
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PRICES, ORDERS), Consumed.with(Serdes.String(), avro))
                .process(CausalProcessors.builder(passthrough())
                        .addBufferStore("parsley")
                        .addBuffers(List.of(PRICES, ORDERS), Serdes.String(), avro)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), avro));
        return builder.build();
    }

    private static ProcessorSupplier<String, SpecificRecord, String, SpecificRecord> passthrough() {
        return () -> new Processor<>() {
            private ProcessorContext<String, SpecificRecord> ctx;

            @Override
            public void init(ProcessorContext<String, SpecificRecord> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, SpecificRecord> record) {
                ctx.forward(record);
            }
        };
    }

    private static Serde<SpecificRecord> avroValueSerde(String registryUrl) {
        SpecificAvroSerde<SpecificRecord> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                "schema.registry.url", registryUrl,
                "specific.avro.reader", true,
                // Order and Price share the output topic; key the subject by record type, not topic,
                // so both schemas can coexist (Parsley resolves the serde by source topic regardless).
                "value.subject.name.strategy", RecordNameStrategy.class.getName()), false);
        return serde;
    }

    private static List<SpecificRecord> pollValues(
            org.apache.kafka.clients.consumer.KafkaConsumer<String, SpecificRecord> consumer, int count) {
        List<SpecificRecord> values = new ArrayList<>();
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            return values.size() >= count;
        });
        return values;
    }

    private static Properties streamsConfig(String bootstrap, String registryUrl) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-it-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        config.put("schema.registry.url", registryUrl);
        config.put("specific.avro.reader", true);
        return config;
    }

    private static Map<String, Object> producerConfig(String bootstrap, String registryUrl) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                "schema.registry.url", registryUrl,
                "value.subject.name.strategy", RecordNameStrategy.class.getName());
    }

    private static Map<String, Object> consumerConfig(String bootstrap, String registryUrl) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
                "schema.registry.url", registryUrl,
                "specific.avro.reader", true);
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
        }
    }
}
