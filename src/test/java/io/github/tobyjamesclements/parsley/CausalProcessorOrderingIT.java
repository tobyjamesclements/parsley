package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end proof, against a real broker, that the causal processor delivers records in causal order
 * when fed by plain Kafka clients using the {@link CausalClock} edge API. A record produced on
 * {@code IN} after "consuming" {@code PREREQ@0} (its dependencies derived with
 * {@link CausalClock#from}) must not reach the output topic before the {@code PREREQ} record
 * it depends on — even when it is produced first.
 *
 * <p>This replaces the deleted standalone producer/consumer round-trip ITs: ordering is now a
 * property of the Streams processor, exercised here through the stateless edge operations.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalProcessorOrderingIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String PREREQ = "prereq";
    private static final String IN = "orders-in";
    private static final String OUT = "orders-out";

    /**
     * An {@code IN} record whose dependencies are {@code from(PREREQ@0)} is held by the processor and
     * only forwarded once {@code PREREQ@0} has been observed, so the output topic shows the
     * {@code PREREQ} record's output before the {@code IN} record's — even though {@code IN} is
     * produced first. Its own declared claim is not proof enough on its own: every record is checked
     * against this node's actual current state, never against its own stamp.
     *
     * Asserts the output topic delivers the two records in causal order (prereq before order).
     */
    @Test
    void causalOrderIsEnforcedAcrossTopicsThroughTheProcessor() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, OUT);

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(PREREQ, IN), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to(OUT, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams streams = new CausalStreams(topology, streamsConfig(bootstrap))) {
            streams.start();

            Properties resolverProps = new Properties();
            resolverProps.put("bootstrap.servers", bootstrap);

            // Dependencies a producer would attach after consuming PREREQ@0 (carries its position).
            ConsumerRecord<String, String> prereqConsumed =
                    new ConsumerRecord<>(PREREQ, 0, 0L, "pk", "prereq");
            CausalClock orderDeps = CausalClock.using(resolverProps).observe(prereqConsumed);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                // Produce the dependent record FIRST — it must be buffered, not delivered early.
                producer.send(orderDeps.stamp(new ProducerRecord<>(IN, "ik", "order"))).get();
                // Then the prerequisite it depends on (lands at PREREQ@0), unblocking it.
                producer.send(CausalClock.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OUT));
                List<String> delivered = pollValues(consumer, 2);
                assertEquals(List.of("PREREQ", "ORDER"), delivered,
                        "the dependent record must be delivered after the prerequisite it depends on");
            }
        }
    }

    /**
     * A resolver backed by a live broker (via {@link CausalClock#using(Properties)}) rejects a
     * topic that does not exist with an {@code IllegalArgumentException} (mapping the broker's
     * {@code UnknownTopicOrPartitionException}).
     *
     * Asserts resolving a non-existent topic throws {@code IllegalArgumentException}.
     */
    @Test
    void resolverRejectsATopicThatDoesNotExist() {
        String bootstrap = kafka.getBootstrapServers();
        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        CausalClock deps = CausalClock.using(resolverProps);
        assertThrows(IllegalArgumentException.class,
                () -> deps.observe(new ConsumerRecord<>("no-such-topic-" + UUID.randomUUID(), 0, 0L, "k", "v")),
                "a resolver must reject a non-existent topic with IllegalArgumentException");
    }

    /** A user processor that forwards each value upper-cased, so output values are distinguishable. */
    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static List<String> pollValues(KafkaConsumer<String, String> consumer, int count) {
        List<String> values = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(record -> values.add(record.value()));
            return values.size() >= count;
        });
        return values;
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ordering-it-" + UUID.randomUUID());
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

    private static Map<String, Object> consumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
