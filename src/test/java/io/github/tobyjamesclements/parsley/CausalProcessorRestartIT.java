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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that a record held by the causal buffer <strong>survives a processor restart</strong>: the
 * buffer is changelog-backed, so a held record persists across a {@link KafkaStreams} stop/start (with
 * a fresh local state directory, forcing a changelog restore) and still drains in causal order once
 * its dependency finally arrives.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalProcessorRestartIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PREREQ = "prereq";
    private static final String IN = "orders-in";
    private static final String OUT = "orders-out";

    /**
     * An {@code IN} record depending on {@code PREREQ@0} is buffered by a first processor instance;
     * after that instance is closed and a fresh one restores state from the changelog, producing the
     * {@code PREREQ} record drains the still-held record in causal order to the output topic.
     *
     * Asserts the output topic stays empty while the record is held (pre-restart), and after restart
     * delivers the prerequisite then the previously-held record.
     */
    @Test
    void aHeldRecordSurvivesAProcessorRestartAndDrainsWhenItsDependencyArrives() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, OUT);
        String appId = "restart-it-" + UUID.randomUUID();
        Path stateDir1 = Files.createTempDirectory("parsley-restart-1");
        Path stateDir2 = Files.createTempDirectory("parsley-restart-2");

        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        // Dependencies a producer would attach after consuming PREREQ@0.
        ConsumerRecord<String, String> prereqConsumed = new ConsumerRecord<>(PREREQ, 0, 0L, "pk", "prereq");
        CausalClock orderDeps = CausalClock.using(resolverProps).observe(prereqConsumed);

        // Phase 1: buffer the dependent record, confirm it is held, then shut the instance down cleanly.
        try (KafkaStreams streams = new KafkaStreams(newTopology(), streamsConfig(bootstrap, appId, stateDir1))) {
            streams.start();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(orderDeps.stamp(new ProducerRecord<>(IN, "ik", "order"))).get();
            }
            try (KafkaConsumer<String, String> out = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                out.subscribe(List.of(OUT));
                assertStaysEmpty(out, Duration.ofSeconds(8));
            }
            streams.close(Duration.ofSeconds(30));
        }

        // Phase 2: a fresh instance restores the held record from the changelog and drains it on arrival.
        try (KafkaStreams streams = new KafkaStreams(newTopology(), streamsConfig(bootstrap, appId, stateDir2))) {
            streams.start();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(CausalClock.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }
            try (KafkaConsumer<String, String> out = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                out.subscribe(List.of(OUT));
                List<String> delivered = pollValues(out, 2);
                assertEquals(List.of("PREREQ", "ORDER"), delivered,
                        "a record held before the restart must drain in causal order after the restart");
            }
            streams.close(Duration.ofSeconds(30));
        }
    }

    private static Topology newTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PREREQ, IN), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(upperCaser())
                        .addBufferStore("parsley")
                        .addSources(List.of(PREREQ, IN), Serdes.String(), Serdes.String())
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

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

    private static void assertStaysEmpty(KafkaConsumer<String, String> consumer, Duration window) {
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertTrue(records.isEmpty(),
                    "the dependent record must be held (not delivered) until its dependency arrives");
        }
    }

    private static List<String> pollValues(KafkaConsumer<String, String> consumer, int count) {
        List<String> values = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            return values.size() >= count;
        });
        return values;
    }

    private static Properties streamsConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
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
