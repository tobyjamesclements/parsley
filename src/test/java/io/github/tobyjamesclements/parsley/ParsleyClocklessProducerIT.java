package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A plain, clockless Kafka producer on a consumed topic (T3.1 IT d): a producer that stamps
 * nothing claims nothing, so its records are causally minimal by definition and deliver
 * immediately — no declaration, no "external source" registration, no coordination (E3; the v5
 * declared-external-source concept dissolved with D7). The member stamps the clockless records'
 * coordinates on consumption, so causal custody begins at the first Parsley hop exactly as if
 * the source had participated from the start.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyClocklessProducerIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String T1 = "t1";
    private static final String OUT = "out";

    /**
     * Two records produced with no {@code parsley-causal-clock} header at all must deliver
     * to the delegate immediately and in order, and the derived outputs' wire clocks must claim
     * the clockless records' exact input coordinates — stamped on consumption.
     *
     * Asserts both derivations arrive in input order and the second output's clock claims the
     * second input's coordinate (which offset-prefix dominance makes a claim on the first too).
     */
    @Test
    void clocklessRecordsDeliverImmediatelyAndAreStampedOnConsumption() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, T1, OUT);
        Uuid t1Id = topicId(bootstrap, T1);

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(T1), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("d:"))
                .to(OUT, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams streams = new CausalStreams(topology, streamsConfig(bootstrap))) {
            streams.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                // Deliberately NOT CausalClock.empty().stamp(...): no clock header at all.
                input.send(new ProducerRecord<>(T1, "k", "one")).get();
                input.send(new ProducerRecord<>(T1, "k", "two")).get();
            }

            List<ConsumerRecord<String, String>> outputs = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OUT));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            outputs.add(record);
                        }
                    });
                    return outputs.size() >= 2;
                });
            }

            assertEquals(List.of("d:one", "d:two"),
                    List.of(outputs.get(0).value(), outputs.get(1).value()),
                    "clockless records must deliver immediately and in input order — causally "
                            + "minimal, never held and never failed");
            assertTrue(wireClock(outputs.get(1)).offsetFor(t1Id, 0) >= 1,
                    "the second derivation's clock must claim the clockless input's coordinate "
                            + "t1@1 — custody begins at the first Parsley hop");
        }
    }

    /** A delegate that forwards every delivery with {@code prefix} prepended to its value. */
    private static ProcessorSupplier<String, String, String, String> prefixingProcessor(String prefix) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(prefix + record.value()));
            }
        };
    }

    /** A record with a value and no Parsley marker header — a business record, not a null message. */
    private static boolean isBusinessRecord(ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null;
    }

    private static ParsleyVectorClock wireClock(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        assertNotNull(header, "every stamped business record must carry the causal-dependencies header");
        return ParsleyVectorClock.fromBytes(header.value());
    }

    private static Uuid topicId(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
        }
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clockless-" + UUID.randomUUID());
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
                ConsumerConfig.GROUP_ID_CONFIG, "observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
