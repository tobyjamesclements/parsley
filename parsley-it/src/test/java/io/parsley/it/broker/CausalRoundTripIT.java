package io.parsley.it.broker;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.FenceToken;
import io.parsley.Partition;
import io.parsley.internal.ImmutableVectorClock;
import io.parsley.streams.CausalConsumer;
import io.parsley.streams.CausalProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class CausalRoundTripIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private String newTopic() throws Exception {
        String topic = "rt-it-" + UUID.randomUUID().toString().substring(0, 8);
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        return topic;
    }

    private Map<String, Object> producerConfig() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
    }

    private Map<String, Object> streamsConfig(Path stateDir) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "rt-it-" + UUID.randomUUID());
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        cfg.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        cfg.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        cfg.put("auto.offset.reset", "earliest");
        return cfg;
    }

    @Test
    void wellOrderedRecordsDeliveredInOrder(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        FenceToken empty = FenceToken.of(ImmutableVectorClock.empty());

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "R1", "v1"), empty).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topic, "R2", "v2"), empty).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topic, "R3", "v3"), empty).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return received.size() >= 3;
            });

            assertEquals(List.of("R1", "R2", "R3"),
                    received.stream().map(ConsumerRecord::key).toList());
        }
    }

    @Test
    void causallyDependentRecordHeldUntilDependencySatisfied(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        FenceToken empty = FenceToken.of(ImmutableVectorClock.empty());
        // R_dep's clock says: "only deliver me after Partition(topic,0) reaches offset 2"
        FenceToken depToken = FenceToken.of(new ImmutableVectorClock(Map.of(new Partition(topic, 0), 2L)));

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            // offset 0: R_dep needs frontier>=2, buffered until R_trigger arrives
            producer.send(new ProducerRecord<>(topic, "R_dep", "dep"), depToken).get(10, TimeUnit.SECONDS);
            // offset 1: empty clock, forwarded immediately
            producer.send(new ProducerRecord<>(topic, "R_free", "free"), empty).get(10, TimeUnit.SECONDS);
            // offset 2: empty clock, forwarded immediately; frontier now >=2, R_dep drains
            producer.send(new ProducerRecord<>(topic, "R_trigger", "trigger"), empty).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return received.size() >= 3;
            });

            List<String> keys = received.stream().map(ConsumerRecord::key).toList();
            assertEquals("R_free", keys.get(0));
            assertEquals("R_trigger", keys.get(1));
            assertEquals("R_dep", keys.get(2));
        }
    }

    @Test
    void dropPolicyDropsUnresolvableRecordOnEviction(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        FenceToken empty = FenceToken.of(ImmutableVectorClock.empty());
        // Impossible dependency: requires offset 1_000_000 which will never exist
        FenceToken impossible = FenceToken.of(
                new ImmutableVectorClock(Map.of(new Partition(topic, 0), 1_000_000L)));

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "R_free", "free"), empty).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topic, "R_impossible", "imp"), impossible).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        // 3-second eviction window so the test doesn't need to wait 30 seconds
        BufferingPolicy dropPolicy = BufferingPolicy.drop(BufferLimit.ofDuration(Duration.ofSeconds(3)));

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), dropPolicy, Map.of(), streamsConfig(tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            // Wait for R_free (confirms streams is running and R_impossible is now buffered)
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return !received.isEmpty();
            });
            assertEquals("R_free", received.get(0).key());

            // Wait past the 3-second eviction window
            Thread.sleep(5_000);

            // R_impossible must have been dropped, not forwarded
            consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            assertEquals(1, received.size(), "Only R_free must be delivered; R_impossible dropped");
        }
    }
}
