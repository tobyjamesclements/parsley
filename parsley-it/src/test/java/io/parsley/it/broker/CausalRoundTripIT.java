package io.parsley.it.broker;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.FenceToken;
import io.parsley.core.FenceTokens;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.CausalConsumer;
import io.parsley.kafka.CausalProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
        return newTopic(1);
    }

    private String newTopic(int partitions) throws Exception {
        String topic = "rt-it-" + UUID.randomUUID().toString().substring(0, 8);
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
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
        FenceToken<KafkaVectorClock> empty = FenceTokens.of(KafkaVectorClock.empty());

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
        FenceToken<KafkaVectorClock> empty = FenceTokens.of(KafkaVectorClock.empty());
        // R_dep's clock says: "only deliver me after TopicPartition(topic,0) reaches offset 2"
        FenceToken<KafkaVectorClock> depToken = FenceTokens.of(new KafkaVectorClock(Map.of(new TopicPartition(topic, 0), 2L)));

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
    void coPartitionedCrossTopicDependencyHeldUntilSatisfied(@TempDir Path tempDir) throws Exception {
        // The README's co-partitioning model: causally related records share the same
        // partition NUMBER across topics, so one Streams task (and one frontier) sees
        // partition N of every topic. Here B-0 depends on A-0; B-1 is independent.
        String topicA = newTopic(2);
        String topicB = newTopic(2);
        FenceToken<KafkaVectorClock> empty = FenceTokens.of(KafkaVectorClock.empty());
        FenceToken<KafkaVectorClock> dependsOnA0 = FenceTokens.of(
                new KafkaVectorClock(Map.of(new TopicPartition(topicA, 0), 0L)));

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            // Partition 0 of topic B: held until partition 0 of topic A reaches offset 0.
            producer.send(new ProducerRecord<>(topicB, 0, "R_held", "held"), dependsOnA0)
                    .get(10, TimeUnit.SECONDS);
            // Partition 1 of topic B: no dependencies, independent of the other task.
            producer.send(new ProducerRecord<>(topicB, 1, "R_independent", "ind"), empty)
                    .get(10, TimeUnit.SECONDS);
            // Partition 0 of topic A: the dependency trigger.
            producer.send(new ProducerRecord<>(topicA, 0, "R_trigger", "trig"), empty)
                    .get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topicA, topicB),
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return received.size() >= 3;
            });

            List<String> keys = received.stream().map(ConsumerRecord::key).toList();
            assertTrue(keys.containsAll(List.of("R_held", "R_independent", "R_trigger")), keys.toString());
            assertTrue(keys.indexOf("R_trigger") < keys.indexOf("R_held"),
                    "the dependent record must not be delivered before its trigger: " + keys);
        }
    }

    @Test
    void dropPolicyDropsUnresolvableRecordOnEviction(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        FenceToken<KafkaVectorClock> empty = FenceTokens.of(KafkaVectorClock.empty());
        // Impossible dependency: requires offset 1_000_000 which will never exist
        FenceToken<KafkaVectorClock> impossible = FenceTokens.of(
                new KafkaVectorClock(Map.of(new TopicPartition(topic, 0), 1_000_000L)));

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
