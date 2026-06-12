package io.parsley.it.broker;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.CausalConsumer;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
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
class CausalConsumerIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();

    private String newTopic() throws Exception {
        String topic = "cons-it-" + UUID.randomUUID().toString().substring(0, 8);
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        return topic;
    }

    private Map<String, Object> streamsConfig(String topic, Path stateDir) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "cons-it-" + UUID.randomUUID());
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        cfg.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        cfg.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        cfg.put("auto.offset.reset", "earliest");
        return cfg;
    }

    private void produceRaw(String topic, String key, String value) throws Exception {
        Map<String, Object> cfg = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add(new RecordHeader("parsley-vector-clock",
                    SERIALISER.serialise(KafkaVectorClock.empty())));
            producer.send(record).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void pollReturnsRecordProducedToTopic(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        produceRaw(topic, "hello", "world");

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return !received.isEmpty();
            });

            assertEquals("hello", received.get(0).key());
            assertEquals("world", received.get(0).value());
        }
    }

    @Test
    void pollEventuallyReturnsAllProducedRecords(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        Map<String, Object> cfg = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k" + i, "v" + i);
                record.headers().add(new RecordHeader("parsley-vector-clock",
                        SERIALISER.serialise(KafkaVectorClock.empty())));
                producer.send(record).get(10, TimeUnit.SECONDS);
            }
        }

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return received.size() >= 5;
            });

            assertEquals(5, received.size());
        }
    }

    @Test
    void frontierAdvancesAfterRecordsProcessed(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        produceRaw(topic, "key", "val");

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !((KafkaVectorClock) consumer.frontier()).positions().isEmpty();
            });

            assertFalse(((KafkaVectorClock) consumer.frontier()).positions().isEmpty());
        }
    }

    @Test
    void fenceTokenMatchesFrontier(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        produceRaw(topic, "key", "val");

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !((KafkaVectorClock) consumer.frontier()).positions().isEmpty();
            });

            assertEquals(consumer.frontier(), consumer.fenceToken().vectorClock());
        }
    }

    @Test
    void recordWithMissingClockHeaderIsForwardedImmediately(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();
        Map<String, Object> cfg = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {
            producer.send(new ProducerRecord<>(topic, "no-clock-key", "no-clock-val")).get(10, TimeUnit.SECONDS);
        }

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(30, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                return !received.isEmpty();
            });

            assertEquals("no-clock-key", received.get(0).key());
        }
    }

    @Test
    void pollTimesOutAndReturnsEmptyWhenNoRecords(@TempDir Path tempDir) throws Exception {
        String topic = newTopic();

        try (CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of(topic), BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig(topic, tempDir))) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            assertTrue(records.isEmpty());
        }
    }
}
