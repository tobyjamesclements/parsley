package io.parsley.it.broker;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.FenceToken;
import io.parsley.core.FenceTokens;
import io.parsley.kafka.CausalProcessorSupplier;
import io.parsley.kafka.CausalProducer;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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

/**
 * End-to-end test of the documented dead-letter pattern: a custom topology built around
 * {@code CausalProcessorSupplier}'s 4-argument constructor whose sink produces evicted
 * records to a real dead-letter topic. ({@code CausalConsumer} cannot express this — it
 * rejects DeadLetter policies.)
 */
@Testcontainers
class CausalDeadLetterIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private String newTopic(String prefix) throws Exception {
        String topic = prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
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

    @Test
    void evictedRecordRoutedToDeadLetterTopicNotOutput(@TempDir Path tempDir) throws Exception {
        String input = newTopic("dlt-in");
        String output = newTopic("dlt-out");
        String deadLetterTopic = newTopic("dlt-dead");

        FenceToken<KafkaVectorClock> empty = FenceTokens.of(KafkaVectorClock.empty());
        FenceToken<KafkaVectorClock> impossible = FenceTokens.of(
                new KafkaVectorClock(Map.of(new TopicPartition(input, 0), 1_000_000L)));

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(input, "R_free", "free"), empty).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(input, "R_impossible", "imp"), impossible).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        KafkaProducer<String, String> deadLetterProducer = new KafkaProducer<>(producerConfig());
        var policy = new BufferingPolicy.DeadLetter(
                BufferLimit.ofDuration(Duration.ofSeconds(3)), deadLetterTopic);
        CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
                policy,
                CausalViolationHandler.noop(),
                new KafkaVectorClockSerialiser(),
                record -> deadLetterProducer.send(
                        new ProducerRecord<>(policy.destination(), record.key(), record.value())));

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(input).process(supplier).to(output);

        Map<String, Object> streamsConfig = new HashMap<>();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "dlt-it-" + UUID.randomUUID());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.toString());
        streamsConfig.put("auto.offset.reset", "earliest");

        KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(streamsConfig));
        streams.start();
        try (KafkaConsumer<String, String> verifier = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-verify-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {

            verifier.subscribe(List.of(output, deadLetterTopic));
            List<ConsumerRecord<String, String>> outputRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> deadLetterRecords = new ArrayList<>();

            await().atMost(45, SECONDS).until(() -> {
                verifier.poll(Duration.ofMillis(200)).forEach(record -> {
                    if (record.topic().equals(output)) outputRecords.add(record);
                    else deadLetterRecords.add(record);
                });
                return !outputRecords.isEmpty() && !deadLetterRecords.isEmpty();
            });

            assertEquals(List.of("R_free"),
                    outputRecords.stream().map(ConsumerRecord::key).toList(),
                    "only the satisfiable record may reach the output topic");
            assertEquals(List.of("R_impossible"),
                    deadLetterRecords.stream().map(ConsumerRecord::key).toList(),
                    "the evicted record must land on the dead-letter topic");
        } finally {
            streams.close();
            deadLetterProducer.close();
        }
    }
}
