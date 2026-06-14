package io.parsley.it;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.VectorClock;
import io.parsley.consumer.CausalConsumer;
import io.parsley.producer.CausalProducer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round trip against a real broker: a {@link CausalProducer} stamps vector clocks,
 * and a {@link CausalConsumer} delivers the records in causal order with an advancing frontier.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalRoundTripIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String TOPIC = "events";

    @Test
    void producedRecordsAreDeliveredInCausalOrderWithAdvancingFrontier() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopic(bootstrap, TOPIC);

        TopicPartition tp = new TopicPartition(TOPIC, 0);

        try (CausalProducer<String, String> producer = CausalProducer.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
             CausalConsumer<String, String> consumer = CausalConsumer.create(
                     List.of(TOPIC),
                     BufferingPolicy.forwardUnsafe(BufferLimit.ofDuration(Duration.ofSeconds(5))),
                     Map.of(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + UUID.randomUUID()),
                     streamsConfig(bootstrap))) {

            // Each record carries a clock advancing the partition it is written to.
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "k", "v" + i),
                        VectorClock.empty().advance(tp, i));
            }

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(received::add);
                return received.size() >= 5;
            });

            assertEquals(List.of("v0", "v1", "v2", "v3", "v4"),
                    received.stream().map(ConsumerRecord::value).toList());
            assertFalse(consumer.frontier().positions().isEmpty(), "frontier must have advanced");
            assertTrue(consumer.frontier().positions().containsKey(tp));

            // Each delivered record still carries the producer's vector-clock header, extractable
            // via the public API — this is the causal context a service would forward to a client.
            for (int i = 0; i < 5; i++) {
                assertEquals(Optional.of(VectorClock.empty().advance(tp, i)),
                        VectorClock.fromRecord(received.get(i)),
                        "record " + i + " should carry its producer's clock");
            }
        }
    }

    private static Map<String, Object> streamsConfig(String bootstrap) {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "rt-app-" + UUID.randomUUID(),
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }
}
