package io.parsley.it.broker;

import io.parsley.FenceToken;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.CausalProducer;
import io.parsley.kafka.internal.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class CausalProducerIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();

    private String newTopic() throws Exception {
        String topic = "prod-it-" + UUID.randomUUID().toString().substring(0, 8);
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

    private List<ConsumerRecord<String, String>> consumeRaw(String topic, int count) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-" + UUID.randomUUID());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cfg)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (result.size() < count && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(result::add);
            }
        }
        return result;
    }

    @Test
    void producerInjectsVectorClockHeader() throws Exception {
        String topic = newTopic();
        TopicPartition p = new TopicPartition(topic, 0);
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(p, 5L));
        FenceToken token = FenceToken.of(clock);

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "k1", "v1"), token).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        ConsumerRecord<String, String> raw = consumeRaw(topic, 1).get(0);
        Header header = raw.headers().lastHeader("parsley-vector-clock");
        assertNotNull(header, "parsley-vector-clock header must be present");
        assertEquals(clock, SERIALISER.deserialise(header.value()));
    }

    @Test
    void producerPreservesKeyAndValue() throws Exception {
        String topic = newTopic();
        FenceToken token = FenceToken.of(KafkaVectorClock.empty());

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "myKey", "myValue"), token).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        ConsumerRecord<String, String> raw = consumeRaw(topic, 1).get(0);
        assertEquals("myKey", raw.key());
        assertEquals("myValue", raw.value());
    }

    @Test
    void producerCallbackFires() throws Exception {
        String topic = newTopic();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<RecordMetadata> metaRef = new AtomicReference<>();

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "cb-key", "cb-value"),
                    FenceToken.of(KafkaVectorClock.empty()),
                    (meta, ex) -> {
                        if (ex == null) {
                            metaRef.set(meta);
                            latch.countDown();
                        }
                    });
        } finally {
            // close() flushes pending sends; callback will have fired before this returns
            producer.close();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback must fire");
        assertTrue(metaRef.get().offset() >= 0);
    }

    @Test
    void emptyFenceTokenProducesEmptyClockHeader() throws Exception {
        String topic = newTopic();
        FenceToken token = FenceToken.of(KafkaVectorClock.empty());

        CausalProducer<String, String> producer = CausalProducer.create(producerConfig());
        try {
            producer.send(new ProducerRecord<>(topic, "empty-key", "empty-val"), token).get(10, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        ConsumerRecord<String, String> raw = consumeRaw(topic, 1).get(0);
        Header header = raw.headers().lastHeader("parsley-vector-clock");
        assertNotNull(header);
        assertTrue(((KafkaVectorClock) SERIALISER.deserialise(header.value())).positions().isEmpty());
    }
}
