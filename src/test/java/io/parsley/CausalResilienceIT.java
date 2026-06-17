package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
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
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario tests for restart-mid-hold recovery and multi-threaded delivery (backlog #9).
 *
 * <p>Multi-instance rebalance (add/remove a KafkaStreams instance) is deferred to the chaos suite
 * (backlog #11); this class is the seed of that work.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalResilienceIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PRICES = "prices";
    private static final String ORDERS = "orders";
    private static final String EVENTS = "events";

    /**
     * Verifies that a record buffered in consumer-1 is recovered by consumer-2 (same
     * {@code applicationId}) and drained correctly once the dependency arrives.
     *
     * <p>This exercises the full state-store changelog restoration path and the
     * {@link ParsleyEngine} WaitIndex re-seeding from the restored buffer.
     *
     * <p>The dependency is expressed with the real Kafka topic UUID (fetched via AdminClient)
     * rather than the {@code advance(TopicPartition, offset)} convenience overload which uses a
     * deterministic name-based UUID. The consumer's frontier uses the real UUID, so only the
     * real UUID triggers a proper drain; the name-based UUID would cause the record to remain
     * buffered until eviction.
     */
    @Test
    void restartWithBufferedRecord_resumesDrainAfterRecovery() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        Uuid pricesUuid = createTopic(bootstrap, PRICES, 1);
        createTopic(bootstrap, ORDERS, 1);

        // Fixed applicationId so Kafka Streams restores the frontier, buffer, and wait-index
        // state stores from their changelog topics when consumer-2 starts.
        String appId = "resilience-restart-app";

        // Phase 1: buffer an ORDERS record that depends on PRICES@0, then close.
        List<ConsumerRecord<String, String>> phase1 = new ArrayList<>();
        try (CausalConsumer<String, String> consumer1 =
                     CausalConsumers.<String, String>builder(
                             List.of(PRICES, ORDERS),
                             CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofMinutes(5))),
                             Map.of(),
                             streamsConfig(bootstrap, appId)).build();
             CausalProducer<String, String> producer = causalProducer(bootstrap)) {

            // Use the real Kafka UUID so the frontier (which also uses the real UUID) can satisfy
            // this dependency when PRICES arrives.
            producer.send(new ProducerRecord<>(ORDERS, "k", "order-1"),
                    CausalDependencies.empty().advance(pricesUuid, 0, 0)).get();

            // Poll a few times — ORDERS should be buffered, not delivered.
            for (int i = 0; i < 3; i++) {
                consumer1.poll(Duration.ofMillis(500)).forEach(phase1::add);
            }
            assertTrue(phase1.isEmpty(), "ORDERS must be buffered, not delivered before PRICES");

            // Allow the 200ms commit interval to fire at least once so the state stores
            // are checkpointed to their changelog topics before the clean shutdown below.
            Thread.sleep(400);
        }

        // Phase 2: new consumer, same applicationId — Kafka Streams restores all three state
        // stores (frontier, buffer, wait-index) from changelog. The ParsleyEngine constructor
        // re-seeds the WaitIndex from the restored buffer in an O(n) pass.
        List<ConsumerRecord<String, String>> phase2 = new ArrayList<>();
        try (CausalConsumer<String, String> consumer2 =
                     CausalConsumers.<String, String>builder(
                             List.of(PRICES, ORDERS),
                             CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofMinutes(5))),
                             Map.of(),
                             streamsConfig(bootstrap, appId)).build();
             CausalProducer<String, String> producer = causalProducer(bootstrap)) {

            // PRICES has no dependencies — admitted immediately, advancing the frontier to
            // pricesUuid/0@offset=0, which satisfies the buffered ORDERS record.
            producer.send(new ProducerRecord<>(PRICES, "k", "price-1"),
                    CausalDependencies.empty()).get();

            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer2.poll(Duration.ofMillis(500)).forEach(phase2::add);
                return phase2.size() >= 2;
            });
        }

        assertEquals(2, phase2.size(), "PRICES and ORDERS must both be delivered after restart");
        assertEquals(PRICES, phase2.get(0).topic(), "PRICES must arrive first (causal dependency)");
        assertEquals(ORDERS, phase2.get(1).topic(), "ORDERS must be drained after PRICES");
    }

    /**
     * Verifies that two stream threads processing two partitions in parallel deliver all records
     * without deadlock and maintain within-partition ordering.
     *
     * <p>Cross-partition causal dependencies between separate tasks are out of scope for the
     * Streams decorator by design (each task owns its own engine and frontier). This test
     * confirms no concurrency issues arise with {@code num.stream.threads=2}.
     */
    @Test
    void multipleStreamThreads_deliverAllRecordsWithoutDeadlock() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopic(bootstrap, EVENTS, 2);

        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (CausalConsumer<String, String> consumer =
                     CausalConsumers.<String, String>builder(
                             List.of(EVENTS),
                             CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)),
                             Map.of(),
                             streamsConfigWithThreads(bootstrap, 2)).build();
             CausalProducer<String, String> producer = causalProducer(bootstrap)) {

            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(EVENTS, 0, "k", "p0-" + i),
                        CausalDependencies.empty()).get();
                producer.send(new ProducerRecord<>(EVENTS, 1, "k", "p1-" + i),
                        CausalDependencies.empty()).get();
            }

            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return received.size() >= 10;
            });
        }

        assertEquals(10, received.size(), "all 10 records must be delivered");

        List<String> p0 = received.stream()
                .filter(r -> r.partition() == 0).map(ConsumerRecord::value).toList();
        List<String> p1 = received.stream()
                .filter(r -> r.partition() == 1).map(ConsumerRecord::value).toList();
        assertEquals(List.of("p0-0", "p0-1", "p0-2", "p0-3", "p0-4"), p0,
                "partition-0 records must arrive in offset order");
        assertEquals(List.of("p1-0", "p1-1", "p1-2", "p1-3", "p1-4"), p1,
                "partition-1 records must arrive in offset order");
    }

    // ---- helpers ----

    private static Map<String, Object> streamsConfig(String bootstrap, String appId) {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, appId,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    }

    private static Map<String, Object> streamsConfigWithThreads(String bootstrap, int threads) {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "resilience-multithread-app",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200,
                StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
    }

    private static CausalProducer<String, String> causalProducer(String bootstrap) {
        return CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
    }

    /** Creates a topic and returns its Kafka-assigned UUID. */
    private static Uuid createTopic(String bootstrap, String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(Set.of(new NewTopic(topic, partitions, (short) 1))).all().get();
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
        }
    }
}
