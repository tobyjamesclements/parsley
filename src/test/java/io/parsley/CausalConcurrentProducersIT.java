package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for causal ordering when multiple independent producers write concurrently
 * to the same output topic (backlog #13).
 *
 * <p>The gap closed: no existing test spins up two independent {@link CausalProducer} instances
 * writing to the same topic with their <em>own</em> frontier clocks (distinct clock dimensions).
 * Previous tests use a single producer or a single clock dimension. This class verifies that the
 * consumer correctly maintains a multi-dimensional frontier and holds/releases records from each
 * producer independently.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalConcurrentProducersIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String SOURCE_A = "source-a";
    private static final String SOURCE_B = "source-b";
    private static final String OUTPUT   = "output";

    /**
     * Verifies that two independent {@link CausalProducer} instances writing concurrently to the
     * same output topic are held and released independently by the consumer, based on each
     * producer's distinct clock dimension.
     *
     * <h2>Topology</h2>
     * <ul>
     *   <li>{@code SOURCE_A} (1 partition) — the upstream topic producer-A declares dependency on.</li>
     *   <li>{@code SOURCE_B} (1 partition) — the upstream topic producer-B declares dependency on.</li>
     *   <li>{@code OUTPUT} (1 partition) — both producers write here with their respective clocks.</li>
     * </ul>
     * The consumer subscribes to all three topics so its frontier can advance on SOURCE_A and
     * SOURCE_B, satisfying the buffered OUTPUT dependencies.
     *
     * <h2>Clock alignment</h2>
     * Both producers and the consumer use {@link MockAdminClient} (no-arg), which substitutes
     * deterministic name-derived UUIDs ({@link CausalPosition#deriveUuid}) for every topic. The
     * test constructs producer clocks with the same {@code deriveUuid} call, so the stamped clock
     * keys match the consumer's frontier keys and records drain naturally rather than waiting for
     * the eviction limit.
     *
     * <h2>Protocol</h2>
     * <ol>
     *   <li><b>Concurrent send</b> — producer-A and producer-B send 3 records each to OUTPUT from
     *       separate threads. Producer-A stamps {@code {SOURCE_A/p0 → i}} and producer-B stamps
     *       {@code {SOURCE_B/p0 → i}}. These are independent clock dimensions; neither producer
     *       knows about the other's upstream topic.</li>
     *   <li><b>Confirm all held</b> — poll several rounds; assert 0 OUTPUT records delivered.
     *       Neither SOURCE_A nor SOURCE_B has any records, so the consumer's frontier is absent on
     *       both dimensions.</li>
     *   <li><b>Release A</b> — send 3 records to SOURCE_A (offsets 0–2). Await until the 3
     *       producer-A OUTPUT records appear. Then assert the 3 producer-B OUTPUT records are
     *       still absent: satisfying SOURCE_A does not satisfy SOURCE_B.</li>
     *   <li><b>Release B</b> — send 3 records to SOURCE_B (offsets 0–2). Await until the 3
     *       producer-B OUTPUT records appear.</li>
     * </ol>
     *
     * <h2>Clock propagation spot-check</h2>
     * One record from each producer is verified with {@link CausalDependencies#fromRecord}: the
     * original producer clock must survive the outbox path ({@code ParsleyConsumer} saves the
     * producer clock as {@code ORIG_CLOCK} before stamping, then restores it in {@code poll()}).
     */
    @Test
    void twoIndependentProducers_concurrentSendsWithDifferentClocks_consumerDeliversOnceDepsSatisfied()
            throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap);

        Uuid topicAId = CausalPosition.deriveUuid(SOURCE_A);
        Uuid topicBId = CausalPosition.deriveUuid(SOURCE_B);

        List<ConsumerRecord<String, String>> received = new ArrayList<>();

        try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
                     List.of(SOURCE_A, SOURCE_B, OUTPUT),
                     CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofMinutes(5))),
                     Map.of(),
                     streamsConfig(bootstrap))
                     .topicAdmin(new MockAdminClient())
                     .build();
             CausalProducer<String, String> producerA = causalProducer(bootstrap);
             CausalProducer<String, String> producerB = causalProducer(bootstrap)) {

            // Phase 1: concurrent send from two independent producers to the same OUTPUT topic.
            // Each producer carries its own clock dimension; neither references the other's upstream.
            CompletableFuture<Void> sendsA = CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 3; i++) {
                        producerA.send(new ProducerRecord<>(OUTPUT, "ka" + i, "a" + i),
                                CausalDependencies.empty().advance(topicAId, 0, i)).get();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            CompletableFuture<Void> sendsB = CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 3; i++) {
                        producerB.send(new ProducerRecord<>(OUTPUT, "kb" + i, "b" + i),
                                CausalDependencies.empty().advance(topicBId, 0, i)).get();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            CompletableFuture.allOf(sendsA, sendsB).get(30, TimeUnit.SECONDS);

            // Phase 2: confirm all 6 OUTPUT records are held (neither source topic has records yet).
            for (int i = 0; i < 5; i++) {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            }
            assertEquals(0, outputRecords(received).size(),
                    "all OUTPUT records must be buffered before any source records arrive");

            // Phase 3: satisfy SOURCE_A only → producer-A's records drain, producer-B's do not.
            try (KafkaProducer<String, String> raw = rawProducer(bootstrap)) {
                for (int i = 0; i < 3; i++) {
                    raw.send(new ProducerRecord<>(SOURCE_A, "sa" + i, "sa-" + i)).get();
                }
            }
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return outputValues(received).containsAll(List.of("a0", "a1", "a2"));
            });
            // SOURCE_B frontier is still absent: producer-B's records must remain buffered.
            assertFalse(outputValues(received).contains("b0"),
                    "b0 must not be released before SOURCE_B records arrive");
            assertFalse(outputValues(received).contains("b1"),
                    "b1 must not be released before SOURCE_B records arrive");
            assertFalse(outputValues(received).contains("b2"),
                    "b2 must not be released before SOURCE_B records arrive");

            // Phase 4: satisfy SOURCE_B → producer-B's records drain.
            try (KafkaProducer<String, String> raw = rawProducer(bootstrap)) {
                for (int i = 0; i < 3; i++) {
                    raw.send(new ProducerRecord<>(SOURCE_B, "sb" + i, "sb-" + i)).get();
                }
            }
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                return outputValues(received).containsAll(List.of("b0", "b1", "b2"));
            });

            assertEquals(6, outputRecords(received).size(), "all 6 OUTPUT records must be delivered");

            // Clock propagation spot-check: the original producer clock must survive the outbox path.
            ConsumerRecord<String, String> a0 = outputRecords(received).stream()
                    .filter(r -> r.value().equals("a0")).findFirst().orElseThrow();
            assertEquals(
                    Optional.of(CausalDependencies.empty().advance(topicAId, 0, 0)),
                    CausalDependencies.fromRecord(a0),
                    "a0 must carry producer-A's original clock");

            ConsumerRecord<String, String> b0 = outputRecords(received).stream()
                    .filter(r -> r.value().equals("b0")).findFirst().orElseThrow();
            assertEquals(
                    Optional.of(CausalDependencies.empty().advance(topicBId, 0, 0)),
                    CausalDependencies.fromRecord(b0),
                    "b0 must carry producer-B's original clock");

            assertTrue(consumer.frontier().positions().stream()
                            .anyMatch(p -> p.topicId().equals(topicAId)),
                    "frontier must have advanced on SOURCE_A");
            assertTrue(consumer.frontier().positions().stream()
                            .anyMatch(p -> p.topicId().equals(topicBId)),
                    "frontier must have advanced on SOURCE_B");
        }
    }

    private List<ConsumerRecord<String, String>> outputRecords(List<ConsumerRecord<String, String>> records) {
        return records.stream().filter(r -> r.topic().equals(OUTPUT)).toList();
    }

    private List<String> outputValues(List<ConsumerRecord<String, String>> records) {
        return outputRecords(records).stream().map(ConsumerRecord::value).toList();
    }

    private static Map<String, Object> streamsConfig(String bootstrap) {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "concurrent-producers-app-" + UUID.randomUUID(),
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    }

    private static CausalProducer<String, String> causalProducer(String bootstrap) {
        return CausalProducers.<String, String>builder(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())).build();
    }

    private static KafkaProducer<String, String> rawProducer(String bootstrap) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
    }

    private static void createTopics(String bootstrap) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(Set.of(
                    new NewTopic(SOURCE_A, 1, (short) 1),
                    new NewTopic(SOURCE_B, 1, (short) 1),
                    new NewTopic(OUTPUT,   1, (short) 1))).all().get();
        }
    }
}
