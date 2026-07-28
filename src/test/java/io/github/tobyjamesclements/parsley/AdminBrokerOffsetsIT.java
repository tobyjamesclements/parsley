package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The broker side of the {@link BrokerOffsets} seam contract, against a real broker at the
 * minimum supported version: end offsets reflect what the broker has assigned, log starts
 * reflect what retention has deleted, and a deleted topic's channels are reported as
 * definitively absent by topic ID even when a topic of the same name exists again. The
 * simulator asserts what the protocol does with these answers; this suite asserts that a
 * real cluster gives them.
 */
@Testcontainers
class AdminBrokerOffsetsIT {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(System.getProperty("parsley.broker.image", "apache/kafka:3.7.2")));

    private static Admin admin;

    @BeforeAll
    static void connect() {
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    @AfterAll
    static void disconnect() {
        admin.close();
    }

    /** End offsets answer the next offset to be assigned, per partition, empty ones included. */
    @Test
    void endOffsetsReflectProducedRecords() throws Exception {
        createTopic("t1", 2);
        produce("t1", 0, 3);

        TopicIds topicIds = TopicIds.fromAdmin(admin);
        UUID id = topicIds.resolve("t1").id();
        BrokerOffsets seam = new AdminBrokerOffsets(topicIds, admin, Set.of("t1"));

        Map<Channel, Long> ends = seam.endOffsets(Set.of(id));
        assertEquals(3L, ends.get(new Channel(id, 0)),
                "the produced partition's end offset must be the next offset to be assigned");
        assertEquals(0L, ends.get(new Channel(id, 1)),
                "an untouched partition must answer zero, not be omitted");
        assertEquals(2, ends.size(),
                "every partition of the sink topic must be answered, no more");
    }

    /** Log starts answer the broker's low-water mark, which record deletion advances. */
    @Test
    void earliestOffsetsReflectLogStart() throws Exception {
        createTopic("t2", 1);
        produce("t2", 0, 3);
        admin.deleteRecords(Map.of(new TopicPartition("t2", 0), RecordsToDelete.beforeOffset(2)))
                .all().get();

        UUID id = TopicIds.fromAdmin(admin).resolve("t2").id();
        BrokerOffsets seam = new AdminBrokerOffsets(TopicIds.fromAdmin(admin), admin, Set.of());
        Channel channel = new Channel(id, 0);

        BrokerOffsets.EarliestOffsets earliest = seam.earliestOffsets(Set.of(channel));
        assertEquals(2L, earliest.logStarts().get(channel),
                "the log start must reflect the records the broker has deleted");
        assertTrue(earliest.confirmedAbsent().isEmpty(),
                "an existing topic must never be reported absent");
    }

    /**
     * A deleted topic's channels are definitively absent by ID, and stay absent when a topic
     * of the same name is created again: identity is the topic ID, not the name, so a
     * recreated topic is a different channel and the old channel's claims are unclaimable
     * forever.
     */
    @Test
    void deletedTopicIsConfirmedAbsentEvenAfterRecreation() throws Exception {
        createTopic("t3", 1);
        UUID staleId = TopicIds.fromAdmin(admin).resolve("t3").id();
        admin.deleteTopics(Set.of("t3")).all().get();

        BrokerOffsets seam = new AdminBrokerOffsets(TopicIds.fromAdmin(admin), admin, Set.of());
        Channel stale = new Channel(staleId, 0);

        // Deletion completes asynchronously on the broker; the seam may transiently throw
        // (fail closed) until the ID is definitively unknown.
        awaitTrue("the deleted topic's channel must become definitively absent",
                () -> seam.earliestOffsets(Set.of(stale)).confirmedAbsent().contains(stale));

        createTopic("t3", 1);
        BrokerOffsets.EarliestOffsets earliest = seam.earliestOffsets(Set.of(stale));
        assertTrue(earliest.confirmedAbsent().contains(stale),
                "a recreated topic must not resurrect the old ID's channels");
        assertTrue(earliest.logStarts().isEmpty(),
                "an absent channel must not also carry a log start");
    }

    private static void createTopic(String name, int partitions) throws Exception {
        admin.createTopics(Set.of(new NewTopic(name, partitions, (short) 1))).all().get();
    }

    private static void produce(String topic, int partition, int records) throws Exception {
        try (var producer = new KafkaProducer<>(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            for (int i = 1; i <= records; i++) {
                producer.send(new ProducerRecord<>(topic, partition, "k" + i, "v" + i)).get();
            }
        }
    }

    /** Polls the condition until it holds, tolerating the seam's fail-closed throws. */
    private static void awaitTrue(String message, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) return;
            } catch (RuntimeException transientFailure) {
                // The seam throws rather than guessing while the broker settles; retry.
            }
            Thread.sleep(200);
        }
        throw new AssertionError(message + " (not within 60s)");
    }
}
