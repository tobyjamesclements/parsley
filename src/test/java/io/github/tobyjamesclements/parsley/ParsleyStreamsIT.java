package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The production runtime end to end on a real broker: {@link Parsley#streams} under
 * exactly-once, records stamped on the wire with parseable protocol headers, and a restart
 * of the same application resuming without loss or duplication. The suite proves the
 * assembled plumbing works against a live cluster; causal-order correctness stays the
 * simulator's obligation.
 */
@Testcontainers
class ParsleyStreamsIT {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(System.getProperty("parsley.broker.image", "apache/kafka:3.7.2")));

    private static final Duration DEADLINE = Duration.ofSeconds(90);

    /**
     * Rebalances are slower than record flow, and a migration that has not happened yet must
     * not be mistaken for one that never will.
     */
    private static final Duration REBALANCE_DEADLINE = Duration.ofSeconds(120);

    private static Admin admin;

    @TempDir
    Path stateDir;

    /**
     * The second instance of a two-instance application needs its own state directory: two
     * instances sharing one would contend on a single RocksDB lock.
     */
    @TempDir
    Path secondStateDir;

    @BeforeAll
    static void connect() {
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    @AfterAll
    static void disconnect() {
        admin.close();
    }

    /**
     * A single stage delivers through the broker: records produced to the source come out of
     * the sink transformed, in per-partition order, each stamped with a clock, a sender tag,
     * and strictly increasing sender sequences.
     */
    @Test
    void singleStageDeliversStampedRecords() throws Exception {
        Topic<String, String> t1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
        Topic<String, String> mid = Topic.of("mid", Codec.utf8(), Codec.utf8());
        createTopic("t1");
        createTopic("mid");

        Stage edge = Stage.named("edge")
                .on(t1, m -> List.of(mid.send(m.key(), m.value().toUpperCase(Locale.ROOT))))
                .into(mid)
                .build();

        try (CausalStreams streams = Parsley.named("parsley-it-single", edge).streams(props("parsley-it-single"))) {
            streams.start();
            produce("t1", "k1", "v1", "k2", "v2", "k3", "v3");

            List<ConsumerRecord<String, String>> out = consume("mid", 3);
            assertEquals(List.of("V1", "V2", "V3"),
                    out.stream().map(ConsumerRecord::value).toList(),
                    "the sink must carry the transformed records in per-partition order");

            UUID sender = null;
            long previousSeq = -1;
            for (ConsumerRecord<String, String> r : out) {
                assertNotNull(CausalHeaders.read(r.headers()),
                        "every stamped record must carry a parseable clock header");
                UUID s = CausalHeaders.readSender(r.headers());
                assertNotNull(s, "every stamped record must carry a sender tag");
                if (sender == null) sender = s;
                assertEquals(sender, s, "one task must stamp one sender identity");
                long seq = CausalHeaders.readSeq(r.headers());
                assertTrue(seq > previousSeq,
                        "sender sequences on one channel must strictly increase");
                previousSeq = seq;
            }
        }
    }

    /**
     * A restart of the same application on the same state directory resumes cleanly: the
     * record processed before the stop is not emitted again, and a record produced after the
     * restart flows through. The deep crash-recovery obligations stay with the simulator;
     * this is the live-cluster init path (restore, end-offset seed) actually running.
     */
    @Test
    void restartResumesWithoutLossOrDuplication() throws Exception {
        Topic<String, String> orders = Topic.of("orders", Codec.utf8(), Codec.utf8());
        Topic<String, String> shipped = Topic.of("shipped", Codec.utf8(), Codec.utf8());
        createTopic("orders");
        createTopic("shipped");

        Stage ship = Stage.named("ship")
                .on(orders, m -> List.of(shipped.send(m.key(), m.value())))
                .into(shipped)
                .build();

        try (CausalStreams first = Parsley.named("parsley-it-restart", ship).streams(props("parsley-it-restart"))) {
            first.start();
            produce("orders", "o1", "packed");
            consume("shipped", 1);
            assertTrue(first.close(Duration.ofSeconds(60)),
                    "the first instance must stop cleanly before the restart");
        }

        try (CausalStreams second = Parsley.named("parsley-it-restart", ship).streams(props("parsley-it-restart"))) {
            second.start();
            produce("orders", "o2", "packed");

            List<ConsumerRecord<String, String>> out = consume("shipped", 2);
            assertEquals(List.of("o1", "o2"),
                    out.stream().map(ConsumerRecord::key).toList(),
                    "the restarted instance must emit the new record exactly once and never"
                            + " re-emit the record processed before the stop");
        }
    }

    /**
     * A task migrating between two live instances of one application keeps the guarantee. Each
     * source partition's records come out exactly once in produced order across two
     * rebalances — a task moving from the first instance to the second, and moving back when
     * the first stops — and carry the same sender identity on the new host as on the old.
     *
     * <p>Sender identity is derived from the application id, the stage name, and the partition
     * rather than from the incarnation, precisely so it survives a move; a migration that reset
     * it, or that lost the causal state the changelog carries, would break claim resolution at
     * every downstream consumer. The rebalances are asserted rather than assumed: each phase
     * waits for the assignment to actually move, so a test that silently never migrated fails
     * at the wait instead of passing on a single-instance run.
     */
    @Test
    void taskMigrationBetweenInstancesPreservesOrderAndSenderIdentity() throws Exception {
        Topic<String, String> moves = Topic.of("moves", Codec.utf8(), Codec.utf8());
        Topic<String, String> moved = Topic.of("moved", Codec.utf8(), Codec.utf8());
        createTopic("moves", 2);
        createTopic("moved", 2);

        Stage relay = Stage.named("relay")
                .on(moves, m -> List.of(moved.send(m.key(), m.value())))
                .into(moved)
                .build();

        // One key per source partition, so each key's records are stamped by one task and land
        // on one sink partition — which makes their offsets a total order over that task's
        // output, and any loss, duplication, or reordering visible.
        String applicationId = "parsley-it-rebalance";
        try (CausalStreams first = Parsley.named(applicationId, relay).streams(props(applicationId, stateDir))) {
            first.start();
            awaitActiveTasks(first, 2, "the first instance alone must own both tasks");
            produceTo("moves", 0, "k0", "a");
            produceTo("moves", 1, "k1", "a");
            consume("moved", 2);

            try (CausalStreams second =
                         Parsley.named(applicationId, relay).streams(props(applicationId, secondStateDir))) {
                second.start();
                // The state here is a handful of records, so the joining instance is inside
                // acceptable.recovery.lag immediately and the assignor moves an active task on
                // the first rebalance rather than warming a standby up first.
                awaitActiveTasks(first, 1,
                        "the first instance must give up a task when the second joins");
                awaitActiveTasks(second, 1, "the second instance must take a task over");

                produceTo("moves", 0, "k0", "b");
                produceTo("moves", 1, "k1", "b");
                consume("moved", 4);

                assertTrue(first.close(Duration.ofSeconds(60)),
                        "the first instance must stop cleanly to hand its task back");
                awaitActiveTasks(second, 2,
                        "the surviving instance must take both tasks over");

                produceTo("moves", 0, "k0", "c");
                produceTo("moves", 1, "k1", "c");
                List<ConsumerRecord<String, String>> out = consume("moved", 6);

                UUID previousSender = null;
                for (String key : List.of("k0", "k1")) {
                    List<ConsumerRecord<String, String>> byKey = out.stream()
                            .filter(r -> key.equals(r.key()))
                            .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                            .toList();
                    assertEquals(List.of("a", "b", "c"),
                            byKey.stream().map(ConsumerRecord::value).toList(),
                            "the records of " + key + " must arrive exactly once, in produced"
                                    + " order, across both migrations");
                    for (ConsumerRecord<String, String> r : byKey) {
                        assertNotNull(CausalHeaders.read(r.headers()),
                                "a record stamped after a migration must still carry a"
                                        + " parseable clock header");
                    }
                    List<UUID> senders = byKey.stream()
                            .map(r -> CausalHeaders.readSender(r.headers()))
                            .distinct().toList();
                    assertEquals(1, senders.size(),
                            "a task's sender identity must survive migration to another"
                                    + " instance, but " + key + " was stamped by " + senders);
                    assertNotEquals(previousSender, senders.get(0),
                            "the two source partitions must be stamped by different tasks, or"
                                    + " the run never exercised two of them");
                    previousSender = senders.get(0);
                }
            }
        }
    }

    private Properties props(String applicationId) {
        return props(applicationId, stateDir);
    }

    private Properties props(String applicationId, Path dir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, dir.toString());
        // One task per thread, so a two-instance application splits its two tasks one apiece
        // instead of parking both on whichever instance started first.
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return props;
    }

    /**
     * Waits until the instance reports exactly {@code expected} active tasks, so a rebalance is
     * observed rather than assumed. A migration that never happens fails here, naming the
     * assignment it waited for, rather than later as an unexplained shortfall at the sink.
     */
    private static void awaitActiveTasks(CausalStreams streams, int expected, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + REBALANCE_DEADLINE.toNanos();
        int seen = -1;
        while (System.nanoTime() < deadline) {
            seen = streams.metadataForLocalThreads().stream()
                    .mapToInt(thread -> thread.activeTasks().size()).sum();
            if (seen == expected) return;
            Thread.sleep(200);
        }
        fail(what + ": expected " + expected + " active tasks within "
                + REBALANCE_DEADLINE.toSeconds() + "s but saw " + seen);
    }

    private static void createTopic(String name) throws Exception {
        createTopic(name, 1);
    }

    private static void createTopic(String name, int partitions) throws Exception {
        admin.createTopics(Set.of(new NewTopic(name, partitions, (short) 1))).all().get();
    }

    private static void produceTo(String topic, int partition, String key, String value)
            throws Exception {
        try (var producer = new KafkaProducer<>(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        }
    }

    private static void produce(String topic, String... keyValuePairs) throws Exception {
        try (var producer = new KafkaProducer<>(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                producer.send(new ProducerRecord<>(topic, keyValuePairs[i], keyValuePairs[i + 1]))
                        .get();
            }
        }
    }

    /**
     * Reads every partition of the topic from the beginning, read-committed, until
     * {@code count} records arrive.
     */
    private static List<ConsumerRecord<String, String>> consume(String topic, int count) {
        try (var consumer = new KafkaConsumer<>(
                Map.<String, Object>of(
                        "bootstrap.servers", KAFKA.getBootstrapServers(),
                        "isolation.level", "read_committed",
                        "auto.offset.reset", "earliest",
                        "group.id", "parsley-it-reader-" + topic),
                new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> tps = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            List<ConsumerRecord<String, String>> out = new ArrayList<>();
            long deadline = System.nanoTime() + DEADLINE.toNanos();
            while (out.size() < count && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(out::add);
            }
            assertEquals(count, out.size(),
                    "expected " + count + " committed records on " + topic + " within "
                            + DEADLINE.toSeconds() + "s but saw " + out.size());
            return out;
        }
    }
}
