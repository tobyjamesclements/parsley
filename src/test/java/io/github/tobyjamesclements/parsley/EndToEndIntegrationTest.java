package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 300, unit = TimeUnit.SECONDS)
/**
 * Establishes delivery order against a real broker under exactly-once semantics.
 *
 * <p>Covers causal chains, restart with held messages, a cause naming an aborted position,
 * retention crossing a held message, log truncation, and a crash part-way through a step.
 */
class EndToEndIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @TempDir
    static Path stateDir;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of("log.retention.check.interval.ms", "500"));
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        ClusterTestSupport.stopCluster(cluster, admin);
    }

    private static void createTopics(String... names) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : names) {
            topics.add(new NewTopic(name, 1, (short) 1));
        }
        admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }

    private static ParsleyConfig config(String prefix) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), prefix)
                .stateDir(stateDir.resolve(prefix).toString())
                .build();
    }

    private static void produce(String topic, String key, String value, RecordHeader... headers) {
        ClusterTestSupport.produce(cluster.bootstrapServers(), topic, key, value, headers);
    }

    private static void produceAborted(String topic, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "aborter-" + UUID.randomUUID());
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "ghost", value));
            producer.flush();
            producer.abortTransaction();
        }
    }

    private static List<ConsumerRecord<String, String>> readAllCommitted(String topic) {
        return ClusterTestSupport.readAllCommitted(cluster.bootstrapServers(), topic);
    }

    private static UUID topicId(String topic) throws Exception {
        return ClusterTestSupport.topicId(admin, topic);
    }

    private static void await(String what, BooleanSupplier condition, Duration timeout) {
        ClusterTestSupport.await(what, condition, timeout);
    }

    private static RecordHeader causesHeader(Map<Channel, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    /** Causal chain delivers in order and output decodes with plain codecs. */
    @Test
    void causalChainDeliversInOrderAndOutputDecodesWithPlainCodecs() throws Exception {
        createTopics("e2e-t0", "e2e-t1", "e2e-t2");
        Topic<String, String> t0 = Topic.of("e2e-t0", Serdes.String(), Serdes.String());
        Topic<String, String> t1 = Topic.of("e2e-t1", Serdes.String(), Serdes.String());
        Topic<String, String> t2 = Topic.of("e2e-t2", Serdes.String(), Serdes.String());

        ConcurrentLinkedQueue<String> deliveredAtP3 = new ConcurrentLinkedQueue<>();
        Process p1 = Process.named("p1")
                .receives(t0, (d, s) -> Effects.builder().send(t1, d.key(), "A").build())
                .sends(t1)
                .build();
        Process p2 = Process.named("p2")
                .receives(t1, (d, s) -> Effects.builder().send(t2, d.key(), "B").build())
                .sends(t2)
                .build();
        Process p3 = Process.named("p3")
                .receives(t1, (d, s) -> {
                    deliveredAtP3.add(d.value());
                    return Effects.none();
                })
                .receives(t2, (d, s) -> {
                    deliveredAtP3.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("e2e"), p1, p2, p3)) {
            produce("e2e-t0", "k", "E");
            await("p3 to deliver both A and B", () -> deliveredAtP3.size() == 2, Duration.ofSeconds(120));
            assertEquals(List.of("A", "B"), List.copyOf(deliveredAtP3),
                    "the cause must be delivered before its effect");
            assertTrue(parsley.healthy());
        }

        List<ConsumerRecord<String, String>> t2Records = readAllCommitted("e2e-t2");
        assertEquals(1, t2Records.size());
        ConsumerRecord<String, String> b = t2Records.get(0);
        assertEquals("B", b.value());
        assertEquals("k", b.key());
        Causes causes = CausesCodec.decode(b.headers().lastHeader(CausesCodec.HEADER_KEY).value());
        assertEquals(0L, causes.byChannel().get(new Channel(topicId("e2e-t1"), 0)),
                "the effect's metadata must express its cause on e2e-t1");
    }

    /** Held message survives a real restart. */
    @Test
    void heldMessageSurvivesARealRestart() throws Exception {
        createTopics("hold-a", "hold-b");
        Topic<String, String> a = Topic.of("hold-a", Serdes.String(), Serdes.String());
        Topic<String, String> b = Topic.of("hold-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process ph = Process.named("ph")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("hold-b", "k", "B", causesHeader(Map.of(new Channel(topicId("hold-a"), 0), 0L)));

        try (Parsley parsley = Parsley.start(config("hold"), ph)) {
            Thread.sleep(5_000);
            assertEquals(List.of(), List.copyOf(delivered), "the effect must be held while its cause is missing");
        }

        try (Parsley parsley = Parsley.start(config("hold"), ph)) {
            produce("hold-a", "k", "A");
            await("A then B after restart", () -> delivered.size() == 2, Duration.ofSeconds(120));
            assertEquals(List.of("A", "B"), List.copyOf(delivered));
        }
    }

    /**
     * A task holding an undelivered effect migrates to a second instance without loss or
     * redelivery — the one leg of "restarts, rebalances and partition reassignment" a
     * single-instance restart does not cross.
     *
     * <p>The second instance may start while the first runs because its bootstrap takes the
     * read-only fast path: the first instance's bootstrap already committed initial
     * positions, and only a commit requires joining the group.
     */
    @Test
    void heldMessageSurvivesTaskMigrationBetweenInstances() throws Exception {
        createTopics("mig-a", "mig-b");
        Topic<String, String> a = Topic.of("mig-a", Serdes.String(), Serdes.String());
        Topic<String, String> b = Topic.of("mig-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process pm = Process.named("pm")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("mig-b", "k", "B", causesHeader(Map.of(new Channel(topicId("mig-a"), 0), 0L)));

        Parsley first = Parsley.start(instanceConfig("mig", "mig-1"), pm);
        try {
            awaitFedAndHeld("mig-pm", "mig-b", delivered);

            try (Parsley second = Parsley.start(instanceConfig("mig", "mig-2"), pm)) {
                first.close();
                produce("mig-a", "k", "A");
                await("A then B on the surviving instance", () -> delivered.size() == 2, Duration.ofSeconds(120));
                Thread.sleep(2_000);
                assertEquals(List.of("A", "B"), List.copyOf(delivered),
                        "the migrated hold must deliver in causal order, exactly once");
            }
        } finally {
            first.close();
        }
    }

    private static ParsleyConfig instanceConfig(String prefix, String instanceDir) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), prefix)
                .stateDir(stateDir.resolve(instanceDir).toString())
                .build();
    }

    /** Held message survives losing all local state: ordering state restores from the changelog. */
    @Test
    void heldMessageSurvivesAStateDirWipeByChangelogRestore() throws Exception {
        createTopics("wipe-a", "wipe-b");
        Topic<String, String> a = Topic.of("wipe-a", Serdes.String(), Serdes.String());
        Topic<String, String> b = Topic.of("wipe-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process pw = Process.named("pw")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("wipe-b", "k", "B", causesHeader(Map.of(new Channel(topicId("wipe-a"), 0), 0L)));

        try (Parsley parsley = Parsley.start(config("wipe"), pw)) {
            awaitFedAndHeld("wipe-pw", "wipe-b", delivered);
        }

        deleteRecursively(stateDir.resolve("wipe"));

        try (Parsley parsley = Parsley.start(config("wipe"), pw)) {
            produce("wipe-a", "k", "A");
            await("A then B after the wipe", () -> delivered.size() == 2, Duration.ofSeconds(120));
            assertEquals(List.of("A", "B"), List.copyOf(delivered),
                    "the hold and its order must be rebuilt entirely from the changelog");
        }
    }

    private static void awaitFedAndHeld(String groupId, String topic, ConcurrentLinkedQueue<String> delivered) {
        ClusterTestSupport.awaitFedAndHeld(admin, groupId, topic, delivered);
    }

    private static void deleteRecursively(java.nio.file.Path root) throws Exception {
        if (!java.nio.file.Files.exists(root)) {
            return;
        }
        try (var paths = java.nio.file.Files.walk(root)) {
            for (java.nio.file.Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.delete(path);
            }
        }
    }

    /**
     * A cause naming a position no committed record occupies — here the abort marker of a
     * transaction, a hand-built header exactly like an out-of-contract stamper's (wire-format
     * constraint 8) — is held, neither rescued by a report nor refused: the process stays
     * healthy, and nothing delivers past the hold until a later record on the
     * channel settles the run below it — receipt of gap-a@3 asserts everything below was fed
     * or never will be, and B goes with it. A parsley stamper never produces this
     * header; the facts round that used to rescue it served stampers this library does not
     * control.
     */
    @Test
    void causeNamingAnAbortedPositionIsHeldAndVisibleUntilALaterRecordSettlesIt() throws Exception {
        createTopics("gap-a", "gap-b");
        Topic<String, String> a = Topic.of("gap-a", Serdes.String(), Serdes.String());
        Topic<String, String> b = Topic.of("gap-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process pg = Process.named("pg")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("gap"), pg)) {
            produce("gap-a", "k", "A0");
            await("A0 delivered", () -> delivered.contains("A0"), Duration.ofSeconds(120));

            // Offset 1 is the aborted record, offset 2 its abort marker: no committed record
            // will ever occupy either, so a cause naming 2 is out of contract.
            produceAborted("gap-a", "ghost");
            produce("gap-b", "k", "B", causesHeader(Map.of(new Channel(topicId("gap-a"), 0), 2L)));

            ClusterTestSupport.awaitCommitted(admin, "gap-pg", "gap-b", 1); // B was fed and its step committed
            Thread.sleep(3_000);
            assertEquals(List.of("A0"), List.copyOf(delivered), "no report and no clock rescues the hold");
            assertTrue(parsley.healthy(), "an out-of-contract cause is held, not refused");

            produce("gap-a", "k", "A3");
            await("B to go once the record that settles the run below it is received",
                    () -> delivered.contains("B") && delivered.contains("A3"), Duration.ofSeconds(120));
            // B and A3 are causally independent — B names gap-a@2, not A3 — so once gap-a@3
            // is received both are deliverable and their relative order is the drain's, not
            // causal order. What is pinned: B went only after that receipt (the 3 s pause
            // above showed it held), and nothing was dropped or delivered twice.
            assertEquals(3, delivered.size(), "each message delivers exactly once");
            assertEquals(java.util.Set.of("A0", "A3", "B"), java.util.Set.copyOf(delivered),
                    "gap-a@3 settles positions 1..2 as never yielding, and B is released by that receipt");
            assertEquals("A0", delivered.peek(), "A0 stays first");
            assertTrue(parsley.healthy());
        }
    }

    /**
     * Retention crossing a held message no longer stops the holder:
     * the message it owes lives in the ordering changelog, its senders keep expressing it,
     * and it delivers in causal order — here from a restart's changelog restore, after its
     * copy on the topic was discarded — once its cause arrives. Assumption 10's intent, and
     * strictly better liveness than a refusal.
     */
    @Test
    void heldMessageDiscardedByRetentionStillDeliversInOrderFromTheChangelog() throws Exception {
        createTopics("ret-a", "ret-b");
        Topic<String, String> a = Topic.of("ret-a", Serdes.String(), Serdes.String());
        Topic<String, String> b = Topic.of("ret-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process pr = Process.named("pr")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("ret-b", "k", "B", causesHeader(Map.of(new Channel(topicId("ret-a"), 0), 0L)));

        try (Parsley parsley = Parsley.start(config("ret"), pr)) {
            awaitFedAndHeld("ret-pr", "ret-b", delivered);
        }

        // Retention discards the held message's copy on its topic while the process is stopped.
        TopicPartition retB = new TopicPartition("ret-b", 0);
        admin.deleteRecords(Map.of(retB, RecordsToDelete.beforeOffset(1))).all().get(30, TimeUnit.SECONDS);
        await("the log start of ret-b to pass the held message", () -> {
            try {
                return admin.listOffsets(Map.of(retB, org.apache.kafka.clients.admin.OffsetSpec.earliest()))
                        .all().get(10, TimeUnit.SECONDS).get(retB).offset() == 1L;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(30));

        try (Parsley parsley = Parsley.start(config("ret"), pr)) {
            produce("ret-a", "k", "A");
            await("A then B, with B restored from the changelog", () -> delivered.size() == 2,
                    Duration.ofSeconds(120));
            assertEquals(List.of("A", "B"), List.copyOf(delivered),
                    "the held message delivers in causal order from the changelog, its topic copy gone");
            assertTrue(parsley.healthy(), "retention crossing a held message is not a refusal");
        }
    }

    /** Truncation beyond the read position stops the process. */
    @Test
    void truncationBeyondTheReadPositionStopsTheProcess() throws Exception {
        createTopics("trunc-a");
        Topic<String, String> a = Topic.of("trunc-a", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        Process pt = Process.named("pt")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("trunc"), pt)) {
            for (int i = 0; i < 3; i++) {
                produce("trunc-a", "k", "m" + i);
            }
            await("first three delivered", () -> delivered.size() == 3, Duration.ofSeconds(120));
        }

        produce("trunc-a", "k", "m3");
        produce("trunc-a", "k", "m4");
        admin.deleteRecords(Map.of(new TopicPartition("trunc-a", 0), RecordsToDelete.beforeOffset(5)))
                .all().get(30, TimeUnit.SECONDS);

        Parsley parsley = Parsley.start(config("trunc"), pt);
        try {
            await("the process to stop rather than skip discarded messages",
                    () -> !parsley.healthy(), Duration.ofSeconds(120));
            assertEquals(List.of("m0", "m1", "m2"), List.copyOf(delivered),
                    "nothing may be delivered past the discarded positions");
            await("the consumer's out-of-range stop to reach status() with its reason",
                    () -> parsley.status().get("pt").refusalReason().isPresent(), Duration.ofSeconds(30));
            assertEquals(FailClosedException.Reason
                            .POSITIONS_DISCARDED_UNREAD,
                    parsley.status().get("pt").refusalReason().orElseThrow(),
                    "the fetch is the one judge of retention, and its stop names Safety 8's condition");
        } finally {
            parsley.close();
        }
    }

    /** Crash mid step delivers effects exactly once. */
    @Test
    void crashMidStepDeliversEffectsExactlyOnce() throws Exception {
        createTopics("once-in", "once-out");
        Topic<String, String> in = Topic.of("once-in", Serdes.String(), Serdes.String());
        Topic<String, String> out = Topic.of("once-out", Serdes.String(), Serdes.String());
        AtomicBoolean alreadyFailed = new AtomicBoolean(false);
        Process po = Process.named("po")
                .receives(in, (d, s) -> {
                    if (d.value().equals("boom") && alreadyFailed.compareAndSet(false, true)) {
                        throw new IllegalStateException("injected crash while handling the delivery");
                    }
                    return Effects.builder().send(out, d.key(), d.value() + "-ok").build();
                })
                .sends(out)
                .build();

        Parsley first = Parsley.start(config("once"), po);
        produce("once-in", "k", "boom");
        await("the process to stop after the injected crash", () -> !first.healthy(), Duration.ofSeconds(120));
        first.close();

        try (Parsley second = Parsley.start(config("once"), po)) {
            List<ConsumerRecord<String, String>> outRecords = new ArrayList<>();
            await("exactly one committed effect", () -> {
                outRecords.clear();
                outRecords.addAll(readAllCommitted("once-out"));
                return outRecords.size() == 1;
            }, Duration.ofSeconds(120));
            assertEquals("boom-ok", outRecords.get(0).value());
            assertNotNull(second);
        }
    }
}
