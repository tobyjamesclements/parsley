package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.Parsley;
import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 300, unit = TimeUnit.SECONDS)
/**
 * Establishes delivery order against a real broker under exactly-once semantics.
 *
 * <p>Covers causal chains, restart with held messages, causes on aborted positions, log
 * truncation, and a crash part-way through a step.
 */
class EndToEndIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @TempDir
    static Path stateDir;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setCombined(true)
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .setConfigProp("group.initial.rebalance.delay.ms", "0")
                .setConfigProp("log.retention.check.interval.ms", "500")
                .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (cluster != null) {
            cluster.close();
        }
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
                .factsInterval(Duration.ofMillis(500))
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
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        List<ConsumerRecord<String, String>> records = new ArrayList<>();

        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            int quietPolls = 0;
            while (System.nanoTime() < deadline && quietPolls < 4) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(250));
                quietPolls = polled.isEmpty() ? quietPolls + 1 : 0;
                polled.forEach(records::add);
            }
        }
        return records;
    }

    private static UUID topicId(String topic) throws Exception {
        return ClusterTestSupport.topicId(admin, topic);
    }

    private static void await(String what, BooleanSupplier condition, Duration timeout) {
        ClusterTestSupport.await(what, condition, timeout);
    }

    private static RecordHeader causesHeader(Map<ChannelId, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    /** Causal chain delivers in order and output decodes with plain codecs. */
    @Test
    void causalChainDeliversInOrderAndOutputDecodesWithPlainCodecs() throws Exception {
        createTopics("e2e-t0", "e2e-t1", "e2e-t2");
        Channel<String, String> t0 = Channel.of("e2e-t0", Serdes.String(), Serdes.String());
        Channel<String, String> t1 = Channel.of("e2e-t1", Serdes.String(), Serdes.String());
        Channel<String, String> t2 = Channel.of("e2e-t2", Serdes.String(), Serdes.String());

        ConcurrentLinkedQueue<String> deliveredAtP3 = new ConcurrentLinkedQueue<>();
        ProcessDefinition p1 = ProcessDefinition.named("p1")
                .receives(t0, (d, s) -> Effects.builder().send(t1, d.key(), "A").build())
                .sends(t1)
                .build();
        ProcessDefinition p2 = ProcessDefinition.named("p2")
                .receives(t1, (d, s) -> Effects.builder().send(t2, d.key(), "B").build())
                .sends(t2)
                .build();
        ProcessDefinition p3 = ProcessDefinition.named("p3")
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
        assertEquals(0L, causes.byChannel().get(new ChannelId(topicId("e2e-t1"), 0)),
                "the effect's metadata must express its cause on e2e-t1");
    }

    /** Held message survives a real restart. */
    @Test
    void heldMessageSurvivesARealRestart() throws Exception {
        createTopics("hold-a", "hold-b");
        Channel<String, String> a = Channel.of("hold-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("hold-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition ph = ProcessDefinition.named("ph")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("hold-b", "k", "B", causesHeader(Map.of(new ChannelId(topicId("hold-a"), 0), 0L)));

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
     * positions, and only a commit requires joining the group (D48).
     */
    @Test
    void heldMessageSurvivesTaskMigrationBetweenInstances() throws Exception {
        createTopics("mig-a", "mig-b");
        Channel<String, String> a = Channel.of("mig-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("mig-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pm = ProcessDefinition.named("pm")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("mig-b", "k", "B", causesHeader(Map.of(new ChannelId(topicId("mig-a"), 0), 0L)));

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
                .factsInterval(Duration.ofMillis(500))
                .build();
    }

    /** Held message survives losing all local state: ordering state restores from the changelog. */
    @Test
    void heldMessageSurvivesAStateDirWipeByChangelogRestore() throws Exception {
        createTopics("wipe-a", "wipe-b");
        Channel<String, String> a = Channel.of("wipe-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("wipe-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pw = ProcessDefinition.named("pw")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("wipe-b", "k", "B", causesHeader(Map.of(new ChannelId(topicId("wipe-a"), 0), 0L)));

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

    /** Cause on aborted positions resolves from read position reports. */
    @Test
    void causeOnAbortedPositionsResolvesFromReadPositionReports() throws Exception {
        createTopics("gap-a", "gap-b");
        Channel<String, String> a = Channel.of("gap-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("gap-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pg = ProcessDefinition.named("pg")
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

            produceAborted("gap-a", "ghost");
            produce("gap-b", "k", "B", causesHeader(Map.of(new ChannelId(topicId("gap-a"), 0), 2L)));

            await("B freed by the read-position report over the aborted trailing run",
                    () -> delivered.contains("B"), Duration.ofSeconds(120));
            assertTrue(parsley.healthy());
        }
    }

    /** Cause on trailing aborted run resolves even after a restart. */
    @Test
    void causeOnTrailingAbortedRunResolvesEvenAfterARestart() throws Exception {
        createTopics("gapr-a", "gapr-b");
        Channel<String, String> a = Channel.of("gapr-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("gapr-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pg = ProcessDefinition.named("pgr")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("gapr"), pg)) {
            produce("gapr-a", "k", "A0");
            await("A0 delivered", () -> delivered.contains("A0"), Duration.ofSeconds(120));
        }

        produceAborted("gapr-a", "ghost");
        produce("gapr-b", "k", "B", causesHeader(Map.of(new ChannelId(topicId("gapr-a"), 0), 2L)));

        try (Parsley parsley = Parsley.start(config("gapr"), pg)) {
            await("B freed after restart despite no record ever arriving on gapr-a again",
                    () -> delivered.contains("B"), Duration.ofSeconds(120));
            assertTrue(parsley.healthy());
        }
    }

    /** Truncation beyond the read position stops the process. */
    @Test
    void truncationBeyondTheReadPositionStopsTheProcess() throws Exception {
        createTopics("trunc-a");
        Channel<String, String> a = Channel.of("trunc-a", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pt = ProcessDefinition.named("pt")
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
        } finally {
            parsley.close();
        }
    }

    /** Crash mid step delivers effects exactly once. */
    @Test
    void crashMidStepDeliversEffectsExactlyOnce() throws Exception {
        createTopics("once-in", "once-out");
        Channel<String, String> in = Channel.of("once-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("once-out", Serdes.String(), Serdes.String());
        AtomicBoolean alreadyFailed = new AtomicBoolean(false);
        ProcessDefinition po = ProcessDefinition.named("po")
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
