package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.Parsley;
import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.ProcessStatus;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The host-torture contract (D115): the failure matrix every host must survive with the
 * guarantee intact, on a real broker. Each case is host-neutral — it drives the public API
 * and the broker, and reads the outcome from committed output and {@code status()} — so
 * {@link #host()} selects the host and a subclass runs the same cases against another.
 *
 * <p>What each case tortures: a step aborted part-way through a burst; tasks migrating
 * under load; a member stalled past its poll interval and its transaction timeout while a
 * second instance takes over; a restore that must read past a foreign open transaction on
 * the state topic; a deep hold-back buffer migrating between instances; retention crossing
 * a held message; a broker bounce; and two instances cold-starting together.
 *
 * <p>Outside the matrix, and recorded honestly: a crash between the offset commit and the
 * transaction commit cannot be injected from outside either host, and a broker-side
 * transaction-coordinator failover is not exercised.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
class HostTortureIntegrationTest {
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

    /** The host under torture; a subclass overrides it. */
    ParsleyConfig.Host host() {
        return ParsleyConfig.Host.KAFKA_STREAMS;
    }

    /** The compacted topic holding a process's ordering state, as this host names it. */
    String orderingStateTopic(String applicationId) {
        return switch (host()) {
            case KAFKA_STREAMS -> ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE);
            case KAFKA_CLIENTS -> applicationId + ClientRuntime.ORDERING_TOPIC_SUFFIX;
        };
    }

    private ParsleyConfig.Builder configBuilder(String prefix, String instance) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), prefix)
                .stateDir(stateDir.resolve(prefix + "-" + instance).toString())
                .factsInterval(Duration.ofMillis(500))
                .host(host())
                // A short session so a closed instance's partitions move within seconds
                // on either host; the Streams host does not leave its group on close.
                .streamsProperty("session.timeout.ms", 10_000)
                .streamsProperty("heartbeat.interval.ms", 3_000);
    }

    private ParsleyConfig config(String prefix) {
        return configBuilder(prefix, "1").build();
    }

    private static void createTopics(int partitions, String... names) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : names) {
            topics.add(new NewTopic(name, partitions, (short) 1));
        }
        admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }

    private static void produce(String topic, String key, String value, RecordHeader... headers) {
        ClusterTestSupport.produce(cluster.bootstrapServers(), topic, key, value, headers);
    }

    private static List<ConsumerRecord<String, String>> readAllCommitted(String topic) {
        return ClusterTestSupport.readAllCommitted(cluster.bootstrapServers(), topic);
    }

    private static RecordHeader causesHeader(Map<ChannelId, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    private static ChannelId channel(String topic, int partition) throws Exception {
        return new ChannelId(ClusterTestSupport.topicId(admin, topic), partition);
    }

    private static void await(String what, java.util.function.BooleanSupplier condition, Duration timeout) {
        ClusterTestSupport.await(what, condition, timeout);
    }

    /** Polls until {@code status()} reports at least {@code atLeast} held messages on {@code topic}. */
    private static void awaitHeld(Parsley parsley, String process, String topic, int atLeast) {
        await(process + " to hold " + atLeast + " message(s) on " + topic, () -> {
            ProcessStatus status = parsley.status().get(process);
            return status != null && status.tasks().stream()
                    .flatMap(task -> task.heldChannels().stream())
                    .filter(held -> held.topic().equals(topic))
                    .mapToInt(held -> held.held()).sum() >= atLeast;
        }, Duration.ofSeconds(60));
    }

    /** Every committed output exactly once, and per key in the order produced. */
    private static void assertExactlyOnceInOrder(List<ConsumerRecord<String, String>> out, int expected) {
        assertEquals(expected, out.size(), "every input must reach the output exactly once");
        Set<Integer> seen = new TreeSet<>();
        Map<String, Integer> lastByKey = new HashMap<>();
        for (ConsumerRecord<String, String> record : out) {
            int value = Integer.parseInt(record.value());
            assertTrue(seen.add(value), "duplicate output for input " + value);
            Integer last = lastByKey.put(record.key(), value);
            assertTrue(last == null || last < value,
                    "output for key " + record.key() + " reordered: " + last + " before " + value);
        }
        assertEquals(expected, seen.size());
    }

    /**
     * Starts an instance, retrying the one refusal a host documents as retryable: the
     * Streams host determines prior state by reading the ordering changelog before its
     * application starts, and a sibling committing while it reads refuses the start with
     * {@code RetryableStartException} and the instruction to retry (D88). The kafka-clients
     * host reads state per task after assignment and has no such window. Each retry is
     * counted and logged, so a run's log shows which host needed it (D115).
     */
    private static Parsley startRetrying(ParsleyConfig config, ProcessDefinition definition) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return Parsley.start(config, definition);
            } catch (ParsleyRuntime.RetryableStartException e) {
                if (attempt == 10) {
                    throw e;
                }
                System.err.println("[torture] start of " + config.applicationIdPrefix() + " under " + config.host()
                        + " refused as retryable (attempt " + attempt + "): " + e.getMessage());
                Thread.sleep(500);
            }
        }
    }

    private static ProcessDefinition relay(String name, Channel<String, String> in, Channel<String, String> out) {
        return ProcessDefinition.named(name)
                .receives(in, (d, s) -> Effects.builder().send(out, d.key(), d.value()).build())
                .sends(out)
                .build();
    }

    /**
     * A step that fails part-way through a burst aborts the interval it sits in; the
     * restart re-feeds from the last commit and the output carries every input exactly
     * once, in order, with nothing from the aborted interval committed twice.
     */
    @Test
    void aStepAbortedMidBurstReplaysExactlyOnceInOrder() throws Exception {
        createTopics(1, "burst-in", "burst-out");
        Channel<String, String> in = Channel.of("burst-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("burst-out", Serdes.String(), Serdes.String());
        AtomicBoolean crashed = new AtomicBoolean();
        ProcessDefinition p = ProcessDefinition.named("pb")
                .receives(in, (d, s) -> {
                    if (d.value().equals("100") && crashed.compareAndSet(false, true)) {
                        throw new IllegalStateException("injected crash mid-burst");
                    }
                    return Effects.builder().send(out, d.key(), d.value()).build();
                })
                .sends(out)
                .build();

        Parsley first = Parsley.start(config("burst"), p);
        try {
            for (int i = 0; i < 200; i++) {
                produce("burst-in", "k" + (i % 3), String.valueOf(i));
            }
            await("the process to stop at the injected crash", () -> !first.healthy(), Duration.ofSeconds(120));
        } finally {
            first.close();
        }
        assertTrue(crashed.get(), "the crash must have been injected");

        try (Parsley second = Parsley.start(config("burst"), p)) {
            List<ConsumerRecord<String, String>> outRecords = new ArrayList<>();
            await("all 200 outputs committed", () -> {
                outRecords.clear();
                outRecords.addAll(readAllCommitted("burst-out"));
                return outRecords.size() >= 200;
            }, Duration.ofSeconds(120));
            assertExactlyOnceInOrder(outRecords, 200);
            assertTrue(second.healthy());
        }
    }

    /**
     * Tasks migrate between instances while records flow: a second instance joins mid-stream
     * and the first leaves mid-stream, each a rebalance with transactions open. Every input
     * reaches the output exactly once and per-key order holds across both migrations.
     */
    @Test
    void tasksMigrateUnderLoadWithoutLossOrDuplication() throws Exception {
        createTopics(4, "load-in", "load-out");
        Channel<String, String> in = Channel.of("load-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("load-out", Serdes.String(), Serdes.String());
        ProcessDefinition p = relay("pl", in, out);

        Parsley a = Parsley.start(configBuilder("load", "a").build(), p);
        Parsley b = null;
        try {
            for (int i = 0; i < 400; i++) {
                produce("load-in", "k" + (i % 8), String.valueOf(i));
                if (i == 100) {
                    b = startRetrying(configBuilder("load", "b").build(), p);
                }
                if (i == 250) {
                    a.close();
                }
                Thread.sleep(5);
            }
            Parsley survivor = b;
            List<ConsumerRecord<String, String>> outRecords = new ArrayList<>();
            await("all 400 outputs committed", () -> {
                outRecords.clear();
                outRecords.addAll(readAllCommitted("load-out"));
                return outRecords.size() >= 400;
            }, Duration.ofSeconds(180));
            Thread.sleep(2_000);
            outRecords.clear();
            outRecords.addAll(readAllCommitted("load-out"));
            assertExactlyOnceInOrder(outRecords, 400);
            assertTrue(survivor.healthy(), "the surviving instance must be healthy after both migrations");
        } finally {
            a.close();
            if (b != null) {
                b.close();
            }
        }
    }

    /**
     * A member stalled inside a handler past its poll interval and its transaction timeout
     * is superseded: a second instance takes the partition and processes the same record.
     * When the stalled step resumes it must not commit — its output must not appear twice
     * — and the stalled instance must recover in place rather than die.
     */
    @Test
    void aStalledMemberIsFencedAndItsStepIsNotDuplicated() throws Exception {
        createTopics(1, "stall-in", "stall-out");
        Channel<String, String> in = Channel.of("stall-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("stall-out", Serdes.String(), Serdes.String());
        CountDownLatch stalled = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ProcessDefinition slow = ProcessDefinition.named("ps")
                .receives(in, (d, s) -> {
                    if (d.value().equals("0")) {
                        stalled.countDown();
                        try {
                            release.await(60, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return Effects.builder().send(out, d.key(), d.value()).build();
                })
                .sends(out)
                .build();
        ProcessDefinition fast = relay("ps", in, out);

        ParsleyConfig stalling = configBuilder("stall", "slow")
                .streamsProperty("max.poll.interval.ms", 8_000)
                .build();
        Parsley slowInstance = Parsley.start(stalling, slow);
        Parsley fastInstance = null;
        try {
            produce("stall-in", "k", "0");
            assertTrue(stalled.await(60, TimeUnit.SECONDS), "the slow instance must reach the stalling step");
            fastInstance = Parsley.start(configBuilder("stall", "fast").build(), fast);
            for (int i = 1; i <= 10; i++) {
                produce("stall-in", "k", String.valueOf(i));
            }
            // Past the poll interval (8 s) and the transaction timeout (10 s): the slow
            // member has been kicked and its transaction aborted by the coordinator.
            Thread.sleep(15_000);
            release.countDown();

            Parsley takeover = fastInstance;
            List<ConsumerRecord<String, String>> outRecords = new ArrayList<>();
            await("all 11 outputs committed", () -> {
                outRecords.clear();
                outRecords.addAll(readAllCommitted("stall-out"));
                return outRecords.size() >= 11;
            }, Duration.ofSeconds(120));
            Thread.sleep(3_000);
            outRecords.clear();
            outRecords.addAll(readAllCommitted("stall-out"));
            assertExactlyOnceInOrder(outRecords, 11);
            await("the stalled instance to recover in place", slowInstance::healthy, Duration.ofSeconds(60));
            assertTrue(takeover.healthy(), "the instance that took over must be healthy");
        } finally {
            release.countDown();
            slowInstance.close();
            if (fastInstance != null) {
                fastInstance.close();
            }
        }
    }

    /**
     * A superseded incarnation can leave a transaction open on the state topic below records
     * a successor committed. A restore that stops at the last stable offset would silently
     * drop that state; it must read to the log's end, waiting out the open transaction.
     * Constructed with a foreign transactional producer holding a transaction open on the
     * ordering-state partition while the process commits above it.
     */
    @Test
    void restoreReadsPastAForeignOpenTransactionOnTheStateTopic() throws Exception {
        createTopics(1, "fore-a", "fore-b");
        Channel<String, String> a = Channel.of("fore-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("fore-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pf = ProcessDefinition.named("pf")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();
        RecordHeader cause = causesHeader(Map.of(channel("fore-a", 0), 0L));
        String stateTopic = orderingStateTopic("fore-pf");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "zombie-" + UUID.randomUUID());
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 20_000);
        try (var zombie = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            try (Parsley parsley = Parsley.start(config("fore"), pf)) {
                produce("fore-b", "k", "B0", cause);
                awaitHeld(parsley, "pf", "fore-b", 1);
                Thread.sleep(2_000);

                zombie.initTransactions();
                zombie.beginTransaction();
                zombie.send(new ProducerRecord<>(stateTopic, 0, "zombie", "uncommitted")).get(30, TimeUnit.SECONDS);

                produce("fore-b", "k", "B1", cause);
                awaitHeld(parsley, "pf", "fore-b", 2);
                Thread.sleep(2_000);
            }
            deleteRecursively(stateDir.resolve("fore-1"));

            try (Parsley parsley = Parsley.start(config("fore"), pf)) {
                produce("fore-a", "k", "A");
                await("A then both holds after a restore across the open transaction",
                        () -> delivered.size() == 3, Duration.ofSeconds(120));
                assertEquals(List.of("A", "B0", "B1"), List.copyOf(delivered),
                        "state committed above the open transaction must be restored, in order");
                assertTrue(parsley.healthy());
            }
            zombie.abortTransaction();
        }
    }

    /**
     * Fifty messages held behind one missing cause migrate from one instance to another
     * and are delivered exactly once, in order, when the cause arrives at the survivor.
     */
    @Test
    void aDeepHoldBackBufferSurvivesMigrationAndDeliversOnceInOrder() throws Exception {
        createTopics(1, "deep-a", "deep-b");
        Channel<String, String> a = Channel.of("deep-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("deep-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pd = ProcessDefinition.named("pd")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();
        RecordHeader cause = causesHeader(Map.of(channel("deep-a", 0), 0L));

        Parsley first = Parsley.start(configBuilder("deep", "1").build(), pd);
        try {
            for (int i = 0; i < 50; i++) {
                produce("deep-b", "k", "B" + i, cause);
            }
            awaitHeld(first, "pd", "deep-b", 50);
            Thread.sleep(2_000);
            try (Parsley second = Parsley.start(configBuilder("deep", "2").build(), pd)) {
                first.close();
                produce("deep-a", "k", "A");
                await("A then fifty holds on the survivor", () -> delivered.size() == 51, Duration.ofSeconds(120));
                Thread.sleep(2_000);
                List<String> expected = new ArrayList<>();
                expected.add("A");
                for (int i = 0; i < 50; i++) {
                    expected.add("B" + i);
                }
                assertEquals(expected, List.copyOf(delivered), "the migrated buffer must deliver once, in order");
                assertTrue(second.healthy());
            }
        } finally {
            first.close();
        }
    }

    /**
     * Retention crossing a held message while the process is stopped is a message the
     * process received but can no longer place: the restart refuses with
     * {@code POSITIONS_DISCARDED_UNREAD} and never delivers it (D104, SPEC Safety 8).
     */
    @Test
    void retentionCrossingAHeldMessageRefusesRatherThanSkips() throws Exception {
        createTopics(1, "ret-a", "ret-b");
        Channel<String, String> a = Channel.of("ret-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("ret-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pr = ProcessDefinition.named("pr")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("ret"), pr)) {
            produce("ret-b", "k", "B", causesHeader(Map.of(channel("ret-a", 0), 0L)));
            awaitHeld(parsley, "pr", "ret-b", 1);
            Thread.sleep(2_000);
        }
        admin.deleteRecords(Map.of(new TopicPartition("ret-b", 0), RecordsToDelete.beforeOffset(1)))
                .all().get(30, TimeUnit.SECONDS);
        produce("ret-a", "k", "A");

        Parsley parsley = Parsley.start(config("ret"), pr);
        try {
            await("the process to refuse", () -> !parsley.healthy(), Duration.ofSeconds(120));
            await("the refusal to settle in status", () -> parsley.status().get("pr").refusalReason().isPresent(),
                    Duration.ofSeconds(30));
            assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD,
                    parsley.status().get("pr").refusalReason().orElseThrow());
            assertFalse(delivered.contains("B"), "a message retention discarded while held must never deliver");
        } finally {
            parsley.close();
        }
    }

    /**
     * Through a full broker outage a held effect stays held, and after recovery real
     * evidence still releases it, in order, exactly once, with the process healthy.
     */
    @Test
    void aHeldMessageSurvivesABrokerBounceAndReleasesOnlyByEvidence() throws Exception {
        createTopics(1, "bounce-a", "bounce-b");
        Channel<String, String> a = Channel.of("bounce-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("bounce-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition pb = ProcessDefinition.named("pb")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();
        produce("bounce-b", "k", "B", causesHeader(Map.of(channel("bounce-a", 0), 0L)));

        try (Parsley parsley = Parsley.start(config("bounce"), pb)) {
            awaitHeld(parsley, "pb", "bounce-b", 1);

            var broker = cluster.brokers().values().iterator().next();
            broker.shutdown();
            broker.awaitShutdown();
            Thread.sleep(3_000);
            broker.startup();
            cluster.waitForReadyBrokers();

            Thread.sleep(3_000);
            assertEquals(List.of(), List.copyOf(delivered),
                    "the outage and recovery must not manufacture evidence that frees the hold");

            produce("bounce-a", "k", "A");
            await("A then B after the bounce", () -> delivered.size() == 2, Duration.ofSeconds(120));
            assertEquals(List.of("A", "B"), List.copyOf(delivered));
            assertTrue(parsley.healthy(), "the process must ride out the bounce without failing");
        }
    }

    /**
     * Two instances cold-starting together, with no prior state and no committed positions,
     * settle their initial positions once and share the tasks: every input reaches the
     * output exactly once and both stay healthy.
     */
    @Test
    void twoInstancesColdStartingTogetherDeliverExactlyOnce() throws Exception {
        createTopics(2, "cold-in", "cold-out");
        Channel<String, String> in = Channel.of("cold-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("cold-out", Serdes.String(), Serdes.String());
        ProcessDefinition p = relay("pc", in, out);

        AtomicReference<Parsley> a = new AtomicReference<>();
        AtomicReference<Parsley> b = new AtomicReference<>();
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread startA = new Thread(() -> {
            try {
                a.set(startRetrying(configBuilder("cold", "a").build(), p));
            } catch (Throwable t) {
                startFailure.compareAndSet(null, t);
            }
        });
        Thread startB = new Thread(() -> {
            try {
                b.set(startRetrying(configBuilder("cold", "b").build(), p));
            } catch (Throwable t) {
                startFailure.compareAndSet(null, t);
            }
        });
        startA.start();
        startB.start();
        startA.join(120_000);
        startB.join(120_000);
        try {
            assertTrue(startFailure.get() == null, "both cold starts must succeed: " + startFailure.get());
            for (int i = 0; i < 100; i++) {
                produce("cold-in", "k" + (i % 4), String.valueOf(i));
            }
            List<ConsumerRecord<String, String>> outRecords = new ArrayList<>();
            await("all 100 outputs committed", () -> {
                outRecords.clear();
                outRecords.addAll(readAllCommitted("cold-out"));
                return outRecords.size() >= 100;
            }, Duration.ofSeconds(180));
            Thread.sleep(2_000);
            outRecords.clear();
            outRecords.addAll(readAllCommitted("cold-out"));
            assertExactlyOnceInOrder(outRecords, 100);
            assertTrue(a.get().healthy() && b.get().healthy(), "both instances must be healthy");
        } finally {
            if (a.get() != null) {
                a.get().close();
            }
            if (b.get() != null) {
                b.get().close();
            }
        }
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (!java.nio.file.Files.exists(root)) {
            return;
        }
        try (var paths = java.nio.file.Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.delete(path);
            }
        }
    }
}
