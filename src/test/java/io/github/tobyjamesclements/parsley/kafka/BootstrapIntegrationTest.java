package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.Parsley;
import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 300, unit = TimeUnit.SECONDS)
/**
 * Establishes the start sequence against a real broker.
 *
 * <p>Covers position pre-commit under group fencing, expired positions, refusal where held
 * messages are stranded, the dead-channel confirmation window, and a width-changing restart.
 */
class BootstrapIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @TempDir
    static Path stateDir;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of("group.min.session.timeout.ms", "1000"));
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        ClusterTestSupport.stopCluster(cluster, admin);
    }

    private static void createTopics(NewTopic... topics) throws Exception {
        admin.createTopics(List.of(topics)).all().get(30, TimeUnit.SECONDS);
    }

    private static ParsleyConfig config(String prefix) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), prefix)
                .stateDir(stateDir.resolve(prefix).toString())
                .factsInterval(Duration.ofMillis(500))
                .build();
    }

    private static void produce(String topic, Integer partition, String key, String value, RecordHeader... headers) {
        ClusterTestSupport.produce(cluster.bootstrapServers(), topic, partition, key, value, headers);
    }

    private static UUID topicId(String topic) throws Exception {
        return ClusterTestSupport.topicId(admin, topic);
    }

    private static void await(String what, BooleanSupplier condition, Duration timeout) {
        ClusterTestSupport.await(what, condition, timeout);
    }

    private static void awaitCommitted(String groupId, String topic, long atLeast) {
        ClusterTestSupport.awaitCommitted(admin, groupId, topic, atLeast);
    }

    private static RecordHeader causesHeader(Map<ChannelId, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    /**
     * Bootstrap membership stays dynamic: a static-membership config cannot defeat
     * leave-on-close. A static member sends no LeaveGroup when closed, so its slot would
     * hold the group — under the consumer protocol — for the full session timeout, failing
     * the Streams start that immediately follows bootstrap.
     */
    @Test
    void bootstrapMemberLeavesOnCloseDespiteStaticMembershipConfig() throws Exception {
        createTopics(new NewTopic("m2-in", 1, (short) 1));
        Map<String, Object> clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", cluster.bootstrapServers());
        clientProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "static-1");
        clientProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60_000);

        try (GroupMembershipCommitter committer = new GroupMembershipCommitter(clientProps, "m2-group")) {
            committer.join(Set.of("m2-in"), Duration.ofSeconds(30));
        }

        await("the closed bootstrap member to have left the group", () -> {
            try {
                return admin.describeConsumerGroups(List.of("m2-group")).all()
                        .get(30, TimeUnit.SECONDS).get("m2-group").members().isEmpty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, Duration.ofSeconds(10));
    }

    /** Stale bootstrap commit is fenced by group membership. */
    @Test
    void staleBootstrapCommitIsFencedByGroupMembership() throws Exception {
        createTopics(new NewTopic("fence-in", 1, (short) 1));
        TopicPartition tp = new TopicPartition("fence-in", 0);
        Map<String, Object> clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", cluster.bootstrapServers());
        clientProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
        clientProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1001);
        clientProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 333);

        try (GroupMembershipCommitter stale = new GroupMembershipCommitter(clientProps, "fence-group")) {
            stale.join(Set.of("fence-in"), Duration.ofSeconds(30));
            stale.committed(Set.of(tp));

            Map<String, Object> newerProps = new HashMap<>(clientProps);
            newerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fence-group");
            newerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            try (var newer = new KafkaConsumer<>(newerProps,
                    new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                newer.subscribe(List.of("fence-in"));
                await("the newer lifetime to be assigned", () -> {
                    newer.poll(Duration.ofMillis(100));
                    return !newer.assignment().isEmpty();
                }, Duration.ofSeconds(30));
                newer.commitSync(Map.of(tp, new OffsetAndMetadata(7)));
            }

            assertThrows(CommitFailedException.class,
                    () -> stale.commit(Map.of(tp, new OffsetAndMetadata(0))),
                    "a stale membership's commit must be rejected by the broker's generation fencing");
        }
        var committed = admin.listConsumerGroupOffsets("fence-group").partitionsToOffsetAndMetadata()
                .get(30, TimeUnit.SECONDS);
        assertEquals(7, committed.get(tp).offset(), "the newer lifetime's offsets stand untouched");
    }

    /**
     * The ordering changelog is created compacted. Changelog restore — and the F2 refusal's
     * premise that the version entry at the head of the changelog outlives every rewrite —
     * both rest on this policy, so its presence is asserted rather than assumed.
     */
    @Test
    void orderingChangelogIsCreatedCompacted() throws Exception {
        createTopics(new NewTopic("clog-in", 1, (short) 1));
        Channel<String, String> in = Channel.of("clog-in", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition p = ProcessDefinition.named("pc")
                .receives(in, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("clog"), p)) {
            produce("clog-in", null, "k", "v");
            await("the message to deliver", () -> delivered.size() == 1, Duration.ofSeconds(120));
        }

        var resource = new org.apache.kafka.common.config.ConfigResource(
                org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                "clog-pc-__parsley.ordering-changelog");
        var config = admin.describeConfigs(List.of(resource)).all().get(30, TimeUnit.SECONDS).get(resource);
        assertEquals("compact",
                config.get(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG).value(),
                "the ordering changelog must be compacted; state restore depends on it");
    }

    /** Expired offsets restart from earliest not the declared latest. */
    @Test
    void expiredOffsetsRestartFromEarliestNotTheDeclaredLatest() throws Exception {
        createTopics(new NewTopic("ex-in", 1, (short) 1));
        produce("ex-in", null, "k", "early");
        Channel<String, String> in = Channel.of("ex-in", Serdes.String(), Serdes.String())
                .startingAt(Channel.InitialPosition.LATEST);
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition p = ProcessDefinition.named("ex")
                .receives(in, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("ex"), p)) {
            produce("ex-in", null, "k", "live");
            await("the live record to deliver", () -> delivered.contains("live"), Duration.ofSeconds(60));
            assertFalse(delivered.contains("early"), "LATEST on a genuine first start skips history");
            awaitCommitted("ex-ex", "ex-in", 2);
        }

        await("the group's offsets to be deletable", () -> {
            try {
                admin.deleteConsumerGroupOffsets("ex-ex", Set.of(new TopicPartition("ex-in", 0)))
                        .all().get(10, TimeUnit.SECONDS);
                return true;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(60));
        produce("ex-in", null, "k", "while-stopped");

        try (Parsley parsley = Parsley.start(config("ex"), p)) {
            await("the record produced while stopped to deliver",
                    () -> delivered.contains("while-stopped"), Duration.ofSeconds(60));
            assertEquals(1, delivered.stream().filter("live"::equals).count(),
                    "re-fed history is dropped by the session floor, not redelivered");
            assertFalse(delivered.contains("early"),
                    "positions below the first execution's baseline stay covered");
        }
    }

    /** Stranded held messages refuse at start. */
    @Test
    void strandedHeldMessagesRefuseAtStart() throws Exception {
        createTopics(new NewTopic("st-a", 1, (short) 1), new NewTopic("st-b", 1, (short) 1));
        UUID aId = topicId("st-a");
        Channel<String, String> a = Channel.of("st-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("st-b", Serdes.String(), Serdes.String());
        ProcessDefinition both = ProcessDefinition.named("st")
                .receives(a, (d, s) -> Effects.none())
                .receives(b, (d, s) -> Effects.none())
                .build();

        try (Parsley parsley = Parsley.start(config("st"), both)) {
            produce("st-b", null, "k", "H", causesHeader(Map.of(new ChannelId(aId, 0), 9L)));
            awaitCommitted("st-st", "st-b", 1);
        }

        ProcessDefinition withoutB = ProcessDefinition.named("st")
                .receives(a, (d, s) -> Effects.none())
                .build();
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> Parsley.start(config("st"), withoutB));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES, e.reason());
    }

    /** Dead verdict waits out the confirmation window. */
    @Test
    void deadVerdictWaitsOutTheConfirmationWindow() throws Exception {
        createTopics(new NewTopic("db-x", 1, (short) 1));
        UUID xId = topicId("db-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        long[] clock = {1_000_000L};
        AdminFactsSource facts = new AdminFactsSource(admin, "db-group", Map.of(xId, "db-x"),
                Map.of("bootstrap.servers", cluster.bootstrapServers()), 1_500, () -> clock[0]);

        assertTrue(facts.gather(Set.of(), Map.of(), Set.of(xChannel)).deadChannels().isEmpty(),
                "a live topic is never dead");
        admin.deleteTopics(List.of("db-x")).all().get(30, TimeUnit.SECONDS);
        await("the deletion to propagate", () -> {
            try {
                return facts.gather(Set.of(), Map.of(), Set.of(xChannel)).logStart().isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(30));

        assertTrue(facts.gather(Set.of(), Map.of(), Set.of(xChannel)).deadChannels().isEmpty(),
                "an unknown id younger than the confirmation window reports nothing");
        clock[0] += 1_000;
        assertTrue(facts.gather(Set.of(), Map.of(), Set.of(xChannel)).deadChannels().isEmpty(),
                "still inside the window: still nothing");
        clock[0] += 1_000;
        assertEquals(Set.of(xChannel), facts.gather(Set.of(), Map.of(), Set.of(xChannel)).deadChannels(),
                "corroborated-unknown for the whole window: dead, terminally");
        facts.close();
    }

    /** Log start is not attributed across an unconfirmed identity. */
    @Test
    void logStartIsNotAttributedAcrossAnUnconfirmedIdentity() throws Exception {
        createTopics(new NewTopic("cf-x", 1, (short) 1));
        produce("cf-x", null, "k", "r0");
        UUID xId = topicId("cf-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        Map<String, Object> props = Map.of("bootstrap.servers", cluster.bootstrapServers());

        AdminFactsSource honest = new AdminFactsSource(admin, "cf-group", Map.of(xId, "cf-x"),
                props, 1_500, () -> 0L);
        assertEquals(0L, honest.gather(Set.of(), Map.of(), Set.of(xChannel)).logStart().get(xChannel),
                "control: with a stable identity the log start is attributed");
        honest.close();

        AdminFactsSource racedByRecreation = new AdminFactsSource(admin, "cf-group", Map.of(xId, "cf-x"),
                props, 1_500, () -> 0L) {
            private int describes;

            @Override
            Map<UUID, String> describeByIds(Set<UUID> topicIds) throws Exception {
                Map<UUID, String> real = super.describeByIds(topicIds);

                return ++describes % 2 == 0 ? Map.of() : real;
            }
        };
        assertTrue(racedByRecreation.gather(Set.of(), Map.of(), Set.of(xChannel)).logStart().isEmpty(),
                "an identity that did not hold across the offsets query attributes nothing (D22)");
        racedByRecreation.close();
    }

    /** Width changing restart is refused with the accurate diagnosis. */
    @Test
    void widthChangingRestartIsRefusedWithTheAccurateDiagnosis() throws Exception {
        createTopics(new NewTopic("mp-in", 3, (short) 1), new NewTopic("mp-single", 1, (short) 1));
        Channel<String, String> wide = Channel.of("mp-in", Serdes.String(), Serdes.String());
        Channel<String, String> narrow = Channel.of("mp-single", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition both = ProcessDefinition.named("mp")
                .receives(wide, (d, s) -> {
                    delivered.add("mp-in[" + d.partition() + "]=" + d.value());
                    return Effects.none();
                })
                .receives(narrow, (d, s) -> {
                    delivered.add("mp-single=" + d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("mp"), both)) {
            produce("mp-in", 0, "k", "w0");
            produce("mp-in", 1, "k", "w1");
            produce("mp-in", 2, "k", "w2");
            produce("mp-single", 0, "k", "n0");
            await("every task of every partition to deliver", () -> delivered.size() == 4, Duration.ofSeconds(90));
            assertTrue(delivered.containsAll(List.of(
                            "mp-in[0]=w0", "mp-in[1]=w1", "mp-in[2]=w2", "mp-single=n0")),
                    "multi-task operation: each partition's task delivers its own channel");
        }

        ProcessDefinition narrowOnly = ProcessDefinition.named("mp")
                .receives(narrow, (d, s) -> Effects.none())
                .build();
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> Parsley.start(config("mp"), narrowOnly));
        assertEquals(ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED, e.reason(),
                "the width change is parsley's condition to name, with a remedy that does not destroy state");
    }
}
