package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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
 * Establishes how topic identity is handled against a real broker.
 *
 * <p>Covers recreation during a run and across a restart, and authorization denial, which is
 * treated as denial rather than as death.
 */
class IdentityIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @TempDir
    static Path stateDir;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of(
                "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "allow.everyone.if.no.acl.found", "true"));
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
                .factsInterval(Duration.ofMillis(500))
                .build();
    }

    private static void produce(String topic, String key, String value, RecordHeader... headers) {
        ClusterTestSupport.produce(cluster.bootstrapServers(), topic, key, value, headers);
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

    private static void denyDescribe(String topic) throws Exception {
        admin.createAcls(List.of(new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY))))
                .all().get(30, TimeUnit.SECONDS);
    }

    private static void allow(String topic, AclOperation operation) throws Exception {
        admin.createAcls(List.of(new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:ANONYMOUS", "*", operation, AclPermissionType.ALLOW))))
                .all().get(30, TimeUnit.SECONDS);
    }

    private static void dropAcls(String topic) throws Exception {
        admin.deleteAcls(List.of(new AclBindingFilter(
                new ResourcePatternFilter(org.apache.kafka.common.resource.ResourceType.TOPIC, topic,
                        PatternType.LITERAL),
                AccessControlEntryFilter.ANY))).all().get(30, TimeUnit.SECONDS);
    }

    private static RecordHeader causesHeader(Map<ChannelId, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    private static List<Causes> emittedCauses(String topic) {
        List<Causes> causes = new ArrayList<>();
        for (var record : ClusterTestSupport.readAllCommitted(cluster.bootstrapServers(), topic)) {
            try {
                causes.add(CausesCodec.decode(record.headers().lastHeader(CausesCodec.HEADER_KEY).value()));
            } catch (CausesCodec.UndecodableMetadataException e) {
                throw new AssertionError("emitted an undecodable header", e);
            }
        }
        return causes;
    }

    /** Mid run recreation of a received topic stops the process. */
    @Test
    void midRunRecreationOfAReceivedTopicStopsTheProcess() throws Exception {
        createTopics("rr-in");
        Channel<String, String> in = Channel.of("rr-in", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition p = ProcessDefinition.named("rr")
                .receives(in, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("rr"), p)) {
            for (int i = 0; i < 5; i++) {
                produce("rr-in", "k", "m" + i);
            }
            await("all five to deliver", () -> delivered.size() == 5, Duration.ofSeconds(60));
            awaitCommitted("rr-rr", "rr-in", 5);

            admin.deleteTopics(List.of("rr-in")).all().get(30, TimeUnit.SECONDS);
            Thread.sleep(200);
            createTopics("rr-in");

            for (int i = 0; i < 8; i++) {
                produce("rr-in", "k", "v" + i);
            }

            await("the process to stop rather than adopt the new incarnation",
                    () -> !parsley.healthy(), Duration.ofSeconds(30));
            assertTrue(delivered.stream().filter(v -> v.startsWith("m")).count() == 5,
                    "the old incarnation's deliveries stand");

            await("the failure to be recorded in the status surface",
                    () -> parsley.status().get("rr").failureDetail().isPresent(), Duration.ofSeconds(30));
        }
    }

    /** Deliberate refusal is readable in the status surface. */
    @Test
    void deliberateRefusalIsReadableInTheStatusSurface() throws Exception {
        createTopics("sr-in", "sr-x");
        UUID xId = topicId("sr-x");
        Channel<String, String> in = Channel.of("sr-in", Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("sr")
                .receives(in, (d, s) -> Effects.none())
                .build();
        ParsleyConfig tinyBudget = ParsleyConfig.builder(cluster.bootstrapServers(), "sr")
                .stateDir(stateDir.resolve("sr").toString())
                .factsInterval(Duration.ofMillis(500))
                .metadataBudgetBytes(64)
                .build();

        try (Parsley parsley = Parsley.start(tinyBudget, p)) {
            Map<ChannelId, Long> big = new java.util.TreeMap<>();
            for (int partition = 0; partition < 5; partition++) {
                big.put(new ChannelId(xId, partition), 1L);
            }
            produce("sr-in", "k", "H", causesHeader(big));

            await("the budget refusal to surface", () -> {
                var status = parsley.status().get("sr");
                return status != null && status.refusalReason().isPresent();
            }, Duration.ofSeconds(60));
            io.github.tobyjamesclements.parsley.api.ProcessStatus status = parsley.status().get("sr");
            assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED,
                    status.refusalReason().orElseThrow());
            assertTrue(status.stoppedDeliberately(),
                    "the stop reads as a deliberate refusal, which recurs identically on restart");
            assertFalse(parsley.healthy(), "healthy() reflects the recorded failure at once");
        }
    }

    /** Denied describe on a frontier topic does not prune its cause. */
    @Test
    void deniedDescribeOnAFrontierTopicDoesNotPruneItsCause() throws Exception {
        createTopics("acl-in", "acl-out", "acl-x");
        UUID xId = topicId("acl-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        Channel<String, String> in = Channel.of("acl-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("acl-out", Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("acl")
                .receives(in, (d, s) -> Effects.builder().send(out, d.key(), d.value()).build())
                .sends(out)
                .build();

        try (Parsley parsley = Parsley.start(config("acl"), p)) {
            produce("acl-in", "k", "first", causesHeader(Map.of(xChannel, 0L)));
            await("the first emission", () -> emittedCauses("acl-out").size() >= 1, Duration.ofSeconds(60));
            assertEquals(0L, emittedCauses("acl-out").get(0).byChannel().get(xChannel),
                    "the injected cause must ride the frontier");

            denyDescribe("acl-x");

            Thread.sleep(4_000);
            produce("acl-in", "k", "second");
            await("the second emission", () -> emittedCauses("acl-out").size() >= 2, Duration.ofSeconds(60));
            List<Causes> causes = emittedCauses("acl-out");
            assertEquals(0L, causes.get(causes.size() - 1).byChannel().get(xChannel),
                    "a denied describe is not evidence of death: the cause must still be expressed");
            assertTrue(parsley.healthy(), "denial is not a failure of this process");
        } finally {
            dropAcls("acl-x");
        }
    }

    /** Denied describe on a received topic does not release held messages. */
    @Test
    void deniedDescribeOnAReceivedTopicDoesNotReleaseHeldMessages() throws Exception {
        createTopics("aclr-in", "aclr-x");
        UUID xId = topicId("aclr-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        Channel<String, String> in = Channel.of("aclr-in", Serdes.String(), Serdes.String());
        Channel<String, String> x = Channel.of("aclr-x", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition p = ProcessDefinition.named("aclr")
                .receives(in, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(x, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        try (Parsley parsley = Parsley.start(config("aclr"), p)) {
            produce("aclr-x", "k", "x0");
            await("x0 to deliver", () -> delivered.contains("x0"), Duration.ofSeconds(60));

            allow("aclr-x", AclOperation.READ);
            allow("aclr-x", AclOperation.WRITE);
            denyDescribe("aclr-x");
            produce("aclr-in", "k", "R", causesHeader(Map.of(xChannel, 3L)));

            Thread.sleep(4_000);
            assertFalse(delivered.contains("R"),
                    "a denial-masked describe must not settle positions that may still arrive");
            assertTrue(parsley.healthy(), "the process holds and waits; denial is not its failure");

            dropAcls("aclr-x");
            produce("aclr-x", "k", "x1");
            produce("aclr-x", "k", "x2");
            produce("aclr-x", "k", "x3");
            await("R to deliver once its cause's positions really arrived",
                    () -> delivered.contains("R"), Duration.ofSeconds(60));
            List<String> order = List.copyOf(delivered);
            assertTrue(order.indexOf("x3") < order.indexOf("R"),
                    "the cause's positions deliver before the effect");
        } finally {
            dropAcls("aclr-x");
        }
    }

    /** Recreation across a restart is diagnosed as identity change not removal. */
    @Test
    void recreationAcrossARestartIsDiagnosedAsIdentityChangeNotRemoval() throws Exception {
        createTopics("md-in", "md-x");
        UUID xId = topicId("md-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        Channel<String, String> in = Channel.of("md-in", Serdes.String(), Serdes.String());
        Channel<String, String> x = Channel.of("md-x", Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("md")
                .receives(in, (d, s) -> Effects.none())
                .receives(x, (d, s) -> Effects.none())
                .build();

        try (Parsley parsley = Parsley.start(config("md"), p)) {
            produce("md-in", "k", "R", causesHeader(Map.of(xChannel, 5L)));
            awaitCommitted("md-md", "md-in", 1);
        }

        admin.deleteTopics(List.of("md-in")).all().get(30, TimeUnit.SECONDS);
        Thread.sleep(200);
        createTopics("md-in");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> Parsley.start(config("md"), p));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason(),
                "nothing was removed from the declaration: the topic's identity changed, and the remedy is a"
                        + " deliberate reset, not a declaration fix");
    }

    /**
     * A frontier id the admin client deems unrepresentable — the reserved zero id, which
     * the client answers locally with {@code InvalidTopicException}, never sending a
     * request — must degrade to "no facts for that channel", not abort the round: the
     * round is the only mid-run carrier of verdicts and settling evidence for every other
     * channel, and ordering state persisted before the decode refusal existed (D83) can
     * still carry such an id in its restored frontier.
     */
    @Test
    void zeroTopicIdInTheFrontierDoesNotAbortTheFactsRound() throws Exception {
        createTopics("zid-in");
        UUID inId = topicId("zid-in");
        ChannelId received = new ChannelId(inId, 0);
        AdminFactsSource source = new AdminFactsSource(admin, "zid-group", Map.of(inId, "zid-in"),
                Map.of("bootstrap.servers", cluster.bootstrapServers()), 1_000L,
                () -> System.nanoTime() / 1_000_000L);

        var facts = source.gather(Set.of(received), Map.of(),
                Set.of(new ChannelId(new UUID(0, 0), 0)));

        assertEquals(Map.of(received, 0L), facts.logStart(),
                "the round must still settle the live channel's facts despite the unanswerable frontier id");
        assertTrue(facts.deadChannels().isEmpty(),
                "an unanswerable id is not evidence of death");
    }
}
