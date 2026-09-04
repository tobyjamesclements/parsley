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
 * <p>Covers recreation across a restart and deletion during a run, the classification a
 * task's initialisation acts on over a real broker, and authorization denial, which is
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
                .statusInterval(Duration.ofMillis(500))
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

    /**
     * A received topic deleted while messages from it are held — an Assumption 17 breach —
     * stops the process before anything delivers past them (SPEC Safety 9). On this host
     * that happens one of two ways, both fail-closed and neither periodic (D115): a
     * rebalance finds the source topic missing and Kafka Streams stops the thread with its
     * own diagnosis, which {@code status()} carries as a transient the restart refines; or
     * the task's transactional commit fails once the partition is gone (the producer's
     * {@code max.block.ms}, a minute, and the abort that follows can spend another) and
     * Streams re-creates the task, whose initialisation reports the topic deleted and
     * refuses with CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES (D46) — or, when the
     * re-creation's rejoin is what first meets the missing topic, stops the thread. Which
     * one wins is the host's timing (measured: the commit and abort timeouts back to back,
     * a little over two minutes); this pins that one does, and that nothing was delivered.
     */
    @Test
    void aReceivedTopicDeletedWhileHeldFromStopsTheProcessBeforeDeliveringPastTheHold() throws Exception {
        createTopics("dh-a", "dh-b");
        Channel<String, String> a = Channel.of("dh-a", Serdes.String(), Serdes.String());
        Channel<String, String> b = Channel.of("dh-b", Serdes.String(), Serdes.String());
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();
        ProcessDefinition p = ProcessDefinition.named("dh")
                .receives(a, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .receives(b, (d, s) -> {
                    delivered.add(d.value());
                    return Effects.none();
                })
                .build();

        produce("dh-a", "k", "H", causesHeader(Map.of(new ChannelId(topicId("dh-b"), 0), 9L)));

        try (Parsley parsley = Parsley.start(config("dh"), p)) {
            ClusterTestSupport.awaitFedAndHeld(admin, "dh-dh", "dh-a", delivered);
            admin.deleteTopics(List.of("dh-a")).all().get(30, TimeUnit.SECONDS);

            await("the process to stop rather than run on without its received topic",
                    () -> !parsley.healthy(), Duration.ofSeconds(300));
            assertEquals(List.of(), List.copyOf(delivered), "nothing may be delivered past the held message");
            await("the stop to be recorded in the status surface",
                    () -> parsley.status().get("dh").failureDetail().isPresent(), Duration.ofSeconds(30));
            io.github.tobyjamesclements.parsley.api.ProcessStatus status = parsley.status().get("dh");
            boolean refusedAtInitialisation = status.refusalReason()
                    .map(reason -> reason == ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES)
                    .orElse(false);
            boolean stoppedByTheHost = status.refusalReason().isEmpty()
                    && status.failureDetail().orElseThrow().contains("source topics were missing");
            assertTrue(refusedAtInitialisation || stoppedByTheHost,
                    "the stop must be the initialisation's refusal or the host's missing-source-topic stop, got: "
                            + status);
        }
    }

    /**
     * The identity classification a task's initialisation acts on, over a real broker
     * (D115): a live topic is neither deleted nor recreated; a deleted one is deleted only
     * once its name is gone across three consistent answers; a topic recreated under its
     * name makes the id it had a dead incarnation; and an id whose name the source never
     * learned is never confirmed dead at all (D75), since a Describe denial masks a live
     * topic as unknown by id exactly the same way.
     */
    @Test
    void identitySourceClassifiesDeletedRecreatedAndNamelessTopicsAgainstARealBroker() throws Exception {
        createTopics("id-x", "id-y");
        UUID xId = topicId("id-x");
        UUID yId = topicId("id-y");
        AdminTopicIdentitySource source = new AdminTopicIdentitySource(admin, "id-group",
                Map.of(xId, "id-x"), Duration.ofMillis(50));

        assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(xId, yId)), "live topics: nothing gone");

        admin.deleteTopics(List.of("id-x", "id-y")).all().get(30, TimeUnit.SECONDS);
        await("the deletions to propagate", () -> {
            try {
                admin.describeTopics(List.of("id-x")).allTopicNames().get(10, TimeUnit.SECONDS);
                return false;
            } catch (Exception e) {
                return e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
            }
        }, Duration.ofSeconds(30));
        TopicIdentityVerdicts afterDeletion = source.resolve(Set.of(xId, yId));
        assertEquals(Set.of(xId, yId), afterDeletion.deleted(),
                "both names were learned — x declared, y described alive at the first check — so both are deleted");
        assertEquals(Set.of(), afterDeletion.recreated());

        createTopics("id-x");
        await("the recreation to propagate", () -> {
            try {
                return !topicId("id-x").equals(xId);
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(30));
        TopicIdentityVerdicts afterRecreation = source.resolve(Set.of(xId));
        assertEquals(Set.of(xId), afterRecreation.recreated(), "the name resolves to a new id: the old one is dead");
        assertEquals(Set.of(), afterRecreation.deleted());

        AdminTopicIdentitySource nameless = new AdminTopicIdentitySource(admin, "id-group-2", Map.of(),
                Duration.ofMillis(50));
        assertEquals(TopicIdentityVerdicts.NONE, nameless.resolve(Set.of(yId)),
                "an id whose name was never learned is never confirmed dead");
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
                .statusInterval(Duration.ofMillis(500))
                .metadataBudgetBytes(64)
                .build();

        try (Parsley parsley = Parsley.start(tinyBudget, p)) {
            Map<ChannelId, Long> big = new java.util.TreeMap<>();
            // Six single-topic partitions encode to 73 grouped bytes, past the 64-byte
            // budget's raw-length gate (five would land exactly on 64, which the strict
            // gate admits).
            for (int partition = 0; partition < 6; partition++) {
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

    /**
     * A Describe denial on a frontier topic, in place when a task initialises, does not
     * prune its cause: the broker masks the denied topic as unknown by id, the source has
     * no name for a topic this process never declared, and an id without a name is never
     * confirmed dead (D75, D115). The restart is what puts the denial in front of an
     * initialisation's identity check.
     */
    @Test
    void deniedDescribeOnAFrontierTopicDoesNotPruneItsCauseAtInitialisation() throws Exception {
        createTopics("acl-in", "acl-out", "acl-x");
        UUID xId = topicId("acl-x");
        ChannelId xChannel = new ChannelId(xId, 0);
        Channel<String, String> in = Channel.of("acl-in", Serdes.String(), Serdes.String());
        Channel<String, String> out = Channel.of("acl-out", Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("acl")
                .receives(in, (d, s) -> Effects.builder().send(out, d.key(), d.value()).build())
                .sends(out)
                .build();

        try {
            try (Parsley parsley = Parsley.start(config("acl"), p)) {
                produce("acl-in", "k", "first", causesHeader(Map.of(xChannel, 0L)));
                await("the first emission", () -> emittedCauses("acl-out").size() >= 1, Duration.ofSeconds(60));
                assertEquals(0L, emittedCauses("acl-out").get(0).byChannel().get(xChannel),
                        "the injected cause must ride the frontier");
            }

            denyDescribe("acl-x");

            try (Parsley parsley = Parsley.start(config("acl"), p)) {
                produce("acl-in", "k", "second");
                await("the second emission", () -> emittedCauses("acl-out").size() >= 2, Duration.ofSeconds(60));
                List<Causes> causes = emittedCauses("acl-out");
                assertEquals(0L, causes.get(causes.size() - 1).byChannel().get(xChannel),
                        "a denied describe is not evidence of death: the cause must still be expressed");
                assertTrue(parsley.healthy(), "denial is not a failure of this process");
            }
        } finally {
            dropAcls("acl-x");
        }
    }

    /**
     * A Describe denial on a received topic neither releases nor refuses a message held
     * for a cause on it: nothing settles a received channel but receiving the record its
     * cause names (D115), so the process holds and waits, healthy, until the record arrives.
     */
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
}
