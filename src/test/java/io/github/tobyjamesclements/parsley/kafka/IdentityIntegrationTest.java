package io.github.tobyjamesclements.parsley.kafka;

import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

/**
 * Channel identity against a real KRaft broker with a real {@code StandardAuthorizer} (ASSESSMENT 1.1–1.3): a topic
 * recreated under its name mid-run stops the process; an authorization denial masked by the broker as unknown-topic
 * is treated as denial, not death — a frontier cause survives it and a held message is not released by it; and a
 * recreation across a restart is diagnosed as the identity change it is, never as a declaration change. The cluster
 * allows everything unless an ACL exists ({@code allow.everyone.if.no.acl.found}), so only the deliberately denied
 * topics behave differently from the main integration suite's cluster.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class IdentityIntegrationTest {

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
                .setConfigProp("authorizer.class.name",
                        "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                .setConfigProp("allow.everyone.if.no.acl.found", "true")
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

    // ------------------------------------------------------------------ helpers

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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            for (RecordHeader header : headers) {
                record.headers().add(header);
            }
            producer.send(record);
            producer.flush();
        }
    }

    private static UUID topicId(String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        return new UUID(description.topicId().getMostSignificantBits(),
                description.topicId().getLeastSignificantBits());
    }

    private static void await(String what, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting " + what);
            }
        }
        throw new AssertionError("timed out awaiting " + what);
    }

    private static void awaitCommitted(String groupId, String topic, long atLeast) {
        await("group " + groupId + " to commit " + topic + " to " + atLeast, () -> {
            try {
                var committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
                return committed != null && committed.offset() >= atLeast;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(60));
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
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        List<Causes> causes = new ArrayList<>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            int quietPolls = 0;
            while (System.nanoTime() < deadline && quietPolls < 4) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(250));
                quietPolls = polled.isEmpty() ? quietPolls + 1 : 0;
                polled.forEach(record -> {
                    try {
                        causes.add(CausesCodec.decode(record.headers().lastHeader(CausesCodec.HEADER_KEY).value()));
                    } catch (CausesCodec.UndecodableMetadataException e) {
                        throw new AssertionError("emitted an undecodable header", e);
                    }
                });
            }
        }
        return causes;
    }

    // ------------------------------------------------------------------ tests

    /** ASSESSMENT 1.1: delete and recreate a received topic while the process runs. The feed path subscribes by
     * name, so without the mid-run identity check the new incarnation's records are adopted under the old channel
     * (observed live: earlier records silently dropped as duplicates, later ones delivered with the dead topic's
     * id in their emissions, {@code healthy()} true throughout). The facts round must catch the recreation and
     * stop the process instead. */
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
            // Enough records that the old read position lands inside the new incarnation's log — the shape that
            // survives every consumer-level guard and must be caught by the identity check alone.
            for (int i = 0; i < 8; i++) {
                produce("rr-in", "k", "v" + i);
            }

            await("the process to stop rather than adopt the new incarnation",
                    () -> !parsley.healthy(), Duration.ofSeconds(30));
            assertTrue(delivered.stream().filter(v -> v.startsWith("m")).count() == 5,
                    "the old incarnation's deliveries stand");
            // SPEC Operational 1 (ASSESSMENT 1.10): the failure is readable programmatically. Which failure wins
            // the race varies — the identity refusal from the facts round, or the foreign fallout of the broker
            // expunging the group mid-recreation — so this asserts readability; the deliberate-refusal reading is
            // pinned deterministically by deliberateRefusalIsReadableInTheStatusSurface.
            await("the failure to be recorded in the status surface",
                    () -> parsley.status().get("rr").failureDetail().isPresent(), Duration.ofSeconds(30));
        }
    }

    /** ASSESSMENT 1.10 / SPEC Operational 1: a deliberate refusal — here the metadata budget failing closed on
     * receipt (D52) — is readable programmatically with its reason, distinguishable from a transient failure.
     * (The budget refusal is the deterministic choice: refusals tied to topic deletion race Kafka Streams' own
     * rebalance-on-metadata-change death, which usually wins with a foreign diagnosis.) */
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
                big.put(new ChannelId(xId, partition), 1L); // 5 entries = 145 bytes, over the 64-byte budget
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

    /** ASSESSMENT 1.2, first leg: a DENY-Describe ACL makes the broker answer describe-by-id with unknown-topic
     * (masking existence) while describe-by-name answers with an authorization error. Denial is not death: the
     * frontier cause must survive arbitrarily many masked rounds and still be expressed. */
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
            // Far beyond the dead-confirmation window (3 × 500 ms): every masked round must count as denial.
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

    /** ASSESSMENT 1.2, received-channel leg (the open question the fix must settle): the same masking on a
     * received topic must not drive its fed-or-never frontier to the end-of-channel sentinel — a held message
     * whose cause names positions that may still arrive must stay held through the denial, and deliver once the
     * positions really arrive. */
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

            // R waits on aclr-x@3, which does not exist yet. Keep fetching legal while describes are denied:
            // an explicit READ allow, with DENY taking precedence over the implied describe only.
            allow("aclr-x", AclOperation.READ);
            allow("aclr-x", AclOperation.WRITE);
            denyDescribe("aclr-x");
            produce("aclr-in", "k", "R", causesHeader(Map.of(xChannel, 3L)));

            Thread.sleep(4_000);
            assertFalse(delivered.contains("R"),
                    "a denial-masked describe must not settle positions that may still arrive");
            assertTrue(parsley.healthy(), "the process holds and waits; denial is not its failure");

            dropAcls("aclr-x"); // no ACLs left: allow-everyone applies again
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

    /** ASSESSMENT 1.3: stop with a held message on topic T, delete and recreate T under the same name, restart
     * with the unchanged declaration. The held entries carry the old identity, so a naive held-versus-declared
     * comparison misdiagnoses this as a declaration change; the accurate diagnosis is the identity change, with
     * D33's deliberate-reset remedy. */
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
            // R is held: its cause names md-x@5, which never arrives. Its read position still commits — that is
            // the point of hold persistence — so the held entry, under md-in's current identity, outlives the stop.
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
