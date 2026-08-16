package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the plumbing across a full broker bounce, on a dedicated cluster.
 *
 * <p>The audit verified the fail-open path is structurally unreachable — an outage cannot
 * mature into evidence, because a dead verdict requires affirmative broker answers. This
 * test is the belt and braces over the recovery plumbing itself: through a broker outage a
 * held effect stays held, and after recovery real evidence still releases it, in order,
 * exactly once, with the process healthy throughout.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class BrokerBounceIntegrationTest {
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

    @Test
    void heldMessageSurvivesABrokerBounceAndReleasesOnlyByEvidence() throws Exception {
        admin.createTopics(List.of(new NewTopic("bounce-a", 1, (short) 1), new NewTopic("bounce-b", 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
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

        UUID aTopicId = topicId("bounce-a");
        produce("bounce-b", "k", "B", new RecordHeader(CausesCodec.HEADER_KEY,
                CausesCodec.encode(Causes.of(Map.of(new ChannelId(aTopicId, 0), 0L)))));

        ParsleyConfig config = ParsleyConfig.builder(cluster.bootstrapServers(), "bounce")
                .stateDir(stateDir.resolve("bounce").toString())
                .factsInterval(Duration.ofMillis(500))
                .build();
        try (Parsley parsley = Parsley.start(config, pb)) {
            awaitFedAndHeld("bounce-pb", "bounce-b", delivered);

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
            assertEquals(List.of("A", "B"), List.copyOf(delivered),
                    "after recovery, real evidence releases the hold in causal order, exactly once");
            assertTrue(parsley.healthy(), "the process must ride out the bounce without failing");
        }
    }

    private static void awaitFedAndHeld(String groupId, String topic,
                                        ConcurrentLinkedQueue<String> delivered) {
        ClusterTestSupport.awaitFedAndHeld(admin, groupId, topic, delivered);
    }

    private static UUID topicId(String topic) throws Exception {
        return ClusterTestSupport.topicId(admin, topic);
    }

    private static void produce(String topic, String key, String value, RecordHeader... headers) {
        ClusterTestSupport.produce(cluster.bootstrapServers(), topic, key, value, headers);
    }

    private static void await(String what, BooleanSupplier condition, Duration timeout) {
        ClusterTestSupport.await(what, condition, timeout);
    }
}
