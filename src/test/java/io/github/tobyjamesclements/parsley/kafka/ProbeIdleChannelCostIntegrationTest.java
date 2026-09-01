package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounds what one facts round's probe costs against a real broker (D107). Every channel a
 * held head waits on is probed for a trailing never-yielding run; before D107 each idle
 * channel was probed alone, four polls of 250 ms each, a second per channel serialised on
 * the runtime's one facts thread. The batched probe assigns every hinted partition at once
 * and runs one poll loop, so eight idle channels resolve within it.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ProbeIdleChannelCostIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of());
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        ClusterTestSupport.stopCluster(cluster, admin);
    }

    private static long timeRound(Map<String, Object> extraProbeProps, String topic, int partitions,
                                  String group) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetAndMetadata> seeded = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            ClusterTestSupport.produce(cluster.bootstrapServers(), topic, p, "k", "v");
            seeded.put(new TopicPartition(topic, p), new OffsetAndMetadata(1L));
        }
        admin.alterConsumerGroupOffsets(group, seeded).all().get(30, TimeUnit.SECONDS);
        UUID id = ClusterTestSupport.topicId(admin, topic);

        Map<String, Object> props = new HashMap<>(extraProbeProps);
        props.put("bootstrap.servers", cluster.bootstrapServers());
        AdminFactsSource source = new AdminFactsSource(admin, group, Map.of(id, topic), props, 3_000L,
                System::currentTimeMillis);
        Set<ChannelId> received = new TreeSet<>();
        Map<ChannelId, Long> hints = new TreeMap<>();
        for (int p = 0; p < partitions; p++) {
            ChannelId channel = new ChannelId(id, p);
            received.add(channel);
            hints.put(channel, 0L); // the record at 0 was fed; nothing lies above it
        }
        try {
            source.gather(received, Map.of(), Set.of()); // warm the admin client and the probe
            long t0 = System.nanoTime();
            PositionFacts facts = source.gather(received, hints, Set.of());
            long millis = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("PROBE COST: %d idle hinted channels, props=%s: round took %d ms (facts=%s)%n",
                    partitions, extraProbeProps, millis, facts.committedNextRead());
            return millis;
        } finally {
            source.close();
        }
    }

    /** Eight idle hinted channels are probed within one poll loop, not one loop each. */
    @Test
    void idleHintedChannelsAreProbedTogetherWithinOnePollLoop() throws Exception {
        long three = timeRound(Map.of(), "probe-three", 3, "probe-three-g");
        long eight = timeRound(Map.of(), "probe-eight", 8, "probe-eight-g");
        assertTrue(three < 2_000,
                "three idle hinted channels must resolve within one poll loop; measured " + three + " ms");
        assertTrue(eight < 2_000,
                "eight idle hinted channels must cost no more than three: the probe is batched; measured "
                        + eight + " ms");
    }
}
