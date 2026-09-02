package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            // Warm the admin client and build the probe consumer: the timed round measures
            // the poll loop, not client construction.
            source.gather(received, hints, Set.of());
            long t0 = System.nanoTime();
            source.gather(received, hints, Set.of());
            return (System.nanoTime() - t0) / 1_000_000;
        } finally {
            source.close();
        }
    }

    /**
     * Eight idle hinted channels are probed within one poll loop, not one loop each. A loop
     * over channels whose hint is never exceeded costs its four short polls, about a second
     * on this broker, whatever the channel count; probed one at a time, eight channels cost
     * eight of them. The absolute bound leaves the batched cost a wide margin on a slow
     * runner while excluding the unbatched one, and the relative bound is the claim itself.
     */
    @Test
    void idleHintedChannelsAreProbedTogetherWithinOnePollLoop() throws Exception {
        long three = timeRound(Map.of(), "probe-three", 3, "probe-three-g");
        long eight = timeRound(Map.of(), "probe-eight", 8, "probe-eight-g");
        assertTrue(three < 4_000,
                "three idle hinted channels must resolve within one poll loop; measured " + three + " ms");
        assertTrue(eight < 4_000,
                "eight idle hinted channels must resolve within one poll loop; measured " + eight + " ms");
        assertTrue(eight < three + 1_000,
                "eight channels must cost no more than three plus one poll loop's slack: the probe is batched;"
                        + " measured three=" + three + " ms, eight=" + eight + " ms");
    }

    /**
     * One channel whose hint lies below its log start does not stop the others settling.
     * The probe consumer runs with {@code auto.offset.reset=none}, so such a seek makes the
     * shared poll throw; the failing partition is set aside for the round's log-start fact
     * to refuse (D104), and a channel with a trailing aborted run above its hint is still
     * settled past the run in the same round. Probed one at a time, each channel had its own
     * failure domain; batched, the isolation has to be kept deliberately.
     */
    @Test
    void aChannelWhoseHintLiesBelowTheLogStartDoesNotStopTheOthersSettling() throws Exception {
        String settling = "probe-settling";
        String truncated = "probe-truncated";
        String group = "probe-isolation-g";
        admin.createTopics(List.of(new NewTopic(settling, 1, (short) 1), new NewTopic(truncated, 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
        ClusterTestSupport.produce(cluster.bootstrapServers(), settling, 0, "k", "fed");   // offset 0
        produceAborted(settling, "ghost-1");                                               // 1 and its marker
        produceAborted(settling, "ghost-2");                                               // 3 and its marker
        ClusterTestSupport.produce(cluster.bootstrapServers(), settling, 0, "k", "next");  // 5
        for (int i = 0; i < 4; i++) {
            ClusterTestSupport.produce(cluster.bootstrapServers(), truncated, 0, "k", "v" + i);
        }
        TopicPartition truncatedPartition = new TopicPartition(truncated, 0);
        admin.deleteRecords(Map.of(truncatedPartition, RecordsToDelete.beforeOffset(3))).all()
                .get(30, TimeUnit.SECONDS);
        admin.alterConsumerGroupOffsets(group, Map.of(
                new TopicPartition(settling, 0), new OffsetAndMetadata(1L),
                truncatedPartition, new OffsetAndMetadata(1L))).all().get(30, TimeUnit.SECONDS);
        UUID settlingId = ClusterTestSupport.topicId(admin, settling);
        UUID truncatedId = ClusterTestSupport.topicId(admin, truncated);
        ChannelId settlingChannel = new ChannelId(settlingId, 0);
        ChannelId truncatedChannel = new ChannelId(truncatedId, 0);

        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", cluster.bootstrapServers());
        AdminFactsSource source = new AdminFactsSource(admin, group,
                Map.of(settlingId, settling, truncatedId, truncated), props, 3_000L, System::currentTimeMillis);
        try {
            Set<ChannelId> received = new TreeSet<>(List.of(settlingChannel, truncatedChannel));
            Map<ChannelId, Long> hints = new TreeMap<>();
            hints.put(settlingChannel, 0L);   // fed the record at 0; the aborted run lies above
            hints.put(truncatedChannel, 0L);  // fed 0, but retention has moved the log start to 3
            PositionFacts facts = source.gather(received, hints, Set.of());
            assertEquals(Long.valueOf(5L), facts.committedNextRead().get(settlingChannel),
                    "the channel with a trailing aborted run must settle past it although the other"
                            + " channel's probe failed: " + facts.committedNextRead());
            Long truncatedReport = facts.committedNextRead().get(truncatedChannel);
            assertTrue(truncatedReport == null || truncatedReport <= 1L,
                    "the truncated channel's probe must not fabricate a report: " + truncatedReport);
            assertEquals(Long.valueOf(3L), facts.logStart().get(truncatedChannel),
                    "the log-start fact carries the truncation for the engine to refuse");
        } finally {
            source.close();
        }
    }

    private static void produceAborted(String topic, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "aborter-" + UUID.randomUUID());
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, "ghost", value));
            producer.flush();
            producer.abortTransaction();
        }
    }
}
