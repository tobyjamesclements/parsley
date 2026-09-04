package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The cluster-facing helpers the integration suites share. One copy, so a tightening of
 * the held-premise check or the wait discipline reaches every suite that asserts it.
 */
final class ClusterTestSupport {

    /**
     * One producer per cluster, so a produce call does not pay a fresh metadata fetch
     * (bounded by {@code max.block.ms}) per record. Closed by {@link #stopCluster}, keyed
     * by bootstrap servers because every embedded cluster binds unique ports.
     */
    private static final ConcurrentHashMap<String, KafkaProducer<String, String>> PRODUCERS =
            new ConcurrentHashMap<>();

    private ClusterTestSupport() {
    }

    /**
     * Starts an embedded single-node KRaft cluster with the suites' shared base
     * configuration, formatted and ready.
     *
     * @param extraConfigProps suite-specific broker properties layered over the base
     * @return the running cluster
     */
    static KafkaClusterTestKit startCluster(Map<String, String> extraConfigProps) throws Exception {
        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setCombined(true)
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .setConfigProp("group.initial.rebalance.delay.ms", "0");
        extraConfigProps.forEach(builder::setConfigProp);
        KafkaClusterTestKit cluster = builder.build();
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (Exception e) {
            // The caller's field is never assigned on failure, so its @AfterAll cannot
            // release a half-started broker; close it here or its non-daemon threads and
            // bound ports outlive this suite into the rest of the surefire fork.
            cluster.close();
            throw e;
        }
        return cluster;
    }

    /**
     * Releases a suite's cluster resources: the shared producer for the cluster, the admin
     * client, then the cluster itself. Null-tolerant so a failed startup can still clean
     * up, and each release is isolated so one failure cannot skip the rest — a skipped
     * {@code cluster.close()} would leak the broker's non-daemon threads and bound ports
     * into the rest of the surefire fork. The first failure is rethrown after every
     * release was attempted, with later ones suppressed.
     */
    static void stopCluster(KafkaClusterTestKit cluster, Admin admin) throws Exception {
        Exception failure = null;
        if (cluster != null) {
            KafkaProducer<String, String> producer = PRODUCERS.remove(cluster.bootstrapServers());
            if (producer != null) {
                try {
                    producer.close(Duration.ofSeconds(10));
                } catch (Exception e) {
                    failure = e;
                }
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Produces one record and waits for the broker's acknowledgement, so a failed send
     * surfaces here as the real error instead of as an unrelated await timing out a minute
     * later — {@code flush()} does not rethrow per-record failures.
     */
    static void produce(String bootstrapServers, String topic, String key, String value,
                        RecordHeader... headers) {
        produce(bootstrapServers, topic, null, key, value, headers);
    }

    /**
     * Produces one record to a chosen partition ({@code null} for the default partitioner)
     * and waits for the broker's acknowledgement.
     */
    static void produce(String bootstrapServers, String topic, Integer partition, String key,
                        String value, RecordHeader... headers) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value);
        for (RecordHeader header : headers) {
            record.headers().add(header);
        }
        try {
            producer(bootstrapServers).send(record).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted producing to " + topic, e);
        } catch (Exception e) {
            throw new AssertionError("produce to " + topic + " failed", e);
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        return PRODUCERS.computeIfAbsent(bootstrapServers, servers -> {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        });
    }

    /**
     * Drains every committed record of a topic with a fresh read_committed consumer: from
     * the earliest offset, under a 10-second deadline, stopping after four consecutive
     * empty 250ms polls — one copy of the drain discipline, so a tightening reaches every
     * suite that asserts committed output.
     */
    static List<ConsumerRecord<String, String>> readAllCommitted(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            int quietPolls = 0;
            while (System.nanoTime() < deadline && quietPolls < 4) {
                var polled = consumer.poll(Duration.ofMillis(250));
                quietPolls = polled.isEmpty() ? quietPolls + 1 : 0;
                polled.forEach(records::add);
            }
        }
        return records;
    }

    static UUID topicId(Admin admin, String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        return new UUID(description.topicId().getMostSignificantBits(),
                description.topicId().getLeastSignificantBits());
    }

    /**
     * Polls until the condition holds. The condition is sampled before the deadline is
     * consulted, so expiry during a sleep still gets a final sample; the interruption cause
     * rides the error ({@code trimStackTrace=false} keeps it visible); and the 200ms
     * interval keeps admin-backed conditions from doubling their RPC load at a broker that
     * is bouncing.
     */
    static void await(String what, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out awaiting " + what);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting " + what, e);
            }
        }
    }

    /** Polls until {@code groupId}'s committed position on partition 0 of {@code topic} reaches {@code atLeast}. */
    static void awaitCommitted(Admin admin, String groupId, String topic, long atLeast) {
        await("group " + groupId + " to commit " + topic + " to " + atLeast, () -> {
            try {
                var committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
                return committed != null && committed.offset() >= atLeast;
            } catch (Exception e) {
                return false; // transient admin failure: not evidence either way, poll again
            }
        }, Duration.ofSeconds(60));
    }

    /**
     * Establishes the held premise by evidence rather than elapsed time: the process's
     * committed read position on the effect's topic reaches past it — so it was fed and its
     * step committed — while nothing has been delivered. A fixed sleep would let a slow
     * runner pass the emptiness assertion without the effect ever having been fed.
     */
    static void awaitFedAndHeld(Admin admin, String groupId, String topic,
                                ConcurrentLinkedQueue<String> delivered) {
        awaitCommitted(admin, groupId, topic, 1);
        assertEquals(List.of(), List.copyOf(delivered), "the effect must be held while its cause is missing");
    }

    /**
     * The same premise, by the public status surface rather than the group's committed
     * offset: the process reports a message held on {@code topic}. Host-neutral, since a
     * host that commits its read position at the hold-back head (D114) never advances the
     * committed offset past a held message.
     */
    static void awaitFedAndHeld(io.github.tobyjamesclements.parsley.api.Parsley parsley, String process,
                                String topic, ConcurrentLinkedQueue<String> delivered) {
        await("process " + process + " to report a hold on " + topic, () -> {
            var status = parsley.status().get(process);
            return status != null && status.tasks().stream().anyMatch(task ->
                    task.heldChannels().stream().anyMatch(held -> held.topic().equals(topic)));
        }, Duration.ofSeconds(60));
        assertEquals(List.of(), List.copyOf(delivered), "the effect must be held while its cause is missing");
    }
}
