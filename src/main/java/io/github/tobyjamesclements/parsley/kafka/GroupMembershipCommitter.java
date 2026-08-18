package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A consumer that joins a group to read and write committed positions without consuming.
 *
 * <p>Kafka Streams commits positions through its own consumer, which is not reachable from a
 * processor. Reading a group's committed positions therefore needs a separate member of that
 * group. Assigned partitions are paused on assignment, so this member never fetches a record.
 */
final class GroupMembershipCommitter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GroupMembershipCommitter.class);

    private final KafkaConsumer<byte[], byte[]> consumer;

    /**
     * @param clientProperties base client properties, including bootstrap servers
     * @param groupId          the group to join
     */
    GroupMembershipCommitter(Map<String, Object> clientProperties, String groupId) {
        Map<String, Object> props = new HashMap<>(clientProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        // read_committed makes committed() a stable-offset fetch: a pending transactional
        // commit from another lifetime is never read as absent-or-old while its transaction
        // is still deciding.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // This member must vacate the group the moment it closes — the Streams start that
        // follows joins the same group under a different protocol. A static member sends no
        // LeaveGroup on close, so an inherited instance id would hold the group for the full
        // session timeout; membership here is always dynamic, and the graceful close's
        // LeaveGroup is what vacates the group. The session timeout matters only for an
        // ungraceful exit: it defaults short, but an explicitly configured value is kept,
        // because brokers may enforce a minimum above the default.
        if (props.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) != null) {
            LOG.warn("{}: ignoring configured group.instance.id for the bootstrap member; static membership"
                    + " would hold the group past close and fail the Streams start that follows", groupId);
        }
        Object sessionTimeout = null;
        for (String key : new String[] {
                org.apache.kafka.streams.StreamsConfig.mainConsumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                org.apache.kafka.streams.StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG}) {
            if (sessionTimeout == null) {
                sessionTimeout = props.get(key);
            }
        }
        if (sessionTimeout == null) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        } else {
            // Resolved across the Streams spellings too: a broker may enforce a minimum
            // above the default, and a timeout configured the idiomatic prefixed way must
            // reach this plain consumer or the join is rejected outright.
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                    Integer.parseInt(String.valueOf(sessionTimeout)));
            LOG.info("{}: bootstrap member inherits session.timeout.ms={}; an ungraceful bootstrap exit"
                    + " holds the group for that long", groupId, sessionTimeout);
        }
        props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30_000);
        this.consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Joins the group and waits for an assignment.
     *
     * @param topics  the topics to subscribe to
     * @param timeout how long to wait for the assignment
     */
    void join(Set<String> topics, Duration timeout) {
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumer.pause(partitions);
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, 0);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }
        });
        long deadline = System.nanoTime() + timeout.toNanos();
        org.apache.kafka.common.errors.InconsistentGroupProtocolException lastProtocolConflict = null;
        while (consumer.assignment().isEmpty()) {
            if (System.nanoTime() - deadline > 0) {
                throw new IllegalStateException("no assignment from group coordinator within " + timeout
                        + (lastProtocolConflict == null
                                ? ""
                                : ": the group is held by protocol-incompatible members, a Kafka Streams"
                                        + " lifetime of this process is running, or a closed one's members have"
                                        + " not yet timed out"),
                        lastProtocolConflict);
            }
            try {
                consumer.poll(Duration.ofMillis(100));
            } catch (org.apache.kafka.common.errors.InconsistentGroupProtocolException e) {
                lastProtocolConflict = e;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while joining the group", interrupted);
                }
            }
        }
    }

    /**
     * @param partitions the partitions to query
     * @return the group's committed position per partition, absent entries meaning none
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return consumer.committed(partitions);
    }

    /**
     * @param offsets the positions to commit for this group
     */
    void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    /** Leaves the group and closes the consumer. */
    @Override
    public void close() {
        consumer.close();
    }
}
