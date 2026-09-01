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

    /**
     * The client id every bootstrap member carries, so a starting instance can see another
     * instance's member in the group description and wait for it to leave before its own
     * Kafka Streams joins (D108).
     */
    static final String CLIENT_ID_PREFIX = "parsley-bootstrap-";

    private final KafkaConsumer<byte[], byte[]> consumer;

    /**
     * @param clientProperties base client properties, including bootstrap servers
     * @param groupId          the group to join
     */
    GroupMembershipCommitter(Map<String, Object> clientProperties, String groupId) {
        this.consumer = new KafkaConsumer<>(memberProperties(clientProperties, groupId),
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /** Client properties for the bootstrap member, with the guarantee-bearing pins applied. */
    static Map<String, Object> memberProperties(Map<String, Object> clientProperties, String groupId) {
        Map<String, Object> props = new HashMap<>(clientProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + java.util.UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        // The member's subscription must never create a missing received topic: every
        // received topic was resolved by start() moments before this join, so a deletion
        // racing the bootstrap must surface as this join's failure, not be papered over by
        // the metadata request auto-creating an empty impostor (D82).
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        // committed() is a transaction-stable offset fetch regardless of configuration:
        // the consumer sets requireStable on every OffsetFetch it sends (verified in
        // kafka-clients 4.3.1, ConsumerCoordinator#sendOffsetFetchRequest), retrying
        // while a pending transactional commit is deciding. Nothing here needs an
        // isolation.level — this member never fetches a record.

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
        java.util.OptionalLong sessionTimeout = configuredSessionTimeoutMillis(props);
        if (sessionTimeout.isEmpty()) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        } else {
            // Resolved across the Streams spellings too: a broker may enforce a minimum
            // above the default, and a timeout configured the idiomatic prefixed way must
            // reach this plain consumer or the join is rejected outright.
            long millis = sessionTimeout.getAsLong();
            if (millis < 1) {
                throw new IllegalArgumentException("session.timeout.ms value " + millis
                        + " is not a usable timeout");
            }
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) millis);
            LOG.info("{}: bootstrap member inherits session.timeout.ms={}; an ungraceful bootstrap exit"
                    + " holds the group for that long", groupId, millis);
        }
        props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30_000);
        return props;
    }

    /**
     * Resolves a configured session timeout across the Streams spellings, accepting
     * exactly what Kafka's own config parser accepts for an INT config — an Integer, or a
     * trimmed string holding one — so a value every other client in this process runs on
     * cannot fail only inside the bootstrap, and a value the clients would reject
     * post-bootstrap fails here, first and naming its property (D87, tightened by D88: a
     * laxer parse would let the bootstrap succeed on a value StreamsConfig then rejects).
     */
    static java.util.OptionalLong configuredSessionTimeoutMillis(Map<String, Object> props) {
        for (String key : new String[] {
                org.apache.kafka.streams.StreamsConfig.mainConsumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                org.apache.kafka.streams.StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG}) {
            Object value = props.get(key);
            if (value == null) {
                continue;
            }
            if (value instanceof Integer integer) {
                return java.util.OptionalLong.of(integer);
            }
            if (value instanceof Number) {
                throw new IllegalArgumentException(key + " value " + value + " must be a 32-bit integer,"
                        + " as Kafka's own config parser requires");
            }
            try {
                return java.util.OptionalLong.of(Integer.parseInt(String.valueOf(value).trim()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(key + " value '" + value + "' is not a parsable 32-bit"
                        + " integer", e);
            }
        }
        return java.util.OptionalLong.empty();
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
        awaitAssignment(consumer, timeout, CONFLICT_BACKOFF);
    }

    /** How long a poll that hit the protocol conflict backs off before asking again (D48). */
    private static final Duration CONFLICT_BACKOFF = Duration.ofMillis(500);

    /**
     * The join wait, extracted so the deadline and its diagnoses are pinnable over a
     * consumer double: polls until the coordinator assigns, backing off on the protocol
     * conflict a live Kafka Streams member provokes (D48), and ends loudly — at the
     * deadline, naming the conflict when one was seen, or on an interrupt during the
     * backoff. This deadline is what turns a contested or unreachable group into a
     * diagnosed bootstrap failure instead of an infinite hang (Operational 2).
     *
     * @param consumer        a subscribed consumer whose assignment is awaited
     * @param timeout         how long to wait for the assignment
     * @param conflictBackoff how long to back off after a protocol-conflict poll —
     *                        {@link #CONFLICT_BACKOFF} in production, tiny in tests so
     *                        the conflict-path pins do not spend real wall-clock time
     */
    static void awaitAssignment(org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer,
                                Duration timeout, Duration conflictBackoff) {
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
                    Thread.sleep(conflictBackoff.toMillis());
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
