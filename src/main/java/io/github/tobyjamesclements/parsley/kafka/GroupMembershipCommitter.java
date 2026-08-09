package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Commits group offsets through group membership, never through admin alteration. The distinction carries a safety
 * property (ASSESSMENT 1.5): {@code Admin#alterConsumerGroupOffsets} succeeds against any empty group, so a
 * bootstrap paused for an arbitrary duration (SPEC Fault model 2) can wake after a newer lifetime has bootstrapped,
 * processed and stopped, and overwrite the newer lifetime's committed offsets with stale ones. Commits made through
 * group membership are generation-fenced by the broker: this helper joins the group and performs the whole
 * read-compute-commit sequence inside that one membership, so a pause long enough for another lifetime to
 * interleave either outlasts the session (the member is fenced out and the commit throws) or has its generation
 * bumped by the newer joiner (the commit is rejected). Every pre-commit site must go through this helper — an
 * admin alter reintroduces the overwrite.
 *
 * <p>Phase-structured (join, read, commit, close) so the fencing property is testable: a test can force a
 * generation change between {@link #committed} and {@link #commit} and assert the stale commit is refused.</p>
 *
 * <p>The subscription's partitions are paused and explicitly positioned the moment they are assigned, so the
 * consumer never fetches and never consults a reset policy (it has none — D9); the only offsets it ever commits
 * are the explicitly computed map.</p>
 */
final class GroupMembershipCommitter implements AutoCloseable {

    private final KafkaConsumer<byte[], byte[]> consumer;

    GroupMembershipCommitter(Map<String, Object> clientProperties, String groupId) {
        Map<String, Object> props = new HashMap<>(clientProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        // A crashed or paused bootstrap must not hold up a newer lifetime's join for the consumer defaults'
        // five minutes: this membership does seconds of work, so it may time out in seconds (Operational 2).
        props.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30_000);
        this.consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /** Join the group and wait for an assignment. A protocol-incompatible group — Kafka Streams members present,
     * either a closed application's members waiting out their session or a live one — fast-fails the join; it is
     * retried within the deadline. The deadline must outlast the lingerers: a closed application's members hold
     * the group for their own session timeout (the consumer default is 45 s, and parsley does not lower it for
     * the Streams consumers), so callers pass a deadline comfortably above that; a genuinely live application
     * keeps refusing until the deadline expires and the bootstrap fails with that diagnosis attached. */
    void join(Set<String> topics, Duration timeout) {
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumer.pause(partitions); // membership is the point; fetching is not
                for (TopicPartition partition : partitions) {
                    // Give every assigned partition an explicit in-memory position so the poll loop never
                    // consults a reset policy (there is none — D9). The position is never fetched from (paused)
                    // and never committed (only the explicitly computed map is), so its value is irrelevant.
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
            if (System.nanoTime() - deadline > 0) { // by difference: nanoTime's absolute values are meaningless
                throw new IllegalStateException("no assignment from group coordinator within " + timeout
                        + (lastProtocolConflict == null
                                ? ""
                                : " — the group is held by protocol-incompatible members: a Kafka Streams"
                                        + " lifetime of this process is running, or a closed one's members have"
                                        + " not yet timed out"),
                        lastProtocolConflict);
            }
            try {
                consumer.poll(Duration.ofMillis(100));
            } catch (org.apache.kafka.common.errors.InconsistentGroupProtocolException e) {
                lastProtocolConflict = e;
                try {
                    Thread.sleep(500); // Streams members linger after close without leaving; wait them out
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while joining the group", interrupted);
                }
            }
        }
    }

    /** The group's committed offsets for these partitions, read through the membership session. */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return consumer.committed(partitions);
    }

    /** Commit explicit offsets. Throws if this member's generation is no longer current — a newer lifetime
     * interleaved — in which case nothing was written. */
    void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    @Override
    public void close() {
        consumer.close(); // leaves the group; the coordinator reverts to empty for the Streams app to join
    }
}
