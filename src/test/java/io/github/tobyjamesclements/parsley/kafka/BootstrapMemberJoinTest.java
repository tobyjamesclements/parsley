package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes how the bootstrap member's join ends when no assignment ever arrives: at the
 * deadline with the coordinator diagnosis, with the protocol-conflict explanation when the
 * group is owned, or as a diagnosed interrupt — never as a silent hang, and never with an
 * unusable inherited session timeout reaching the broker at all.
 *
 * <p>The join wait is the one place a contested group is discovered (D48: a live Kafka
 * Streams lifetime makes every poll throw the protocol conflict), so its deadline is what
 * turns "another lifetime owns this group" into a diagnosed bootstrap failure instead of an
 * infinite hang (Operational 2). Each waiting test is bounded twice: the class timeout, and
 * a poll budget inside the consumer double that turns an endless loop into an assertion
 * failure rather than a hang.
 */
@Timeout(value = 30)
class BootstrapMemberJoinTest {

    /**
     * The inherited-session-timeout floor: a resolved value below one millisecond can never
     * be a usable session timeout, and it must be refused during property composition —
     * before any network contact, naming the property and the value — rather than stand up
     * a consumer whose join the broker then rejects with its own vocabulary (D48; D87's
     * attributable-refusal rule for this property).
     */
    @Test
    void subMillisecondInheritedSessionTimeoutIsRefusedBeforeAnyNetworkContact() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> GroupMembershipCommitter.memberProperties(
                        Map.of("bootstrap.servers", "b:9092",
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 0), "g"),
                "a sub-millisecond session timeout must be refused at property composition");
        assertTrue(e.getMessage().contains("session.timeout.ms value 0 is not a usable timeout"),
                "the refusal names the property and the unusable value: " + e.getMessage());
    }

    /**
     * The join deadline itself: a coordinator that never assigns must fail the bootstrap
     * loudly within the deadline, naming the timeout it waited (D48, Operational 2). This
     * is the site whose deletion turns a diagnosed bootstrap failure into an infinite
     * hang — the consumer double's poll budget and the class timeout convert that hang
     * into a visible failure.
     */
    @Test
    void joinWithoutAnAssignmentFailsAtTheDeadlineNamingTheTimeout() {
        UnassignedConsumer consumer = new UnassignedConsumer();

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> GroupMembershipCommitter.awaitAssignment(consumer, Duration.ofMillis(200)),
                "a join the coordinator never answers must end at the deadline, not hang");
        assertTrue(e.getMessage().contains("no assignment from group coordinator within PT0.2S"),
                "the failure names the deadline it waited: " + e.getMessage());
    }

    /**
     * The deadline's diagnosis when the group is owned: a live Kafka Streams lifetime (or
     * a closed one's members not yet timed out) makes every poll throw the protocol
     * conflict, and the deadline must surface that as the likely-holders explanation with
     * the conflict as cause — a bare timeout would send the operator to the network when
     * the group is simply held (D48's join-fast-fail contract; D86 records the deadline
     * this diagnosis reaches operators through).
     */
    @Test
    void protocolConflictAtTheDeadlineNamesTheLikelyHoldersAndCarriesTheCause() {
        UnassignedConsumer consumer = new UnassignedConsumer();
        InconsistentGroupProtocolException conflict =
                new InconsistentGroupProtocolException("streams member holds the group");
        consumer.pollThrows = conflict;

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> GroupMembershipCommitter.awaitAssignment(consumer, Duration.ofMillis(200)),
                "a join grinding on the protocol conflict must still end at the deadline");
        assertTrue(e.getMessage().contains("held by protocol-incompatible members"),
                "the deadline explains the conflict shape it saw: " + e.getMessage());
        assertSame(conflict, e.getCause(),
                "the last protocol conflict rides as the cause for the operator");
    }

    /**
     * An interrupt during the protocol-conflict backoff: a supervisor tearing down a
     * contested bootstrap must end the join as its own diagnosed failure with the
     * interrupt status restored — not have the interrupt swallowed into further backoff
     * cycles until the deadline dresses the stop as a coordinator timeout (D48).
     */
    @Test
    void interruptDuringTheProtocolConflictBackoffFailsTheJoinAsInterrupted() {
        UnassignedConsumer consumer = new UnassignedConsumer();
        consumer.pollThrows = new InconsistentGroupProtocolException("streams member holds the group");

        Thread.currentThread().interrupt();
        IllegalStateException e;
        boolean interrupted;
        try {
            e = assertThrows(IllegalStateException.class,
                    () -> GroupMembershipCommitter.awaitAssignment(consumer, Duration.ofSeconds(2)),
                    "an interrupt in the backoff must fail the join, not be swallowed");
        } finally {
            // read-and-clear so a failure above cannot leak interrupt status into later tests
            interrupted = Thread.interrupted();
        }
        assertTrue(e.getMessage().contains("interrupted while joining the group"),
                "the failure names the interrupt, not the deadline: " + e.getMessage());
        assertInstanceOf(InterruptedException.class, e.getCause(),
                "the interrupt rides as the cause");
        assertTrue(interrupted, "the interrupt status must be restored for the caller");
    }

    /**
     * A hand-rolled consumer double for the join wait: never assigned, every poll empty —
     * or throwing the scripted protocol conflict. Implements only what
     * {@code awaitAssignment} uses — {@code assignment} and {@code poll} — and refuses
     * everything else, so the loop growing a new dependency fails loudly here. An empty
     * poll naps briefly, as the real consumer's poll timeout would, and a poll budget
     * turns an endless loop into an assertion failure rather than a hang.
     */
    private static final class UnassignedConsumer implements Consumer<byte[], byte[]> {
        volatile RuntimeException pollThrows;
        private int polls;

        @Override
        public Set<TopicPartition> assignment() {
            return Set.of();
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            if (++polls > 5_000) {
                throw new AssertionError("the join loop spent " + polls + " polls with no"
                        + " assignment ever arriving; the deadline that should have failed it"
                        + " loudly is gone");
            }
            if (pollThrows != null) {
                throw pollThrows;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new org.apache.kafka.common.errors.InterruptException(e);
            }
            return new ConsumerRecords<>(Map.of(), Map.of());
        }

        private static UnsupportedOperationException notUsed() {
            return new UnsupportedOperationException("not used by awaitAssignment");
        }

        @Override
        public Set<String> subscription() {
            throw notUsed();
        }

        @Override
        public void subscribe(Collection<String> topics) {
            throw notUsed();
        }

        @Override
        public void subscribe(Collection<String> topics,
                              org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            throw notUsed();
        }

        @Override
        public void assign(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public void subscribe(java.util.regex.Pattern pattern,
                              org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            throw notUsed();
        }

        @Override
        public void subscribe(java.util.regex.Pattern pattern) {
            throw notUsed();
        }

        @Override
        public void subscribe(org.apache.kafka.clients.consumer.SubscriptionPattern pattern,
                              org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            throw notUsed();
        }

        @Override
        public void subscribe(org.apache.kafka.clients.consumer.SubscriptionPattern pattern) {
            throw notUsed();
        }

        @Override
        public void unsubscribe() {
            throw notUsed();
        }

        @Override
        public void commitSync() {
            throw notUsed();
        }

        @Override
        public void commitSync(Duration timeout) {
            throw notUsed();
        }

        @Override
        public void commitSync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
            throw notUsed();
        }

        @Override
        public void commitSync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets,
                               Duration timeout) {
            throw notUsed();
        }

        @Override
        public void commitAsync() {
            throw notUsed();
        }

        @Override
        public void commitAsync(org.apache.kafka.clients.consumer.OffsetCommitCallback callback) {
            throw notUsed();
        }

        @Override
        public void commitAsync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets,
                                org.apache.kafka.clients.consumer.OffsetCommitCallback callback) {
            throw notUsed();
        }

        @Override
        public void registerMetricForSubscription(org.apache.kafka.common.metrics.KafkaMetric metric) {
            throw notUsed();
        }

        @Override
        public void unregisterMetricFromSubscription(org.apache.kafka.common.metrics.KafkaMetric metric) {
            throw notUsed();
        }

        @Override
        public void seek(TopicPartition partition, long offset) {
            throw notUsed();
        }

        @Override
        public void seek(TopicPartition partition,
                         org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata) {
            throw notUsed();
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public void seekToEnd(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public long position(TopicPartition partition) {
            throw notUsed();
        }

        @Override
        public long position(TopicPartition partition, Duration timeout) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed(
                Set<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed(
                Set<TopicPartition> partitions, Duration timeout) {
            throw notUsed();
        }

        @Override
        public org.apache.kafka.common.Uuid clientInstanceId(Duration timeout) {
            throw notUsed();
        }

        @Override
        public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
            throw notUsed();
        }

        @Override
        public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic) {
            throw notUsed();
        }

        @Override
        public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic, Duration timeout) {
            throw notUsed();
        }

        @Override
        public Map<String, List<org.apache.kafka.common.PartitionInfo>> listTopics() {
            throw notUsed();
        }

        @Override
        public Map<String, List<org.apache.kafka.common.PartitionInfo>> listTopics(Duration timeout) {
            throw notUsed();
        }

        @Override
        public void pause(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public Set<TopicPartition> paused() {
            throw notUsed();
        }

        @Override
        public void resume(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            throw notUsed();
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            throw notUsed();
        }

        @Override
        public java.util.OptionalLong currentLag(TopicPartition topicPartition) {
            throw notUsed();
        }

        @Override
        public org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata() {
            throw notUsed();
        }

        @Override
        public void enforceRebalance() {
            throw notUsed();
        }

        @Override
        public void enforceRebalance(String reason) {
            throw notUsed();
        }

        @Override
        public void close() {
            throw notUsed();
        }

        @Override
        public void close(Duration timeout) {
            throw notUsed();
        }

        @Override
        public void close(org.apache.kafka.clients.consumer.CloseOptions option) {
            throw notUsed();
        }

        @Override
        public void wakeup() {
            throw notUsed();
        }
    }
}
