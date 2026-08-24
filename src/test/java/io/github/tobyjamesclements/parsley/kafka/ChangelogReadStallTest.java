package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes how the bootstrap's ordering-changelog read ends: at every partition's
 * snapshot end, or loudly.
 *
 * <p>The read loop drains the changelog up to an end-offset snapshot taken at the log's
 * true (read-uncommitted) end. Three behaviours carry the design (D79): no progress for
 * the stall deadline fails the start loudly instead of blocking it indefinitely
 * (Operational 2); a partition that reached its snapshot end is paused, because a live
 * writer's post-snapshot records would otherwise keep resetting the shared stall deadline
 * forever while another partition sat pinned below its end — turning the promised loud
 * stall into an indefinite hang; and the snapshot itself asks for READ_UNCOMMITTED where
 * every sibling listing asks for the committed view, so an orphaned open transaction
 * cannot silently hide committed tail records from the checks the view feeds.
 *
 * <p>Before the loop ever runs, the reader's own metadata answer is corroborated against
 * the describe the read was keyed on: with auto-create pinned off, a lagging broker
 * answers an empty partition list immediately, and trusting it would flip prior state off
 * one stale view (D88). That comparison is pinned here too.
 *
 * <p>Each test is bounded twice: a JUnit timeout, and a poll budget inside the consumer
 * double that turns an endless loop into an assertion failure rather than a hang.
 */
@Timeout(value = 10)
class ChangelogReadStallTest {
    private static final String CHANGELOG = "app-shipper-__parsley.ordering-changelog";
    private static final TopicPartition P0 = new TopicPartition(CHANGELOG, 0);
    private static final TopicPartition P1 = new TopicPartition(CHANGELOG, 1);
    private static final TopicPartition P2 = new TopicPartition(CHANGELOG, 2);

    /**
     * Catches the stall deadline being dropped: a partition whose snapshot end never
     * arrives — retention advanced, a broker stopped answering — must fail the read with
     * the no-progress diagnosis within the deadline, not block the start forever
     * (Operational 2, D79).
     */
    @Test
    void aPartitionThatNeverProgressesFailsTheReadLoudly() {
        ScriptedConsumer consumer = new ScriptedConsumer(List.of(P0));

        IllegalStateException stall = assertThrows(IllegalStateException.class,
                () -> ParsleyRuntime.readToEnds(consumer, CHANGELOG, List.of(P0),
                        Map.of(P0, 5L), Duration.ofMillis(300)),
                "a partition pinned below its snapshot end with no records arriving must fail"
                        + " the read within the stall deadline, not block the start forever");
        assertTrue(stall.getMessage().contains("no progress reading " + CHANGELOG),
                "the stall must name the changelog it could not finish reading: " + stall.getMessage());
    }

    /**
     * Catches the per-partition pause being dropped: p0 reaches its snapshot end while p1
     * sits below its end, and a live writer keeps appending post-snapshot records to p0.
     * Paused at its end, p0 stops feeding the loop and p1's starvation surfaces as the
     * loud stall; unpaused, every post-snapshot p0 record resets the stall deadline and
     * the promised loud stall becomes an indefinite hang (D79) — which the poll budget
     * and the JUnit timeout convert into a visible failure.
     */
    @Test
    void aPartitionAtItsSnapshotEndIsPausedSoAPinnedSiblingStallsLoudly() {
        ScriptedConsumer consumer = new ScriptedConsumer(List.of(P0, P1));
        consumer.append(P0, "key", "value");
        consumer.neverStopsAppending(P0);

        IllegalStateException stall = assertThrows(IllegalStateException.class,
                () -> ParsleyRuntime.readToEnds(consumer, CHANGELOG, List.of(P0, P1),
                        Map.of(P0, 1L, P1, 5L), Duration.ofMillis(300)),
                "with the finished partition paused, the pinned sibling must surface as the"
                        + " loud stall, not as an endless loop fed by post-snapshot records");
        assertTrue(stall.getMessage().contains("no progress reading"),
                "the pinned sibling's starvation must carry the no-progress diagnosis: "
                        + stall.getMessage());
        assertEquals((Long) 1L, consumer.pausedAtPosition.get(P0),
                "p0 must be paused exactly when it reached its snapshot end, so post-snapshot"
                        + " records cannot keep resetting the stall deadline");
        assertFalse(consumer.pausedAtPosition.containsKey(P1),
                "p1 below its snapshot end must never be paused; its records are the ones"
                        + " the read is waiting for");
    }

    /**
     * Catches the end-offset snapshot's isolation regressing to the read-committed
     * default: the snapshot must bound the scan at the log's true end, because the last
     * stable offset would silently hide committed tail records sitting above a superseded
     * execution's open transaction (D79; the sibling listOffsets in commitInitialPositions
     * deliberately asks for the committed view, so a shared default cannot pin this).
     */
    @Test
    void theEndOffsetSnapshotAsksForTheUncommittedEnd() {
        assertEquals(IsolationLevel.READ_UNCOMMITTED,
                ParsleyRuntime.changelogEndOffsetIsolation().isolationLevel(),
                "the end-offset snapshot must ask for the log's true end; the read-committed"
                        + " last stable offset would truncate the restored view below an open"
                        + " transaction's committed tail (D79)");
    }

    /**
     * Catches the completed-read path breaking: every partition reaching its snapshot end
     * must end the loop and return the view — the latest value per key, and exactly the
     * partitions that held records (the per-partition prior-state evidence D88 keys on).
     */
    @Test
    void reachingEverySnapshotEndReturnsTheCompactedView() {
        ScriptedConsumer consumer = new ScriptedConsumer(List.of(P0, P1, P2));
        consumer.append(P0, "task0", "superseded");
        consumer.append(P0, "task0", "latest");
        consumer.append(P1, "task1", "only");

        ParsleyRuntime.ChangelogView view = ParsleyRuntime.readToEnds(consumer, CHANGELOG,
                List.of(P0, P1, P2), Map.of(P0, 2L, P1, 1L, P2, 0L), Duration.ofSeconds(5));

        assertArrayEquals(bytes("latest"), view.latest().get(bytes("task0")),
                "the view must compact to the latest value per key, as changelog restoration would");
        assertArrayEquals(bytes("only"), view.latest().get(bytes("task1")),
                "a key written once must survive the read");
        assertEquals(Set.of(0, 1), view.partitionsWithRecords(),
                "exactly the partitions that held records must be reported; an empty partition"
                        + " reporting records would hide the per-partition loss shape (D88)");
    }

    /**
     * Catches the width-corroboration guard being deleted (SAFETY): a reader whose
     * metadata answers fewer partitions than the changelog was described with is reading
     * a lagging broker's view — with auto-create pinned off, an empty answer arrives
     * immediately, no retry, no timeout — and scanning it vacuously would flip prior
     * state off one stale view, the exact single-answer trust D84 removed from the
     * describe path (D88). The refusal must be the retryable transient naming both
     * counts, never a terminal diagnosis with a destructive remedy.
     */
    @Test
    void aLaggingReaderMetadataAnswerRefusesTheStartAsRetryable() {
        ParsleyRuntime.RetryableStartException lag =
                assertThrows(ParsleyRuntime.RetryableStartException.class,
                        () -> ParsleyRuntime.requireCorroboratedWidth("app-shipper", 3, 0),
                        "a reader answering fewer partitions than described must refuse the start,"
                                + " not scan the lagging view vacuously");
        assertTrue(lag.getMessage().contains("described with 3 partition(s)"),
                "the diagnosis must carry the described count the reader failed to corroborate: "
                        + lag.getMessage());
        assertTrue(lag.getMessage().contains("metadata answered 0"),
                "the diagnosis must carry the reader's contradicting answer: " + lag.getMessage());
        assertTrue(lag.getMessage().contains("Retry this start."),
                "the printed remedy must be a retry; a lagging broker needs a moment, not a reset: "
                        + lag.getMessage());
    }

    /**
     * Catches the corroboration over-reaching: a reader whose metadata agrees with the
     * describe is the healthy path, and the guard must pass it silently — a spurious
     * refusal here would turn every start into a retry loop.
     */
    @Test
    void anAgreeingReaderMetadataAnswerPassesCorroboration() {
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                () -> ParsleyRuntime.requireCorroboratedWidth("app-shipper", 3, 3),
                "an agreeing metadata answer is corroboration; the guard must not refuse it");
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A hand-rolled consumer double for the read loop: an assigned, rewound consumer over
     * scripted per-partition logs. Implements only what {@code readToEnds} uses —
     * {@code poll}, {@code position}, {@code pause}, {@code paused} — and refuses
     * everything else, so the loop growing a new dependency fails loudly here. An empty
     * poll naps briefly, as the real consumer's poll timeout would, and a poll budget
     * turns an endless loop into an assertion failure rather than a hang.
     */
    private static final class ScriptedConsumer implements Consumer<byte[], byte[]> {
        private final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> logs = new HashMap<>();
        private final Map<TopicPartition, Long> positions = new HashMap<>();
        private final Set<TopicPartition> endless = new HashSet<>();
        private final Set<TopicPartition> paused = new HashSet<>();
        final Map<TopicPartition, Long> pausedAtPosition = new HashMap<>();
        private int polls;

        ScriptedConsumer(Collection<TopicPartition> parts) {
            parts.forEach(tp -> {
                positions.put(tp, 0L);
                logs.put(tp, new ArrayList<>());
            });
        }

        /** Appends one record at the partition's next offset. */
        void append(TopicPartition tp, String key, String value) {
            List<ConsumerRecord<byte[], byte[]>> log = logs.get(tp);
            log.add(new ConsumerRecord<>(tp.topic(), tp.partition(), log.size(),
                    bytes(key), bytes(value)));
        }

        /**
         * Marks the partition as having a live writer: whenever it is polled unpaused with
         * its scripted log drained, it yields a fresh record at the next offset — the
         * post-snapshot stream that must not keep resetting the stall deadline.
         */
        void neverStopsAppending(TopicPartition tp) {
            endless.add(tp);
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            if (++polls > 100_000) {
                throw new AssertionError("the read loop spent " + polls + " polls without"
                        + " finishing or stalling; post-snapshot records are being consumed"
                        + " forever instead of the finished partition being paused");
            }
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> batch = new HashMap<>();
            positions.forEach((tp, position) -> {
                if (paused.contains(tp)) {
                    return;
                }
                if (endless.contains(tp)) {
                    while (logs.get(tp).size() <= position) {
                        append(tp, "post-snapshot", "post-snapshot");
                    }
                }
                if (position < logs.get(tp).size()) {
                    batch.put(tp, List.of(logs.get(tp).get(position.intValue())));
                }
            });
            batch.forEach((tp, records) -> positions.put(tp, records.get(records.size() - 1).offset() + 1));
            if (batch.isEmpty()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted while napping on an empty poll", e);
                }
            }
            return new ConsumerRecords<>(batch, Map.of());
        }

        @Override
        public long position(TopicPartition partition) {
            return positions.get(partition);
        }

        @Override
        public void pause(Collection<TopicPartition> partitions) {
            for (TopicPartition tp : partitions) {
                paused.add(tp);
                pausedAtPosition.putIfAbsent(tp, positions.get(tp));
            }
        }

        @Override
        public Set<TopicPartition> paused() {
            return Set.copyOf(paused);
        }

        private static UnsupportedOperationException notUsed() {
            return new UnsupportedOperationException("not used by readToEnds");
        }

        @Override
        public Set<TopicPartition> assignment() {
            throw notUsed();
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
