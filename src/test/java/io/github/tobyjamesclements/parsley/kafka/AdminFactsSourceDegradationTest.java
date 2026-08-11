package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that one unavailable partition degrades only its own channel's facts.
 *
 * <p>The facts round is the only mid-run carrier of evidence — including the sticky dead and
 * recreated verdicts — so a round must survive partial failure: a leaderless partition
 * withholds its own channel's positions and nothing else. Failure is injected at the
 * per-partition offset seam, standing where a broker outage fails the real future.
 */
class AdminFactsSourceDegradationTest {
    private static final UUID X_ID = new UUID(1, 1);
    private static final UUID Y_ID = new UUID(2, 2);
    private static final UUID Z_ID = new UUID(3, 3);
    private static final ChannelId A = new ChannelId(X_ID, 0);
    private static final ChannelId B = new ChannelId(Y_ID, 0);
    private static final ChannelId R = new ChannelId(Z_ID, 0);
    private static final TopicPartition X0 = new TopicPartition("x", 0);
    private static final TopicPartition Y0 = new TopicPartition("y", 0);

    /** Answers through the admin seams: topic x's partition is unavailable, y's is healthy,
     * and topic z's name now resolves to a different identity (recreated). */
    static final class DegradedFacts extends AdminFactsSource {
        DegradedFacts() {
            super(null, "g", Map.of(X_ID, "x", Y_ID, "y", Z_ID, "z"), Map.of(), 1_000L, () -> 0L);
        }

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) {
            Map<UUID, String> names = new HashMap<>();
            if (topicIds.contains(X_ID)) {
                names.put(X_ID, "x");
            }
            if (topicIds.contains(Y_ID)) {
                names.put(Y_ID, "y");
            }
            return names;
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            if (names.contains("z")) {
                outcome.put("z", new UUID(9, 9));
            }
            return outcome;
        }

        @Override
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsets(
                Map<TopicPartition, OffsetSpec> queries) {
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();
            for (TopicPartition tp : queries.keySet()) {
                if (tp.equals(X0)) {
                    KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> failed = new KafkaFutureImpl<>();
                    failed.completeExceptionally(new TimeoutException("no leader for " + tp));
                    futures.put(tp, failed);
                } else {
                    futures.put(tp, KafkaFuture.completedFuture(
                            new ListOffsetsResult.ListOffsetsResultInfo(4L, 0L, Optional.empty())));
                }
            }
            return futures;
        }

        @Override
        Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
            return Map.of(X0, new OffsetAndMetadata(2L), Y0, new OffsetAndMetadata(7L));
        }
    }

    /**
     * A task initialising while another round grinds against a slow broker must start
     * unseeded after a bounded wait, not stack on the stream thread behind the round.
     */
    @Test
    void aBusySourceYieldsAnUnseededStartInsteadOfAStall() throws Exception {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        AdminFactsSource parked = new AdminFactsSource(null, "g", Map.of(X_ID, "x"), Map.of(), 1_000L, () -> 0L) {
            @Override
            Map<UUID, String> describeByIds(Set<UUID> topicIds) throws Exception {
                entered.countDown();
                release.await();
                return Map.of();
            }

            @Override
            Map<String, Object> describeByNames(Set<String> names) {
                return Map.of();
            }

            @Override
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsets(
                    Map<TopicPartition, OffsetSpec> queries) {
                return Map.of();
            }

            @Override
            Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
                return Map.of();
            }
        };
        Thread background = new Thread(() -> {
            try {
                parked.gather(Set.of(A), Map.of(), Set.of());
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        background.start();
        assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS), "the round must be in flight");

        PositionFacts seed = parked.gatherForSeed(Set.of(A), Map.of(), Set.of());

        assertEquals(PositionFacts.EMPTY, seed, "a busy source must yield an unseeded start, not a stall");
        release.countDown();
        background.join(5_000);
    }

    @Test
    void oneUnavailablePartitionWithholdsOnlyItsOwnChannelsFacts() throws Exception {
        PositionFacts facts = new DegradedFacts().gather(Set.of(A, B), Map.of(), Set.of());

        assertEquals(Map.of(A, 2L, B, 7L), facts.committedNextRead(),
                "read positions come from the coordinator and must survive the partition outage");
        assertEquals(Map.of(B, 4L), facts.logStart(),
                "only the unavailable partition's log-start fact is withheld");
    }

    @Test
    void verdictsRideTheRoundThroughAPartitionOutage() throws Exception {
        PositionFacts facts = new DegradedFacts().gather(Set.of(A, B, R), Map.of(), Set.of());

        assertEquals(Set.of(R), facts.recreatedChannels(),
                "the recreated verdict must reach the engine despite the unavailable partition");
        assertTrue(facts.deadChannels().isEmpty());
        assertEquals(Map.of(A, 2L, B, 7L), facts.committedNextRead());
    }
}
