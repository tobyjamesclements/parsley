package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyOffsetSeeder}: pre-start consumer-group offset seeding under
 * {@code AutoOffsetReset.none()} sources. Exercised through a hand-rolled {@link ParsleySeedAdmin} double
 * (Parsley uses no mock framework), so the orchestration — the first-start seed, the surviving-state guard
 * that turns offset expiry into a loud failure, and the concurrent-startup robustness — is verified without
 * a broker.
 */
class ParsleyOffsetSeederTest {

    private static final String APP = "app";
    private static final String T1 = "t1";
    private static final String CHANGELOG = APP + "-somestore-changelog";
    private static final Set<String> NO_CHANGELOGS = Set.of();
    private static final Set<String> CHANGELOGS = Set.of(CHANGELOG);

    /**
     * A genuine first start — no changelog topics, no committed offsets — seeds every source partition to
     * its log-start offset in a single commit.
     */
    @Test
    void firstStartSeedsEveryMissingPartitionToLogStart() throws Exception {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.partitionCounts.put(T1, 2);
        admin.earliest.put(new TopicPartition(T1, 0), 100L);
        admin.earliest.put(new TopicPartition(T1, 1), 100L);

        ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS);

        assertEquals(1, admin.commitCalls.size(), "a first start must issue exactly one seed commit");
        assertEquals(Map.of(new TopicPartition(T1, 0), 100L, new TopicPartition(T1, 1), 100L),
                admin.commitCalls.get(0),
                "both source partitions must be seeded to their log-start offset");
    }

    /**
     * A normal restart — every source partition already has a committed offset — seeds nothing. A
     * genuinely out-of-range committed offset is deliberately left to fail fast under {@code none()}, not
     * re-seeded here.
     */
    @Test
    void normalRestartWithCommittedOffsetsSeedsNothing() throws Exception {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.partitionCounts.put(T1, 2);
        admin.committed.put(new TopicPartition(T1, 0), 5L);
        admin.committed.put(new TopicPartition(T1, 1), 7L);

        ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS);

        assertTrue(admin.commitCalls.isEmpty(), "a restart with committed offsets must not seed anything");
    }

    /**
     * Surviving Parsley state (a changelog topic exists) with a missing committed offset is NOT a first
     * start — it is offset expiry or a manual offset delete that left the causal state behind. Seeding then
     * could deliver records past the surviving frontier, so it must fail loudly instead. The re-list the
     * guard performs still shows the partition uncommitted (no peer is starting), so it is a genuine failure.
     */
    @Test
    void offsetExpiryWithSurvivingStateFailsLoudly() {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.topics.add(CHANGELOG);                             // surviving causal state
        admin.partitionCounts.put(T1, 1);
        admin.earliest.put(new TopicPartition(T1, 0), 400L);     // committed offsets expired (absent)

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), CHANGELOGS),
                "surviving state with a missing offset must fail the start, not seed over the frontier");
        assertTrue(thrown.getMessage().contains("surviving"),
                "the failure must explain that surviving state means this is not a first start: "
                        + thrown.getMessage());
        assertTrue(admin.commitCalls.isEmpty(), "nothing may be seeded once surviving state is detected");
    }

    /**
     * A peer instance's concurrent first start: this instance sees the peer's just-created changelog
     * (surviving state) but its initial offset list predates the peer's seed commit. The guard's re-list —
     * a fresh coordinator read, guaranteed to observe a commit that happens-before the changelog it already
     * saw — finds the partition committed by the peer, so this instance returns quietly without seeding or
     * failing.
     */
    @Test
    void survivingStateButAPeerAlreadySeededReturnsQuietly() throws Exception {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.topics.add(CHANGELOG);                             // a peer that already started created this
        admin.partitionCounts.put(T1, 1);
        // First list (initial missing check): empty. Second list (the guard re-list): the peer's seed.
        admin.committedResponses.add(Map.of());
        admin.committedResponses.add(Map.of(new TopicPartition(T1, 0), 100L));

        ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), CHANGELOGS);

        assertTrue(admin.commitCalls.isEmpty(),
                "a peer's concurrent seed (visible on the re-list) means this instance must not seed");
    }

    /**
     * On a first start, if {@code alterConsumerGroupOffsets} fails because the group is no longer empty (a
     * peer joined), the failure is tolerated and then verified: the re-list shows the peer committed the
     * partition, so the start proceeds.
     */
    @Test
    void groupNotEmptyIsToleratedWhenThePeerCommittedThePartition() throws Exception {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.partitionCounts.put(T1, 1);
        admin.earliest.put(new TopicPartition(T1, 0), 100L);
        // Initial list: empty (this instance thinks it must seed). Post-alter re-list: peer committed it.
        admin.committedResponses.add(Map.of());
        admin.committedResponses.add(Map.of(new TopicPartition(T1, 0), 100L));
        admin.commitOutcomes.put(new TopicPartition(T1, 0), new UnknownMemberIdException("group not empty"));

        ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS);   // must not throw
    }

    /**
     * A tolerated group-not-empty failure whose partition is STILL uncommitted on the re-list aborts the
     * start — no peer covered it, so proceeding would leave a first-start partition to fail under
     * {@code none()} record by record instead of loudly at startup.
     */
    @Test
    void groupNotEmptyThatNoPeerCoversAbortsTheStart() {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.partitionCounts.put(T1, 1);
        admin.earliest.put(new TopicPartition(T1, 0), 100L);
        admin.committedResponses.add(Map.of());   // initial
        admin.committedResponses.add(Map.of());   // re-list: still uncommitted
        admin.commitOutcomes.put(new TopicPartition(T1, 0), new UnknownMemberIdException("group not empty"));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS),
                "a group-not-empty failure that no peer covers must abort the start");
        assertTrue(thrown.getMessage().contains("no longer empty"),
                "the failure must explain the group was not empty and no peer committed: " + thrown.getMessage());
    }

    /**
     * Any seed-commit failure other than group-not-empty (here an authorization error) aborts the start
     * immediately — it is never swallowed as a concurrent-startup artifact.
     */
    @Test
    void aNonGroupNotEmptyCommitFailureAbortsTheStart() {
        FakeSeedAdmin admin = new FakeSeedAdmin();
        admin.partitionCounts.put(T1, 1);
        admin.earliest.put(new TopicPartition(T1, 0), 100L);
        admin.commitOutcomes.put(new TopicPartition(T1, 0), new TopicAuthorizationException("denied"));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS),
                "a non-group-not-empty commit failure must abort the start, not be tolerated");
        assertTrue(thrown.getMessage().contains("failed to seed"),
                "the failure must name the seed commit that failed: " + thrown.getMessage());
    }

    /**
     * A causal source topic that does not exist at startup aborts the start, rather than being silently
     * skipped — the admin surfaces {@code UnknownTopicOrPartitionException} when describing it (exactly as
     * the real admin's {@code describeTopics(...).get()} does), which propagates out of seeding.
     */
    @Test
    void anAbsentSourceTopicAbortsTheStart() {
        FakeSeedAdmin admin = new FakeSeedAdmin();   // partitionCounts has no entry for T1

        assertThrows(UnknownTopicOrPartitionException.class,
                () -> ParsleyOffsetSeeder.seed(admin, APP, Set.of(T1), NO_CHANGELOGS),
                "an absent causal source topic must abort the start");
    }

    /**
     * A programmable {@link ParsleySeedAdmin} double. {@code committedResponses} supplies successive
     * results for {@link #committedOffsets} (for the multi-list concurrent-startup paths); when it is
     * exhausted, {@link #committed} is returned. A successful seed commit records into {@link #committed}.
     */
    private static final class FakeSeedAdmin implements ParsleySeedAdmin {
        final Set<String> topics = new HashSet<>();
        final Map<String, Integer> partitionCounts = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        final Map<TopicPartition, Long> earliest = new HashMap<>();
        final Map<TopicPartition, Throwable> commitOutcomes = new HashMap<>();
        final Deque<Map<TopicPartition, Long>> committedResponses = new ArrayDeque<>();
        final List<Map<TopicPartition, Long>> commitCalls = new ArrayList<>();

        @Override
        public Set<String> listTopicNames() {
            return new HashSet<>(topics);
        }

        @Override
        public Map<String, Integer> partitionCounts(Collection<String> requested) {
            Map<String, Integer> counts = new HashMap<>();
            for (String topic : requested) {
                Integer count = partitionCounts.get(topic);
                if (count == null) {
                    // Mirror the real admin: describeTopics(...).get() surfaces this for an absent topic.
                    throw new UnknownTopicOrPartitionException("no such topic: " + topic);
                }
                counts.put(topic, count);
            }
            return counts;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets(String groupId) {
            if (!committedResponses.isEmpty()) {
                return new HashMap<>(committedResponses.poll());
            }
            return new HashMap<>(committed);
        }

        @Override
        public Map<TopicPartition, Long> earliestOffsets(Collection<TopicPartition> partitions) {
            Map<TopicPartition, Long> result = new HashMap<>();
            partitions.forEach(partition -> result.put(partition, earliest.get(partition)));
            return result;
        }

        @Override
        public Map<TopicPartition, @Nullable Throwable> commitSeedOffsets(
                String groupId, Map<TopicPartition, Long> offsets) {
            commitCalls.add(new HashMap<>(offsets));
            Map<TopicPartition, @Nullable Throwable> outcomes = new HashMap<>();
            offsets.forEach((partition, offset) -> {
                Throwable failure = commitOutcomes.get(partition);
                outcomes.put(partition, failure);
                if (failure == null) {
                    committed.put(partition, offset);
                }
            });
            return outcomes;
        }

        @Override
        public void close() {
        }
    }
}
