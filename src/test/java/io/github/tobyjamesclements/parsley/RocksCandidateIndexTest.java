package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link RocksCandidateIndex} — the {@link org.apache.kafka.streams.state.KeyValueStore}-backed
 * {@link ParsleyCandidateIndex} implementation — against a real {@link org.apache.kafka.streams.state.KeyValueStore}
 * (via {@link TestKeyValueStore}), exercising the actual big-endian byte-key range scan rather than
 * {@link MockCandidateIndex}'s in-memory structure.
 */
class RocksCandidateIndexTest {

    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    /**
     * A record indexed with a dependency the frontier has not yet reached is found by a subsequent
     * {@code findCandidates} scan once the new offset reaches the required offset; a dependency the
     * frontier already satisfies at index time is never indexed.
     *
     * Asserts the satisfied-at-index-time dependency produces no candidate, and the unsatisfied one
     * is returned once the scanned offset range covers it.
     */
    @Test
    void indexOnlyStoresUnsatisfiedDependenciesAndFindCandidatesScansTheRealRange() {
        RocksCandidateIndex index = new RocksCandidateIndex(newRocksStore());
        ParsleyClock frontier = ParsleyClock.empty().observe(TOPIC_ID, 0, 5);
        ParsleyClock required = ParsleyClock.empty()
                .observe(TOPIC_ID, 0, 10)   // ahead of the frontier (5) — must be indexed
                .observe(TOPIC_ID, 1, 2);   // partition 1 has no frontier entry (-1) — must be indexed

        index.index(42L, required, frontier);

        List<ParsleyCandidateIndex.Candidate> tooEarly = index.findCandidates(TOPIC_ID, 0, 9);
        assertEquals(List.of(), tooEarly, "a scan below the required offset must not find the candidate");

        List<ParsleyCandidateIndex.Candidate> found = index.findCandidates(TOPIC_ID, 0, 10);
        assertEquals(1, found.size(), "a scan reaching the required offset must find exactly one candidate");
        assertEquals(42L, found.get(0).recordId(), "the candidate must carry the indexed record id");
        assertEquals(10L, found.get(0).requiredOffset(), "the candidate must carry its required offset");

        List<ParsleyCandidateIndex.Candidate> otherPartition = index.findCandidates(TOPIC_ID, 1, 2);
        assertEquals(1, otherPartition.size(), "partition 1's unsatisfied dependency must also be indexed");
    }

    /**
     * {@code prune} removes the underlying store entry for a candidate, so a later
     * {@code findCandidates} scan over the same range no longer returns it — this is how a resolved
     * dependency is retired from the index.
     *
     * Asserts the candidate is present before pruning and absent after.
     */
    @Test
    void pruneRemovesTheCandidateFromTheRealStore() {
        RocksCandidateIndex index = new RocksCandidateIndex(newRocksStore());
        ParsleyClock required = ParsleyClock.empty().observe(TOPIC_ID, 0, 10);
        index.index(7L, required, ParsleyClock.empty());

        List<ParsleyCandidateIndex.Candidate> before = index.findCandidates(TOPIC_ID, 0, 10);
        assertEquals(1, before.size(), "the candidate must be present before pruning");

        index.prune(before.get(0));

        List<ParsleyCandidateIndex.Candidate> after = index.findCandidates(TOPIC_ID, 0, 10);
        assertTrue(after.isEmpty(), "the candidate must be gone from the store after pruning");
    }

    /**
     * Candidates for distinct {@code (topicId, partition)} coordinates occupy disjoint key ranges
     * (the coordinate is the leading portion of the big-endian key), so a scan for one partition
     * never returns another partition's — or another topic's — candidates.
     *
     * Asserts a scan for one partition returns only that partition's candidate.
     */
    @Test
    void findCandidatesIsScopedToItsCoordinate() {
        RocksCandidateIndex index = new RocksCandidateIndex(newRocksStore());
        Uuid otherTopicId = Uuid.randomUuid();
        index.index(1L, ParsleyClock.empty().observe(TOPIC_ID, 0, 10), ParsleyClock.empty());
        index.index(2L, ParsleyClock.empty().observe(TOPIC_ID, 1, 10), ParsleyClock.empty());
        index.index(3L, ParsleyClock.empty().observe(otherTopicId, 0, 10), ParsleyClock.empty());

        List<ParsleyCandidateIndex.Candidate> candidates = index.findCandidates(TOPIC_ID, 0, 10);

        assertEquals(1, candidates.size(), "a scan must not cross partition or topic boundaries");
        assertEquals(1L, candidates.get(0).recordId(), "the scan must return only the matching coordinate's candidate");
    }

    private static org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> newRocksStore() {
        return new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned);
    }
}
