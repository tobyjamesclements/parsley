package io.parsley;

import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory {@link ParsleyWaitIndex} backed by a nested {@link TreeMap}. Used in unit tests that
 * exercise {@link ParsleyEngine} without a Kafka state store. Production uses
 * {@link RocksWaitIndex}.
 */
final class MockWaitIndex implements ParsleyWaitIndex {

    private record CoordKey(Uuid topicId, int partition) {}

    // coordinate → (requiredOffset → set of recordIds)
    private final Map<CoordKey, TreeMap<Long, Set<Long>>> index = new HashMap<>();

    @Override
    public void index(long recordId, CausalDependencies required, CausalFrontier frontier) {
        for (CausalPosition pos : required.dependencies()) {
            if (frontier.offsetFor(pos.topicId(), pos.partition()) < pos.offset()) {
                index.computeIfAbsent(new CoordKey(pos.topicId(), pos.partition()), k -> new TreeMap<>())
                     .computeIfAbsent(pos.offset(), o -> new HashSet<>())
                     .add(recordId);
            }
        }
    }

    @Override
    public List<Candidate> findCandidates(Uuid topicId, int partition, long newOffset) {
        NavigableMap<Long, Set<Long>> byOffset = index.get(new CoordKey(topicId, partition));
        if (byOffset == null) return List.of();
        List<Candidate> result = new ArrayList<>();
        byOffset.headMap(newOffset, true).forEach((offset, recordIds) -> {
            for (long recordId : recordIds) {
                result.add(new Candidate(topicId, partition, offset, recordId));
            }
        });
        return result;
    }

    @Override
    public void prune(Candidate candidate) {
        CoordKey key = new CoordKey(candidate.topicId(), candidate.partition());
        TreeMap<Long, Set<Long>> byOffset = index.get(key);
        if (byOffset == null) return;
        Set<Long> ids = byOffset.get(candidate.requiredOffset());
        if (ids != null) {
            ids.remove(candidate.recordId());
            if (ids.isEmpty()) byOffset.remove(candidate.requiredOffset());
        }
        if (byOffset.isEmpty()) index.remove(key);
    }
}
