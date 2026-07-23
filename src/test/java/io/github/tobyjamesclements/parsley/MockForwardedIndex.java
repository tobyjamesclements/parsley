package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * An in-memory {@link ParsleyForwardedIndex} backed by a per-coordinate {@link TreeSet}. Used in
 * unit tests that exercise {@link ParsleyCausalBroadcast} without a Kafka state store. Production uses
 * {@link StoreBackedForwardedIndex}.
 */
final class MockForwardedIndex implements ParsleyForwardedIndex {

    private record CoordKey(Uuid topicId, int partition) {}

    private final Map<CoordKey, TreeSet<Long>> index = new HashMap<>();

    @Override
    public void mark(Uuid topicId, int partition, long offset) {
        index.computeIfAbsent(new CoordKey(topicId, partition), k -> new TreeSet<>()).add(offset);
    }

    @Override
    public List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset) {
        NavigableSet<Long> offsets = index.get(new CoordKey(topicId, partition));
        if (offsets == null) return List.of();
        return new ArrayList<>(offsets.tailSet(frontierOffset + 1));
    }

    @Override
    public boolean contains(Uuid topicId, int partition, long offset) {
        NavigableSet<Long> offsets = index.get(new CoordKey(topicId, partition));
        return offsets != null && offsets.contains(offset);
    }

    @Override
    public void unmark(Uuid topicId, int partition, long offset) {
        CoordKey key = new CoordKey(topicId, partition);
        NavigableSet<Long> offsets = index.get(key);
        if (offsets == null) return;
        offsets.remove(offset);
        if (offsets.isEmpty()) index.remove(key);
    }

    @Override
    public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
        if (watermark < 0) return;
        CoordKey key = new CoordKey(topicId, partition);
        NavigableSet<Long> offsets = index.get(key);
        if (offsets == null) return;
        offsets.headSet(watermark, true).clear();
        if (offsets.isEmpty()) index.remove(key);
    }
}
