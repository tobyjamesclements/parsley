package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory {@link ParsleyOrphanIndex} backed by a per-coordinate map. Used in unit tests that
 * exercise {@link ParsleyEngine}/{@link ParsleyFrontier} without a Kafka state store. Production uses
 * {@link RocksOrphanIndex}.
 */
final class MockOrphanIndex implements ParsleyOrphanIndex {

    private record CoordKey(Uuid topicId, int partition) {}

    private final Map<CoordKey, Long> floors = new HashMap<>();

    @Override
    public boolean markOrphaned(Uuid topicId, int partition, long floor) {
        CoordKey key = new CoordKey(topicId, partition);
        Long existing = floors.get(key);
        if (existing != null && existing <= floor) {
            return false;
        }
        floors.put(key, floor);
        return true;
    }

    @Override
    public long orphanFloor(Uuid topicId, int partition) {
        return floors.getOrDefault(new CoordKey(topicId, partition), -1L);
    }

    @Override
    public void prune(Uuid topicId, int partition) {
        floors.remove(new CoordKey(topicId, partition));
    }
}
