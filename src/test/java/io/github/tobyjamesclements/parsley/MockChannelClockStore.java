package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory {@link ParsleyChannelClockStore} backed by a {@link HashMap}. Used in unit tests
 * that exercise {@link ParsleyEngine#completeness()} without a Kafka state store. Production uses
 * {@link RocksChannelClockStore}.
 */
final class MockChannelClockStore implements ParsleyChannelClockStore {

    private record CoordKey(Uuid topicId, int partition) {}

    private final Map<CoordKey, ParsleyClock> clocks = new HashMap<>();

    @Override
    public void update(Uuid topicId, int partition, ParsleyClock clock) {
        clocks.merge(new CoordKey(topicId, partition), clock, ParsleyClock::merge);
    }

    @Override
    public ParsleyClock get(Uuid topicId, int partition) {
        return clocks.getOrDefault(new CoordKey(topicId, partition), ParsleyClock.empty());
    }

    @Override
    public void remove(Uuid topicId, int partition) {
        clocks.remove(new CoordKey(topicId, partition));
    }

    @Override
    public List<ChannelEntry> allEntries() {
        List<ChannelEntry> entries = new ArrayList<>();
        clocks.forEach((key, clock) -> entries.add(new ChannelEntry(key.topicId(), key.partition(), clock)));
        return entries;
    }
}
