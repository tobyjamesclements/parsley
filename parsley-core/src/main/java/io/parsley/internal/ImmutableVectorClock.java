package io.parsley.internal;

import io.parsley.Partition;
import io.parsley.VectorClock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public record ImmutableVectorClock(Map<Partition, Long> positions) implements VectorClock {

    public ImmutableVectorClock {
        positions = Collections.unmodifiableMap(new HashMap<>(positions));
    }

    public static ImmutableVectorClock empty() {
        return new ImmutableVectorClock(Map.of());
    }

    public ImmutableVectorClock merge(VectorClock other) {
        Map<Partition, Long> merged = new HashMap<>(positions);
        other.positions().forEach((p, offset) -> merged.merge(p, offset, Math::max));
        return new ImmutableVectorClock(merged);
    }

    public ImmutableVectorClock advance(Partition partition, long offset) {
        Map<Partition, Long> updated = new HashMap<>(positions);
        updated.merge(partition, offset, Math::max);
        return new ImmutableVectorClock(updated);
    }
}
