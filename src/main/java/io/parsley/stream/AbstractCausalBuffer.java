package io.parsley.stream;

import io.parsley.VectorClock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

abstract sealed class AbstractCausalBuffer<K, V>
        implements CausalBuffer<K, V>
        permits IgnoreBuffer, DropBuffer, DeadLetterBuffer {

    record Buffered<K, V>(CausalRecord<K, V> record, VectorClock dependencies) {}

    final LinkedHashMap<Long, Buffered<K, V>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    @Override
    public final void add(CausalRecord<K, V> record, VectorClock dependencies) {
        buffer.put(sequence++, new Buffered<>(record, dependencies));
    }

    @Override
    public final List<CausalRecord<K, V>> drain(VectorClock frontier) {
        List<CausalRecord<K, V>> released = new ArrayList<>();
        buffer.entrySet().removeIf(entry -> {
            if (entry.getValue().dependencies().satisfiedBy(frontier)) {
                released.add(entry.getValue().record());
                return true;
            }
            return false;
        });
        return released;
    }

    @Override
    public final int size() {
        return buffer.size();
    }
}
