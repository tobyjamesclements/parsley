package io.parsley.core.internal.buffer;

import io.parsley.CausalBuffer;
import io.parsley.CausalRecord;
import io.parsley.VectorClock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public abstract sealed class AbstractCausalBuffer<K, V, T extends VectorClock<T>>
        implements CausalBuffer<K, V, T>
        permits IgnoreBuffer, DropBuffer, DeadLetterBuffer {

    record Buffered<K, V, T extends VectorClock<T>>(CausalRecord<K, V, T> record, T dependencies) {}

    final LinkedHashMap<Long, Buffered<K, V, T>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    @Override
    public final void add(CausalRecord<K, V, T> record, T dependencies) {
        buffer.put(sequence++, new Buffered<>(record, dependencies));
    }

    @Override
    public final List<CausalRecord<K, V, T>> drain(T frontier) {
        List<CausalRecord<K, V, T>> released = new ArrayList<>();
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
