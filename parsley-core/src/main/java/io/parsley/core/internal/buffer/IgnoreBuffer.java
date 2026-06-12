package io.parsley.core.internal.buffer;

import io.parsley.BufferLimit;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;

import java.util.ArrayList;
import java.util.List;

public final class IgnoreBuffer<K, V, T extends VectorClock<T>> extends AbstractCausalBuffer<K, V, T> {

    @Override
    public List<CausalRecord<K, V, T>> evict(BufferLimit limit, CausalViolationHandler handler) {
        List<CausalRecord<K, V, T>> evicted = new ArrayList<>(buffer.size());
        buffer.values().forEach(buffered -> {
            handler.onViolation(buffered.record(), CausalViolationReason.LIMIT_REACHED);
            evicted.add(buffered.record());
        });
        buffer.clear();
        return evicted;
    }
}
