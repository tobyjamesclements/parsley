package io.parsley.core.internal.buffer;

import io.parsley.BufferLimit;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;

import java.util.List;

public final class DropBuffer<K, V, T extends VectorClock<T>> extends AbstractCausalBuffer<K, V, T> {

    @Override
    public List<CausalRecord<K, V, T>> evict(BufferLimit limit, CausalViolationHandler handler) {
        buffer.values().forEach(buffered ->
                handler.onViolation(buffered.record(), CausalViolationReason.LIMIT_REACHED));
        buffer.clear();
        return List.of();
    }
}
