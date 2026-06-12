package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;

import java.util.List;

final class DropBuffer<K, V> extends AbstractCausalBuffer<K, V> {

    @Override
    public List<CausalRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler) {
        buffer.values().forEach(buffered ->
                handler.onViolation(buffered.record().toConsumerRecord(), CausalViolationReason.LIMIT_REACHED));
        buffer.clear();
        return List.of();
    }
}
