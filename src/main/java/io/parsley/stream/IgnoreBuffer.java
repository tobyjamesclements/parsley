package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;

import java.util.ArrayList;
import java.util.List;

final class IgnoreBuffer<K, V> extends AbstractCausalBuffer<K, V> {

    @Override
    public List<CausalRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler) {
        List<CausalRecord<K, V>> evicted = new ArrayList<>(buffer.size());
        buffer.values().forEach(buffered -> {
            handler.onViolation(buffered.record().toConsumerRecord(), CausalViolationReason.LIMIT_REACHED);
            evicted.add(buffered.record());
        });
        buffer.clear();
        return evicted;
    }
}
