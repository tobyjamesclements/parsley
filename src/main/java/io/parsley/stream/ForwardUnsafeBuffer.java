package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

final class ForwardUnsafeBuffer<K, V> extends AbstractCausalBuffer<K, V> {

    @Override
    public List<CausalRecord<K, V>> evict(
            BufferLimit limit, CausalViolationHandler handler, Consumer<CausalRecord<K, V>> onRemoved) {
        List<CausalRecord<K, V>> evicted = new ArrayList<>(buffer.size());
        buffer.values().forEach(buffered -> {
            handler.onViolation(buffered.record().toConsumerRecord(), CausalViolationReason.LIMIT_REACHED);
            onRemoved.accept(buffered.record());
            evicted.add(buffered.record());
        });
        buffer.clear();
        return evicted;
    }
}
