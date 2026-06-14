package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;

import java.util.List;
import java.util.function.Consumer;

final class DeadLetterBuffer<K, V> extends AbstractCausalBuffer<K, V> {

    private final Consumer<CausalRecord<K, V>> deadLetterSink;

    DeadLetterBuffer(Consumer<CausalRecord<K, V>> deadLetterSink) {
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public List<CausalRecord<K, V>> evict(
            BufferLimit limit, CausalViolationHandler handler, Consumer<CausalRecord<K, V>> onRemoved) {
        buffer.values().forEach(buffered -> {
            deadLetterSink.accept(buffered.record());
            handler.onViolation(buffered.record().toConsumerRecord(), CausalViolationReason.LIMIT_REACHED);
            onRemoved.accept(buffered.record());
        });
        buffer.clear();
        return List.of();
    }
}
