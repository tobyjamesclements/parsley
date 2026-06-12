package io.parsley.core.internal.buffer;

import io.parsley.BufferLimit;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;

import java.util.List;
import java.util.function.Consumer;

public final class DeadLetterBuffer<K, V, T extends VectorClock<T>> extends AbstractCausalBuffer<K, V, T> {

    private final Consumer<CausalRecord<K, V, T>> deadLetterSink;

    public DeadLetterBuffer(Consumer<CausalRecord<K, V, T>> deadLetterSink) {
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public List<CausalRecord<K, V, T>> evict(BufferLimit limit, CausalViolationHandler handler) {
        buffer.values().forEach(buffered -> {
            deadLetterSink.accept(buffered.record());
            handler.onViolation(buffered.record(), CausalViolationReason.LIMIT_REACHED);
        });
        buffer.clear();
        return List.of();
    }
}
