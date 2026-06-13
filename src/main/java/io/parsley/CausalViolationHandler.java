package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Callback invoked whenever a causal ordering violation is detected.
 *
 * <p>A violation occurs when a record cannot be delivered in strict causal order — either
 * because it carried an unresolvable clock, lacked a clock attribute entirely, or was evicted
 * from the buffer due to a {@link BufferLimit} firing. The offending record is handed back as
 * the original Kafka {@link ConsumerRecord} so the handler can inspect its topic, partition,
 * offset, key, value, and headers.
 *
 * <p>Built-in strategies:
 * <ul>
 *   <li>{@link #throwing()} — throws a {@link CausalViolationException} on every violation
 *   <li>{@link #noop()} — silently ignores violations
 * </ul>
 */
@FunctionalInterface
public interface CausalViolationHandler {

    /**
     * Called when a causal violation occurs.
     *
     * @param record the record involved in the violation; never {@code null}
     * @param reason the reason for the violation; never {@code null}
     */
    void onViolation(ConsumerRecord<?, ?> record, CausalViolationReason reason);

    /**
     * Returns a handler that throws {@link CausalViolationException} on every violation.
     *
     * <p>Use when any causal disorder should be treated as a fatal error.
     *
     * @return a throwing {@code CausalViolationHandler}
     */
    static CausalViolationHandler throwing() {
        return (record, reason) -> { throw new CausalViolationException(record, reason); };
    }

    /**
     * Returns a handler that silently ignores all violations.
     *
     * @return a no-op {@code CausalViolationHandler}
     */
    static CausalViolationHandler noop() {
        return (record, reason) -> {};
    }
}
