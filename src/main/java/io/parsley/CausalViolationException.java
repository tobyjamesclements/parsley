package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Thrown by {@link CausalViolationHandler#throwOnViolation()} when a causal violation is detected.
 *
 * <p>Carries the offending {@link ConsumerRecord} — a record evicted from the causal buffer
 * before its dependencies were satisfied.
 */
public class CausalViolationException extends RuntimeException {

    private final transient ConsumerRecord<?, ?> record;

    /**
     * Constructs a new exception for the given record.
     *
     * @param record the record that caused the violation
     */
    public CausalViolationException(ConsumerRecord<?, ?> record) {
        super("Causal violation [EVICTED] on " + record.topic() + "-" + record.partition() + "@" + record.offset());
        this.record = record;
    }

    /**
     * Returns the record that triggered the violation.
     *
     * @return the offending record
     */
    public ConsumerRecord<?, ?> record() { return record; }
}
