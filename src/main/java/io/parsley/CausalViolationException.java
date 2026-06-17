package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Thrown by {@link CausalViolationHandler#throwOnViolation()} when a causal violation is detected.
 *
 * <p>Carries the offending {@link ConsumerRecord} and the {@link CausalViolationReason} that
 * describes why the violation occurred.
 */
public class CausalViolationException extends RuntimeException {

    private final transient ConsumerRecord<?, ?> record;
    private final CausalViolationReason reason;

    /**
     * Constructs a new exception for the given record and violation reason.
     *
     * @param record the record that caused the violation
     * @param reason the reason the violation was raised
     */
    public CausalViolationException(ConsumerRecord<?, ?> record, CausalViolationReason reason) {
        super("Causal violation [" + reason + "] on "
                + record.topic() + "-" + record.partition() + "@" + record.offset());
        this.record = record;
        this.reason = reason;
    }

    /**
     * Returns the record that triggered the violation.
     *
     * @return the offending record
     */
    public ConsumerRecord<?, ?> record() { return record; }

    /**
     * Returns the reason the violation was raised.
     *
     * @return the violation reason
     */
    public CausalViolationReason reason() { return reason; }
}
