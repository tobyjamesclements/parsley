package io.parsley;

/**
 * Thrown by {@link CausalViolationHandler#throwing()} when a causal violation is detected.
 *
 * <p>Carries the offending {@link CausalRecord} and the {@link CausalViolationReason} that
 * describes why the violation occurred.
 */
public class CausalViolationException extends RuntimeException {

    private final transient CausalRecord<?, ?, ?> record;
    private final CausalViolationReason reason;

    /**
     * Constructs a new exception for the given record and violation reason.
     *
     * @param record the record that caused the violation
     * @param reason the reason the violation was raised
     */
    public CausalViolationException(CausalRecord<?, ?, ?> record, CausalViolationReason reason) {
        super("Causal violation [" + reason + "] on " + record.source());
        this.record = record;
        this.reason = reason;
    }

    /**
     * Returns the record that triggered the violation.
     *
     * @return the offending record
     */
    public CausalRecord<?, ?, ?> record() { return record; }

    /**
     * Returns the reason the violation was raised.
     *
     * @return the violation reason
     */
    public CausalViolationReason reason() { return reason; }
}
