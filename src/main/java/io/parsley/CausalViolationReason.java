package io.parsley;

/**
 * The reason a causal ordering violation was reported.
 *
 * <p>A violation is reported to the {@link CausalViolationHandler} whenever a record cannot
 * be delivered in strict causal order.
 *
 * <ul>
 *   <li>{@link #MISSING_HEADER} — the record carried no {@code parsley-causal-dependencies} attribute
 *   <li>{@link #UNRESOLVABLE_DEPENDENCIES} — the attribute was present but could not be decoded
 *   <li>{@link #LIMIT_REACHED} — the record was evicted because a {@link CausalBufferLimit} fired
 * </ul>
 */
public enum CausalViolationReason {

    /**
     * The incoming record carried no {@code parsley-causal-dependencies} header at all. This
     * indicates a producer that is not Parsley-aware (e.g. a plain {@code KafkaProducer}).
     * A {@link CausalProducer} that has no dependencies stamps an explicit empty header instead,
     * which is not a violation.
     */
    MISSING_HEADER,

    /**
     * The {@code parsley-causal-dependencies} header was present but its contents could not be
     * deserialised into a {@link CausalDependencies}. This may indicate a version mismatch
     * between producer and consumer.
     */
    UNRESOLVABLE_DEPENDENCIES,

    /**
     * The record's causal dependencies were not satisfied before the {@link CausalBufferLimit}
     * fired. The record was evicted from the buffer according to the {@link CausalBufferPolicy}
     * (forwarded out-of-order, dropped, or dead-lettered).
     */
    LIMIT_REACHED
}
