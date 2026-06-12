package io.parsley;

/**
 * The reason a causal ordering violation was reported.
 *
 * <p>A violation is reported to the {@link CausalViolationHandler} whenever a record cannot
 * be delivered in strict causal order.
 *
 * <ul>
 *   <li>{@link #MISSING_HEADER} — the record carried no {@code parsley-vector-clock} attribute
 *   <li>{@link #UNRESOLVABLE_CLOCK} — the attribute was present but could not be decoded
 *   <li>{@link #LIMIT_REACHED} — the record was evicted because a {@link BufferLimit} fired
 * </ul>
 */
public enum CausalViolationReason {

    /**
     * The incoming record had no {@code parsley-vector-clock} attribute. It was forwarded
     * immediately without buffering. This typically indicates a producer that is not
     * Parsley-aware.
     */
    MISSING_HEADER,

    /**
     * The {@code parsley-vector-clock} attribute was present but its contents could not be
     * deserialised into a {@link VectorClock}. The record was forwarded immediately.
     * This may indicate a version mismatch between producer and consumer.
     */
    UNRESOLVABLE_CLOCK,

    /**
     * The record's causal dependencies were not satisfied before the {@link BufferLimit}
     * fired. The record was evicted from the buffer according to the {@link BufferingPolicy}
     * (forwarded out-of-order, dropped, or dead-lettered).
     */
    LIMIT_REACHED
}
