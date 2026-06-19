package io.parsley;

/**
 * The action a {@link CausalBufferPolicy} takes when a causal violation occurs.
 *
 * <ul>
 *   <li>{@link #FORWARD_UNSAFE} — forward the record downstream out-of-causal-order (lenient;
 *       delivery is preserved but the causal guarantee is suspended for that record)
 *   <li>{@link #DROP} — silently discard the record (strict; the causal guarantee holds for every
 *       delivered record, at the cost of message loss)
 *   <li>{@link #DEAD_LETTER} — route the record to the configured dead-letter sink (strict;
 *       requires {@link CausalProcessors.Builder#deadLetterSink} to be set)
 * </ul>
 *
 * <p>A {@link CausalViolationHandler} is always invoked regardless of which action is taken.
 */
public enum CausalViolationAction {
    FORWARD_UNSAFE,
    DROP,
    DEAD_LETTER
}
