package io.github.tobyjamesclements.parsley;

/**
 * A {@link CausalBufferLimit} that never fires. Records are held until their causal dependencies
 * are satisfied, regardless of depth or wait time.
 */
record ParsleyUnboundedLimit() implements CausalBufferLimit {
}
