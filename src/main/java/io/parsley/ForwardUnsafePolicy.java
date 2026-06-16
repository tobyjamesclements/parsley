package io.parsley;

/**
 * Lenient eviction: forward the evicted record out-of-order and flag a
 * {@link CausalViolationReason#LIMIT_REACHED} violation.
 *
 * @param limit the eviction trigger
 */
record ForwardUnsafePolicy(CausalBufferLimit limit) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return false;
    }
}
