package io.parsley;

/**
 * Lenient eviction: forward the evicted record out-of-order and flag a violation.
 */
record ForwardUnsafePolicy(CausalBufferLimit limit) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return false;
    }
}
