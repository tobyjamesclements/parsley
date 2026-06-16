package io.parsley;

/**
 * Strict eviction: discard the evicted record and report a violation.
 */
record DropPolicy(CausalBufferLimit limit) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return true;
    }
}
