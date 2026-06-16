package io.parsley;

/**
 * Strict eviction: discard the evicted record and report a
 * {@link CausalViolationReason#LIMIT_REACHED} violation.
 *
 * @param limit the eviction trigger
 */
record DropPolicy(CausalBufferLimit limit) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return true;
    }
}
