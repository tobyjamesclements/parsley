package io.parsley;

/**
 * Strict eviction: route the evicted record to {@code destination} and report a violation.
 */
record DeadLetterPolicy(CausalBufferLimit limit, String destination) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return true;
    }
}
