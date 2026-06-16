package io.parsley;

/**
 * Strict eviction: route the evicted record to {@code destination} and report a
 * {@link CausalViolationReason#LIMIT_REACHED} violation.
 *
 * @param limit       the eviction trigger
 * @param destination the dead-letter destination name (e.g. a Kafka topic)
 */
record DeadLetterPolicy(CausalBufferLimit limit, String destination) implements CausalBufferPolicy {
    @Override
    public boolean strict() {
        return true;
    }
}
