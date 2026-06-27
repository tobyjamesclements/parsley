package io.github.tobyjamesclements.parsley;

/**
 * A {@link CausalBufferLimit} that evicts records when it fires. Use
 * {@link CausalBufferLimit#unbounded()} to hold records indefinitely without eviction.
 */
public sealed interface CausalBoundedBufferLimit
        extends CausalBufferLimit
        permits ParsleySizeLimit, ParsleyDurationLimit, ParsleyFirstLimit {
}
