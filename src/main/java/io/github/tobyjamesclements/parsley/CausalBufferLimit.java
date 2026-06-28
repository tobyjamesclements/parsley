package io.github.tobyjamesclements.parsley;

import java.time.Duration;
import java.util.List;

/**
 * Defines when records held in a causal buffer should be evicted. Use the static factory methods to
 * construct instances:
 *
 * <ul>
 *   <li>{@link #ofDuration(Duration)} — evict after waiting this long with no frontier advancement
 *   <li>{@link #ofSize(int)} — evict when the buffer holds this many messages
 *   <li>{@link #first(CausalBoundedBufferLimit...)} — evict when the first of several limits fires
 *   <li>{@link #unbounded()} — never evict; hold records until their dependencies are satisfied
 * </ul>
 *
 * <p>When a bounded limit fires, the outcome depends on
 * {@code parsley.buffer.eviction.failure.policy}: the default ({@code fail}) fails the Streams task
 * fast rather than delivering the record out of causal order; {@code continue} evicts and forwards
 * it out of order instead, logging the causal gap and counting the violation metric. Either way,
 * Parsley never drops or silently loses a record.
 */
public sealed interface CausalBufferLimit permits CausalBoundedBufferLimit, ParsleyUnboundedLimit {

    /**
     * Creates a limit that evicts records buffered longer than {@code duration} without their causal
     * dependencies being satisfied.
     *
     * @param duration the maximum buffer duration; must not be {@code null}, zero, or negative
     * @return a new duration limit
     * @throws IllegalArgumentException if {@code duration} is {@code null}, zero, or negative
     */
    static CausalBoundedBufferLimit ofDuration(Duration duration) {
        return new ParsleyDurationLimit(duration);
    }

    /**
     * Creates a limit that evicts the oldest buffered records, one at a time, whenever the buffer
     * reaches {@code messages} entries — just enough to bring the buffer back under the limit.
     * Younger records remain held.
     *
     * @param messages the maximum buffer size in message count; must be positive
     * @return a new size limit
     * @throws IllegalArgumentException if {@code messages} is not positive
     */
    static CausalBoundedBufferLimit ofSize(int messages) {
        return new ParsleySizeLimit(messages);
    }

    /**
     * Creates a limit that fires when the <em>first</em> of {@code limits} fires.
     *
     * @param limits the constituent limits; at least one required, none {@code null}
     * @return a new composite limit
     * @throws IllegalArgumentException if {@code limits} is empty
     */
    static CausalBoundedBufferLimit first(CausalBoundedBufferLimit... limits) {
        return new ParsleyFirstLimit(List.of(limits));
    }

    /**
     * Creates a limit that never evicts. Records are held until their causal dependencies are
     * satisfied, regardless of how long they wait or how many accumulate.
     *
     * <p><strong>Warning:</strong> if a dependency can never be satisfied — for example because the
     * producing topic was deleted or its producer has stopped permanently — records will accumulate
     * without bound and will eventually exhaust disk on the RocksDB state store and the Kafka
     * changelog. Monitor buffer depth when using this option.
     *
     * @return an unbounded limit
     */
    static CausalBufferLimit unbounded() {
        return new ParsleyUnboundedLimit();
    }
}
