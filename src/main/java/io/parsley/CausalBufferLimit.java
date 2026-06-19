package io.parsley;

import java.time.Duration;
import java.util.List;

/**
 * Defines when records held in a causal buffer should be evicted. Use the static factory methods to
 * construct instances:
 *
 * <ul>
 *   <li>{@link #ofDuration(Duration)} — evict after waiting this long with no frontier advancement
 *   <li>{@link #ofSize(int)} — evict when the buffer holds this many messages
 *   <li>{@link #first(CausalBufferLimit...)} — evict when the first of several limits fires
 * </ul>
 *
 * <p>A {@code CausalBufferLimit} is always paired with a {@link CausalBufferPolicy} that determines
 * <em>what</em> happens to evicted records (forward with a violation, drop, or dead-letter).
 */
public sealed interface CausalBufferLimit permits ParsleyDurationLimit, ParsleySizeLimit, ParsleyFirstLimit {

    /**
     * Creates a limit that evicts records buffered longer than {@code duration} without their causal
     * dependencies being satisfied.
     *
     * @param duration the maximum buffer duration; must not be {@code null}, zero, or negative
     * @return a new duration limit
     * @throws IllegalArgumentException if {@code duration} is {@code null}, zero, or negative
     */
    static CausalBufferLimit ofDuration(Duration duration) {
        return new ParsleyDurationLimit(duration);
    }

    /**
     * Creates a limit that evicts the oldest buffered records, one at a time, whenever the buffer
     * reaches {@code messages} entries — just enough to bring the buffer back under the limit,
     * according to the {@link CausalBufferPolicy}. Younger records remain held.
     *
     * @param messages the maximum buffer size in message count; must be positive
     * @return a new size limit
     * @throws IllegalArgumentException if {@code messages} is not positive
     */
    static CausalBufferLimit ofSize(int messages) {
        return new ParsleySizeLimit(messages);
    }

    /**
     * Creates a limit that fires when the <em>first</em> of {@code limits} fires.
     *
     * @param limits the constituent limits; at least one required, none {@code null}
     * @return a new composite limit
     * @throws IllegalArgumentException if {@code limits} is empty
     */
    static CausalBufferLimit first(CausalBufferLimit... limits) {
        return new ParsleyFirstLimit(List.of(limits));
    }
}
