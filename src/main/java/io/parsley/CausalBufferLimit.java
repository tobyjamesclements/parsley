package io.parsley;

import java.time.Duration;
import java.util.List;

/**
 * Defines when records held in a causal buffer should be evicted.
 *
 * <p>{@code CausalBufferLimit} is a sealed interface. Use the static factory methods to construct
 * instances:
 *
 * <ul>
 *   <li>{@link #ofDuration(Duration)} — evict after waiting this long with no frontier advancement
 *   <li>{@link #ofSize(int)} — evict when the buffer holds this many messages
 *   <li>{@link #first(CausalBufferLimit...)} — evict when the first of several limits fires
 * </ul>
 *
 * <p>A {@code CausalBufferLimit} is always paired with a {@link CausalBufferingPolicy} that determines
 * <em>what</em> happens to evicted records (forward with a violation, drop, or dead-letter).
 */
public sealed interface CausalBufferLimit permits
        CausalBufferLimit.DurationLimit,
        CausalBufferLimit.SizeLimit,
        CausalBufferLimit.FirstLimit {

    /**
     * Creates a {@link DurationLimit} that evicts records buffered longer than {@code duration}.
     *
     * @param duration the maximum buffer duration; must not be {@code null}
     * @return a new {@code DurationLimit}
     */
    static CausalBufferLimit ofDuration(Duration duration) {
        return new DurationLimit(duration);
    }

    /**
     * Creates a {@link SizeLimit} that evicts the buffer when it reaches
     * {@code messages} entries.
     *
     * @param messages the maximum buffer size in message count; must be positive
     * @return a new {@code SizeLimit}
     */
    static CausalBufferLimit ofSize(int messages) {
        return new SizeLimit(messages);
    }

    /**
     * Creates a {@link FirstLimit} that fires when the first of {@code limits} fires.
     *
     * @param limits the constituent limits; at least one required, none {@code null}
     * @return a new {@code FirstLimit}
     */
    static CausalBufferLimit first(CausalBufferLimit... limits) {
        return new FirstLimit(List.of(limits));
    }

    /**
     * Evicts records that have been buffered longer than {@code duration} without their
     * causal dependencies being satisfied.
     *
     * @param duration the maximum time to hold a record; must not be {@code null},
     *                 zero, or negative
     */
    record DurationLimit(Duration duration) implements CausalBufferLimit {
        /**
         * Canonical constructor.
         *
         * @throws IllegalArgumentException if {@code duration} is {@code null}, zero,
         *                                  or negative
         */
        public DurationLimit {
            if (duration == null || duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(
                        "duration must be positive, was " + duration);
            }
        }
    }

    /**
     * Evicts the buffer when its size reaches {@code messages}.
     * Eviction releases <em>all</em> buffered records according to the
     * {@link CausalBufferingPolicy}.
     *
     * @param messages the maximum number of records to hold at once; must be positive
     */
    record SizeLimit(int messages) implements CausalBufferLimit {
        /**
         * Canonical constructor.
         *
         * @throws IllegalArgumentException if {@code messages} is not positive
         */
        public SizeLimit {
            if (messages <= 0) {
                throw new IllegalArgumentException(
                        "messages must be positive, was " + messages);
            }
        }
    }

    /**
     * Evicts records when the <em>first</em> of the given {@code limits} fires.
     *
     * @param limits the constituent limits; must not be {@code null} or empty; copied
     *               defensively
     */
    record FirstLimit(List<CausalBufferLimit> limits) implements CausalBufferLimit {
        /**
         * Canonical constructor; defensively copies {@code limits}.
         *
         * @throws IllegalArgumentException if {@code limits} is empty
         * @throws NullPointerException     if {@code limits} or any element is {@code null}
         */
        public FirstLimit {
            limits = List.copyOf(limits);
            if (limits.isEmpty()) {
                throw new IllegalArgumentException("at least one limit is required");
            }
        }
    }
}
