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
public sealed interface CausalBufferLimit permits DurationLimit, SizeLimit, FirstLimit {

    /**
     * Creates a limit that evicts records buffered longer than {@code duration} without their causal
     * dependencies being satisfied.
     *
     * @param duration the maximum buffer duration; must not be {@code null}, zero, or negative
     * @return a new duration limit
     * @throws IllegalArgumentException if {@code duration} is {@code null}, zero, or negative
     */
    static CausalBufferLimit ofDuration(Duration duration) {
        return new DurationLimit(duration);
    }

    /**
     * Creates a limit that evicts the buffer when it reaches {@code messages} entries (eviction
     * releases <em>all</em> buffered records according to the {@link CausalBufferPolicy}).
     *
     * @param messages the maximum buffer size in message count; must be positive
     * @return a new size limit
     * @throws IllegalArgumentException if {@code messages} is not positive
     */
    static CausalBufferLimit ofSize(int messages) {
        return new SizeLimit(messages);
    }

    /**
     * Creates a limit that fires when the <em>first</em> of {@code limits} fires.
     *
     * @param limits the constituent limits; at least one required, none {@code null}
     * @return a new composite limit
     * @throws IllegalArgumentException if {@code limits} is empty
     */
    static CausalBufferLimit first(CausalBufferLimit... limits) {
        return new FirstLimit(List.of(limits));
    }
}

/** Evicts records buffered longer than {@code duration}. */
record DurationLimit(Duration duration) implements CausalBufferLimit {
    DurationLimit {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("duration must be positive, was " + duration);
        }
    }
}

/** Evicts the buffer when its size reaches {@code messages}. */
record SizeLimit(int messages) implements CausalBufferLimit {
    SizeLimit {
        if (messages <= 0) {
            throw new IllegalArgumentException("messages must be positive, was " + messages);
        }
    }
}

/** Evicts when the first of {@code limits} fires; copies {@code limits} defensively. */
record FirstLimit(List<CausalBufferLimit> limits) implements CausalBufferLimit {
    FirstLimit {
        limits = List.copyOf(limits);
        if (limits.isEmpty()) {
            throw new IllegalArgumentException("at least one limit is required");
        }
    }
}
