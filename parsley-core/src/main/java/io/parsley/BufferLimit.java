package io.parsley;

import java.time.Duration;

/**
 * Defines when records held in a causal buffer should be evicted.
 *
 * <p>{@code BufferLimit} is a sealed interface with five variants. Use the static factory methods
 * to construct instances:
 *
 * <ul>
 *   <li>{@link #ofDuration(Duration)} — evict after waiting this long with no frontier advancement
 *   <li>{@link #ofSize(int)} — evict when the buffer holds this many messages
 *   <li>{@link #ofBytes(long)} — evict when the buffer exceeds this total byte size
 *       <em>(not yet implemented; throws at construction time)</em>
 *   <li>{@link #ofFrontierAdvancement(long)} — evict after the frontier has advanced by this many
 *       offsets <em>(not yet implemented; throws at construction time)</em>
 *   <li>{@link #first(BufferLimit...)} — evict when the first of several limits fires
 * </ul>
 *
 * <p>A {@code BufferLimit} is always paired with a {@link BufferingPolicy} that determines
 * <em>what</em> happens to evicted records (forward with a violation, drop, or dead-letter).
 */
public sealed interface BufferLimit
        permits BufferLimit.DurationLimit, BufferLimit.SizeLimit, BufferLimit.BytesLimit,
                BufferLimit.FrontierAdvancementLimit, BufferLimit.FirstLimit {

    /**
     * Evicts records that have been buffered longer than {@code duration} without their
     * causal dependencies being satisfied.
     *
     * @param duration the maximum time to hold a record; must not be {@code null}
     */
    record DurationLimit(Duration duration) implements BufferLimit {}

    /**
     * Evicts records when the buffer size reaches {@code messages}.
     * Eviction removes the oldest record in the buffer.
     *
     * @param messages the maximum number of records to hold at once; must be positive
     */
    record SizeLimit(int messages) implements BufferLimit {}

    /**
     * Evicts records when the total serialised byte size of the buffer exceeds {@code bytes}.
     *
     * <p><strong>Not yet implemented.</strong> Constructing a {@link BufferingPolicy} with this
     * limit throws {@link UnsupportedOperationException}.
     *
     * @param bytes the maximum total byte size of buffered records
     */
    record BytesLimit(long bytes) implements BufferLimit {}

    /**
     * Evicts records once the partition frontier has advanced by at least {@code offsets} since
     * the record was buffered.
     *
     * <p><strong>Not yet implemented.</strong> Constructing a {@link BufferingPolicy} with this
     * limit throws {@link UnsupportedOperationException}.
     *
     * @param offsets the minimum frontier advancement required before eviction
     */
    record FrontierAdvancementLimit(long offsets) implements BufferLimit {}

    /**
     * Evicts records when the <em>first</em> of the given {@code limits} fires.
     *
     * @param limits two or more constituent limits; must not be {@code null} or empty
     */
    record FirstLimit(BufferLimit[] limits) implements BufferLimit {}

    /**
     * Creates a {@link DurationLimit} that evicts records buffered longer than {@code duration}.
     *
     * @param duration the maximum buffer duration; must not be {@code null}
     * @return a new {@code DurationLimit}
     */
    static BufferLimit ofDuration(Duration duration) { return new DurationLimit(duration); }

    /**
     * Creates a {@link SizeLimit} that evicts the oldest record when the buffer reaches
     * {@code messages} entries.
     *
     * @param messages the maximum buffer size in message count; must be positive
     * @return a new {@code SizeLimit}
     */
    static BufferLimit ofSize(int messages) { return new SizeLimit(messages); }

    /**
     * Creates a {@link BytesLimit}.
     *
     * <p><strong>Not yet implemented.</strong>
     *
     * @param bytes the maximum buffer size in bytes
     * @return a new {@code BytesLimit}
     */
    static BufferLimit ofBytes(long bytes) { return new BytesLimit(bytes); }

    /**
     * Creates a {@link FrontierAdvancementLimit}.
     *
     * <p><strong>Not yet implemented.</strong>
     *
     * @param offsets the frontier advancement threshold
     * @return a new {@code FrontierAdvancementLimit}
     */
    static BufferLimit ofFrontierAdvancement(long offsets) { return new FrontierAdvancementLimit(offsets); }

    /**
     * Creates a {@link FirstLimit} that fires when the first of {@code limits} fires.
     *
     * @param limits the constituent limits; at least one required
     * @return a new {@code FirstLimit}
     */
    static BufferLimit first(BufferLimit... limits) { return new FirstLimit(limits); }
}
