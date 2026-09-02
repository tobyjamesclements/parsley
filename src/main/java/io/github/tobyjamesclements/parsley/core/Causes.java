package io.github.tobyjamesclements.parsley.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * A causal frontier: per channel, the highest position known to be a cause.
 *
 * <p>This plays the role a vector clock plays in process-indexed causal broadcast, indexed by
 * channel because the protocol carries no process identity. A message stamped with a frontier
 * asserts that everything at or below each named position happened before it was sent.
 *
 * <p>Instances are immutable, and iterate in {@link ChannelId} order, which is the order the
 * wire format encodes in.
 *
 * @see CausesCodec
 * @see Deliverability#decide(Causes, java.util.Set, Deliverability.SettledView)
 */
public final class Causes {
    private static final Causes NONE = new Causes(Collections.emptySortedMap());

    private final java.util.SortedMap<ChannelId, Long> byChannel;

    private Causes(java.util.SortedMap<ChannelId, Long> byChannel) {
        this.byChannel = byChannel;
    }

    /**
     * The empty frontier, expressed by a message with no causes.
     *
     * @return a frontier naming no channel
     */
    public static Causes none() {
        return NONE;
    }

    /**
     * Builds a frontier.
     *
     * @param byChannel per channel, the highest causal position
     * @return the frontier, canonically ordered
     * @throws IllegalArgumentException if any position is null, negative, or the reserved
     *         maximum {@code Long.MAX_VALUE}, which no channel can assign (D105)
     */
    public static Causes of(Map<ChannelId, Long> byChannel) {
        if (byChannel.isEmpty()) {
            return NONE;
        }
        TreeMap<ChannelId, Long> copy = new TreeMap<>();
        byChannel.forEach((channel, position) -> {
            if (position == null || position < 0) {
                throw new IllegalArgumentException("position must be non-negative on " + channel + ": " + position);
            }
            if (position == Long.MAX_VALUE) {
                throw new IllegalArgumentException("position on " + channel + " is beyond any position a channel"
                        + " can assign: " + position);
            }
            copy.put(channel, position);
        });
        return new Causes(Collections.unmodifiableSortedMap(copy));
    }

    /**
     * Returns the frontier as an unmodifiable map in {@link ChannelId} order.
     *
     * @return the frontier as an unmodifiable map in {@link ChannelId} order
     */
    public java.util.SortedMap<ChannelId, Long> byChannel() {
        return byChannel;
    }

    /**
     * Returns {@code true} when this frontier names no channel.
     *
     * @return {@code true} when this frontier names no channel
     */
    public boolean isEmpty() {
        return byChannel.isEmpty();
    }

    /**
     * Returns how many channels this frontier names.
     *
     * @return how many channels this frontier names
     */
    public int size() {
        return byChannel.size();
    }

    /**
     * @param o the object to compare against
     * @return {@code true} when {@code o} is a frontier naming the same positions
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof Causes other && byChannel.equals(other.byChannel);
    }

    /**
     * Returns a hash consistent with {@link #equals(Object)}.
     *
     * @return a hash consistent with {@link #equals(Object)}
     */
    @Override
    public int hashCode() {
        return byChannel.hashCode();
    }

    /**
     * Returns the frontier rendered for diagnostics.
     *
     * @return the frontier rendered for diagnostics
     */
    @Override
    public String toString() {
        return "Causes" + byChannel;
    }
}
