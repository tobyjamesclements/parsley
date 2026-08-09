package io.github.tobyjamesclements.parsley.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * The causes a message expresses, or a process's causal frontier: at most one (channel, position) pair per channel,
 * the pair standing for that cause and every cause on the same channel at a lower position (compression under SPEC
 * Structural 13). Positions are non-negative.
 */
public final class Causes {

    private static final Causes NONE = new Causes(Collections.emptySortedMap());

    private final java.util.SortedMap<ChannelId, Long> byChannel;

    private Causes(java.util.SortedMap<ChannelId, Long> byChannel) {
        this.byChannel = byChannel;
    }

    public static Causes none() {
        return NONE;
    }

    public static Causes of(Map<ChannelId, Long> byChannel) {
        if (byChannel.isEmpty()) {
            return NONE;
        }
        TreeMap<ChannelId, Long> copy = new TreeMap<>();
        byChannel.forEach((channel, position) -> {
            if (position == null || position < 0) {
                throw new IllegalArgumentException("position must be non-negative on " + channel + ": " + position);
            }
            copy.put(channel, position);
        });
        return new Causes(Collections.unmodifiableSortedMap(copy));
    }

    /** Iteration order is the total order on {@link ChannelId}, so encodings are canonical. */
    public java.util.SortedMap<ChannelId, Long> byChannel() {
        return byChannel;
    }

    public boolean isEmpty() {
        return byChannel.isEmpty();
    }

    public int size() {
        return byChannel.size();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Causes other && byChannel.equals(other.byChannel);
    }

    @Override
    public int hashCode() {
        return byChannel.hashCode();
    }

    @Override
    public String toString() {
        return "Causes" + byChannel;
    }
}
