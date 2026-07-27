package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * A vector clock over channels: a map from {@link Channel} to an offset watermark, meaning
 * "every offset at or below the watermark on that channel is claimed". Absent channels claim
 * nothing. Offsets are broker offsets, so watermarks are {@code >= 0}; {@link #NOTHING} is the
 * absent sentinel.
 *
 * <p>Mutable; callers copy when they need a snapshot. The serialized form is versioned and
 * sorted by channel so equal clocks are byte-identical.
 */
public final class Clock {

    public static final long NOTHING = -1L;
    private static final byte WIRE_VERSION = 1;

    private final TreeMap<Channel, Long> entries = new TreeMap<>();

    public Clock() {}

    public static Clock of(Channel c, long offset) {
        Clock k = new Clock();
        k.advanceTo(c, offset);
        return k;
    }

    public long get(Channel c) {
        Long v = entries.get(c);
        return v == null ? NOTHING : v;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int width() {
        return entries.size();
    }

    /** Raises the watermark for {@code c} to at least {@code offset}; never lowers it. */
    public void advanceTo(Channel c, long offset) {
        if (offset < 0) throw new IllegalArgumentException("offset " + offset);
        entries.merge(c, offset, Math::max);
    }

    /** Pointwise max-merge of {@code other} into this clock. */
    public void mergeMax(Clock other) {
        other.entries.forEach((c, o) -> entries.merge(c, o, Math::max));
    }

    /** True iff this clock is pointwise {@code >=} {@code other} on every channel of other. */
    public boolean dominates(Clock other) {
        for (Map.Entry<Channel, Long> e : other.entries.entrySet()) {
            if (get(e.getKey()) < e.getValue()) return false;
        }
        return true;
    }

    /** A new clock containing only entries whose channel is in {@code channels}. */
    public Clock restrictedTo(Set<Channel> channels) {
        Clock k = new Clock();
        entries.forEach((c, o) -> {
            if (channels.contains(c)) k.entries.put(c, o);
        });
        return k;
    }

    public void remove(Channel c) {
        entries.remove(c);
    }

    /** Drops every entry at or below the corresponding watermark in {@code stability}. */
    public void truncateAtOrBelow(Clock stability) {
        entries.entrySet().removeIf(e -> e.getValue() <= stability.get(e.getKey()));
    }

    public void forEach(BiConsumer<Channel, Long> consumer) {
        entries.forEach(consumer);
    }

    public Clock copy() {
        Clock k = new Clock();
        k.entries.putAll(entries);
        return k;
    }

    public byte[] serialize() {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + entries.size() * 28);
        buf.put(WIRE_VERSION);
        buf.putInt(entries.size());
        entries.forEach((c, o) -> {
            buf.putLong(c.topicId().getMostSignificantBits());
            buf.putLong(c.topicId().getLeastSignificantBits());
            buf.putInt(c.partition());
            buf.putLong(o);
        });
        return buf.array();
    }

    /**
     * @throws CorruptClockException on any malformed input; a present but undecodable clock must
     *     fail the task, never read as empty (an empty read would silently drop claims).
     */
    public static Clock deserialize(byte[] bytes) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            byte version = buf.get();
            if (version != WIRE_VERSION) throw new CorruptClockException("unknown clock version " + version);
            int n = buf.getInt();
            if (n < 0 || bytes.length != 1 + 4 + n * 28) {
                throw new CorruptClockException("clock length mismatch: " + n + " entries, " + bytes.length + " bytes");
            }
            Clock k = new Clock();
            for (int i = 0; i < n; i++) {
                var topicId = new java.util.UUID(buf.getLong(), buf.getLong());
                int partition = buf.getInt();
                long offset = buf.getLong();
                if (partition < 0 || offset < 0) throw new CorruptClockException("negative field in clock entry");
                k.entries.put(new Channel(topicId, partition), offset);
            }
            return k;
        } catch (CorruptClockException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CorruptClockException("undecodable clock: " + e, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Clock k && entries.equals(k.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return entries.toString();
    }
}
