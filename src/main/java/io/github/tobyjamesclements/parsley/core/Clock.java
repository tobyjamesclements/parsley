package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * A vector clock over channels, with two claim kinds:
 *
 * <ul>
 *   <li><b>Offset entries</b> — a map from {@link Channel} to an offset watermark: "every
 *       offset at or below the watermark on that channel is claimed".</li>
 *   <li><b>Sequence entries</b> — a map from (channel, sender) to a send-sequence watermark:
 *       "every record the sender sent to that channel with sequence at or below the watermark
 *       is claimed". Sequence claims are known synchronously at send time (the broker assigns
 *       offsets asynchronously), which is what lets a stamp claim the sender's own
 *       just-issued sends without waiting for acknowledgements.</li>
 * </ul>
 *
 * Absent entries claim nothing. Mutable; callers copy when they need a snapshot. The
 * serialized form is versioned and sorted so equal clocks are byte-identical.
 */
public final class Clock {

    public static final long NOTHING = -1L;
    private static final byte WIRE_VERSION = 1;

    /** A sequence-claim key: one sender's sends to one channel. */
    public record SeqKey(Channel channel, UUID sender) implements Comparable<SeqKey> {
        @Override
        public int compareTo(SeqKey o) {
            int c = channel.compareTo(o.channel);
            return c != 0 ? c : sender.compareTo(o.sender);
        }
    }

    private final TreeMap<Channel, Long> entries = new TreeMap<>();
    private final TreeMap<SeqKey, Long> seqEntries = new TreeMap<>();

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
        return entries.isEmpty() && seqEntries.isEmpty();
    }

    public int width() {
        return entries.size() + seqEntries.size();
    }

    /** Raises the watermark for {@code c} to at least {@code offset}; never lowers it. */
    public void advanceTo(Channel c, long offset) {
        if (offset < 0) throw new IllegalArgumentException("offset " + offset);
        entries.merge(c, offset, Math::max);
    }

    /** Raises the sequence watermark for {@code (c, sender)}; never lowers it. */
    public void advanceSeq(Channel c, UUID sender, long seq) {
        if (seq < 0) throw new IllegalArgumentException("seq " + seq);
        seqEntries.merge(new SeqKey(c, sender), seq, Math::max);
    }

    public long getSeq(SeqKey key) {
        Long v = seqEntries.get(key);
        return v == null ? NOTHING : v;
    }

    /** Pointwise max-merge of {@code other} into this clock, both claim kinds. */
    public void mergeMax(Clock other) {
        other.entries.forEach((c, o) -> entries.merge(c, o, Math::max));
        other.seqEntries.forEach((k, s) -> seqEntries.merge(k, s, Math::max));
    }

    /**
     * True iff this clock's offset entries are pointwise {@code >=} {@code other}'s. Sequence
     * entries are excluded: their satisfaction is a per-node question (delivered sequence),
     * answered by the gate, not by clock comparison.
     */
    public boolean dominates(Clock other) {
        for (Map.Entry<Channel, Long> e : other.entries.entrySet()) {
            if (get(e.getKey()) < e.getValue()) return false;
        }
        return true;
    }

    /** A new clock containing only entries (of both kinds) whose channel is in {@code channels}. */
    public Clock restrictedTo(Set<Channel> channels) {
        Clock k = new Clock();
        entries.forEach((c, o) -> {
            if (channels.contains(c)) k.entries.put(c, o);
        });
        seqEntries.forEach((key, s) -> {
            if (channels.contains(key.channel())) k.seqEntries.put(key, s);
        });
        return k;
    }

    public void remove(Channel c) {
        entries.remove(c);
        seqEntries.keySet().removeIf(k -> k.channel().equals(c));
    }

    /** Replaces one sequence claim with an offset claim (normalisation). */
    public void normalizeSeq(SeqKey key, long offset) {
        seqEntries.remove(key);
        advanceTo(key.channel(), offset);
    }

    /** Drops one sequence claim (when a stronger claim subsumes it). */
    public void removeSeq(SeqKey key) {
        seqEntries.remove(key);
    }

    /**
     * Drops every offset entry at or below the corresponding watermark in {@code stability}.
     * Sequence entries are untouched: they are transient by design (normalised away as
     * deliveries resolve them) and carry no offset to compare against the bound.
     */
    public void truncateAtOrBelow(Clock stability) {
        entries.entrySet().removeIf(e -> e.getValue() <= stability.get(e.getKey()));
    }

    public void forEach(BiConsumer<Channel, Long> consumer) {
        entries.forEach(consumer);
    }

    public void forEachSeq(BiConsumer<SeqKey, Long> consumer) {
        seqEntries.forEach(consumer);
    }

    public Clock copy() {
        Clock k = new Clock();
        k.entries.putAll(entries);
        k.seqEntries.putAll(seqEntries);
        return k;
    }

    public byte[] serialize() {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + entries.size() * 28 + 4 + seqEntries.size() * 44);
        buf.put(WIRE_VERSION);
        buf.putInt(entries.size());
        entries.forEach((c, o) -> {
            buf.putLong(c.topicId().getMostSignificantBits());
            buf.putLong(c.topicId().getLeastSignificantBits());
            buf.putInt(c.partition());
            buf.putLong(o);
        });
        buf.putInt(seqEntries.size());
        seqEntries.forEach((k, s) -> {
            buf.putLong(k.channel().topicId().getMostSignificantBits());
            buf.putLong(k.channel().topicId().getLeastSignificantBits());
            buf.putInt(k.channel().partition());
            buf.putLong(k.sender().getMostSignificantBits());
            buf.putLong(k.sender().getLeastSignificantBits());
            buf.putLong(s);
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
            if (n < 0) throw new CorruptClockException("negative entry count");
            Clock k = new Clock();
            for (int i = 0; i < n; i++) {
                var topicId = new UUID(buf.getLong(), buf.getLong());
                int partition = buf.getInt();
                long offset = buf.getLong();
                if (partition < 0 || offset < 0) throw new CorruptClockException("negative field in clock entry");
                k.entries.put(new Channel(topicId, partition), offset);
            }
            int m = buf.getInt();
            if (m < 0 || bytes.length != 1 + 4 + n * 28 + 4 + m * 44) {
                throw new CorruptClockException("clock length mismatch: " + n + "+" + m
                        + " entries, " + bytes.length + " bytes");
            }
            for (int i = 0; i < m; i++) {
                var topicId = new UUID(buf.getLong(), buf.getLong());
                int partition = buf.getInt();
                var sender = new UUID(buf.getLong(), buf.getLong());
                long seq = buf.getLong();
                if (partition < 0 || seq < 0) throw new CorruptClockException("negative field in seq entry");
                k.seqEntries.put(new SeqKey(new Channel(topicId, partition), sender), seq);
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
        return o instanceof Clock k && entries.equals(k.entries) && seqEntries.equals(k.seqEntries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode() * 31 + seqEntries.hashCode();
    }

    @Override
    public String toString() {
        return entries + (seqEntries.isEmpty() ? "" : " seq" + seqEntries);
    }
}
