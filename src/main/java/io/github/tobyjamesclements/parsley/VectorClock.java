package io.github.tobyjamesclements.parsley;

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
 *   <li><b>Offset entries</b>, a map from {@link Channel} to an offset watermark, claiming
 *       every offset at or below the watermark on that channel.</li>
 *   <li><b>Sequence entries</b>, a map from channel and sender to a send-sequence watermark,
 *       claiming every record that sender sent to that channel with sequence at or below the
 *       watermark. Sequence claims are known synchronously at send time, where the broker
 *       assigns offsets asynchronously, which is what lets a stamp claim the sender's own
 *       just-issued sends without waiting for acknowledgements.</li>
 * </ul>
 *
 * <p>Absent entries claim nothing. Mutable, so callers copy when they need a snapshot. The
 * serialized form is versioned and sorted so equal clocks are byte-identical.
 */
final class VectorClock {

    /** The watermark of an absent entry, which claims nothing. */
    static final long NOTHING = -1L;
    private static final byte WIRE_VERSION = 1;

    /**
     * A sequence-claim key, naming one sender's sends to one channel.
     *
     * @param channel the destination channel
     * @param sender the sending node's identity
     */
    record SeqKey(Channel channel, UUID sender) implements Comparable<SeqKey> {
        @Override
        public int compareTo(SeqKey o) {
            int c = channel.compareTo(o.channel);
            return c != 0 ? c : sender.compareTo(o.sender);
        }
    }

    private final TreeMap<Channel, Long> entries = new TreeMap<>();
    private final TreeMap<SeqKey, Long> seqEntries = new TreeMap<>();

    /** Creates an empty clock, claiming nothing. */
    VectorClock() {}

    /**
     * Returns a clock claiming one offset on one channel.
     *
     * @param c the channel to claim on
     * @param offset the offset watermark to claim
     * @return the new clock
     * @throws IllegalArgumentException if {@code offset} is negative
     */
    static VectorClock of(Channel c, long offset) {
        VectorClock k = new VectorClock();
        k.advanceTo(c, offset);
        return k;
    }

    /**
     * Returns the offset watermark claimed on {@code c}.
     *
     * @param c the channel to read
     * @return the watermark, or {@link #NOTHING} when the channel is unclaimed
     */
    long get(Channel c) {
        Long v = entries.get(c);
        return v == null ? NOTHING : v;
    }

    /**
     * Reports whether the clock carries no claim of either kind.
     *
     * @return {@code true} when both entry maps are empty
     */
    boolean isEmpty() {
        return entries.isEmpty() && seqEntries.isEmpty();
    }

    /**
     * Returns the number of offset entries.
     *
     * @return the offset-claim count
     */
    int offsetEntryCount() {
        return entries.size();
    }

    /**
     * Returns the number of sequence entries.
     *
     * @return the sequence-claim count
     */
    int sequenceEntryCount() {
        return seqEntries.size();
    }

    /**
     * Raises the watermark for {@code c} to at least {@code offset}. Never lowers it.
     *
     * @param c the channel to claim on
     * @param offset the offset watermark to raise to
     * @throws IllegalArgumentException if {@code offset} is negative
     */
    void advanceTo(Channel c, long offset) {
        if (offset < 0) throw new IllegalArgumentException("offset " + offset);
        entries.merge(c, offset, Math::max);
    }

    /**
     * Raises the sequence watermark for {@code c} and {@code sender}. Never lowers it.
     *
     * @param c the destination channel
     * @param sender the sending node's identity
     * @param seq the send-sequence watermark to raise to
     * @throws IllegalArgumentException if {@code seq} is negative
     */
    void advanceSeq(Channel c, UUID sender, long seq) {
        if (seq < 0) throw new IllegalArgumentException("seq " + seq);
        seqEntries.merge(new SeqKey(c, sender), seq, Math::max);
    }

    /**
     * Returns the send-sequence watermark claimed for {@code key}.
     *
     * @param key the channel and sender to read
     * @return the watermark, or {@link #NOTHING} when that pair is unclaimed
     */
    long getSeq(SeqKey key) {
        Long v = seqEntries.get(key);
        return v == null ? NOTHING : v;
    }

    /**
     * Pointwise max-merge of {@code other} into this clock, both claim kinds.
     *
     * @param other the clock to merge in, left unchanged
     */
    void mergeMax(VectorClock other) {
        other.entries.forEach((c, o) -> entries.merge(c, o, Math::max));
        other.seqEntries.forEach((k, s) -> seqEntries.merge(k, s, Math::max));
    }

    /**
     * Reports whether this clock's offset entries are pointwise {@code >=} {@code other}'s.
     *
     * <p>Sequence entries are excluded. Their satisfaction is a per-node question about the
     * delivered sequence, answered by the gate rather than by clock comparison.
     *
     * @param other the clock to compare against
     * @return {@code true} when every offset entry of {@code other} is matched or exceeded
     */
    boolean dominates(VectorClock other) {
        for (Map.Entry<Channel, Long> e : other.entries.entrySet()) {
            if (get(e.getKey()) < e.getValue()) return false;
        }
        return true;
    }

    /**
     * Replaces one sequence claim with an offset claim, the normalisation step.
     *
     * @param key the sequence claim to replace
     * @param offset the offset watermark to claim in its place
     */
    void normalizeSeq(SeqKey key, long offset) {
        seqEntries.remove(key);
        advanceTo(key.channel(), offset);
    }

    /**
     * Drops one sequence claim, for when a stronger claim subsumes it.
     *
     * @param key the sequence claim to drop
     */
    void removeSeq(SeqKey key) {
        seqEntries.remove(key);
    }

    /**
     * Drops every offset entry at or below the corresponding watermark in {@code stability}.
     *
     * <p>Sequence entries are untouched. They are transient by design, normalised away as
     * deliveries resolve them, and carry no offset to compare against the bound.
     *
     * @param stability the stability bound to truncate against
     */
    void truncateAtOrBelow(VectorClock stability) {
        entries.entrySet().removeIf(e -> e.getValue() <= stability.get(e.getKey()));
    }

    /**
     * Visits every offset entry, in channel order.
     *
     * @param consumer receives each channel and its offset watermark
     */
    void forEach(BiConsumer<Channel, Long> consumer) {
        entries.forEach(consumer);
    }

    /**
     * Visits every sequence entry, in channel-then-sender order.
     *
     * @param consumer receives each key and its send-sequence watermark
     */
    void forEachSeq(BiConsumer<SeqKey, Long> consumer) {
        seqEntries.forEach(consumer);
    }

    /**
     * Returns an independent copy, sharing no mutable state with this clock.
     *
     * @return the copy
     */
    VectorClock copy() {
        VectorClock k = new VectorClock();
        k.entries.putAll(entries);
        k.seqEntries.putAll(seqEntries);
        return k;
    }

    /**
     * Returns the wire form, versioned and sorted so equal clocks are byte-identical.
     *
     * @return the serialized clock
     */
    byte[] serialize() {
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
     * Reads a clock from its serialized form.
     *
     * <p>A present but undecodable clock must fail the task, never read as empty, because an
     * empty read would silently drop the sender's claims.
     *
     * @param bytes the serialized clock
     * @return the decoded clock
     * @throws CorruptClockException on any malformed input
     */
    static VectorClock deserialize(byte[] bytes) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            byte version = buf.get();
            if (version != WIRE_VERSION) throw new CorruptClockException("unknown clock version " + version);
            int n = buf.getInt();
            if (n < 0) throw new CorruptClockException("negative entry count");
            VectorClock k = new VectorClock();
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
        return o instanceof VectorClock k && entries.equals(k.entries) && seqEntries.equals(k.seqEntries);
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
