package io.parsley;

import org.apache.kafka.common.Uuid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The engine/consumer horizon: the highest offset the causal processor has admitted on each
 * topic-partition, keyed by the topic's Kafka UUID so that topic deletion and recreation produce a
 * different identity even when the name is reused.
 *
 * <p>Instances are immutable. {@link #observe} and {@link #merge} return new frontiers.
 *
 * <p>For paths without a real AdminClient UUID, derive one via {@link CausalPosition#deriveUuid}
 * and call {@link #observe(CausalPosition)} directly.
 *
 * <h2>Converting to dependencies</h2>
 * Call {@link #toDependencies()} to turn the frontier into a {@link CausalDependencies} that can be
 * stamped onto a produced record or handed to another service. This is intentionally explicit — it
 * makes the "I depend on everything I have seen" trade-off visible at the call site. For a single
 * consumed record's causal context, prefer {@link CausalDependencies#fromRecord} instead.
 */
public final class CausalFrontier {

    /** Leading byte of the wire format; shared with {@link CausalDependencies}. */
    static final byte WIRE_VERSION = 1;

    private record Key(Uuid topicId, int partition) {}

    private final Map<Key, Long> offsetFor; // always immutable

    private CausalFrontier(Map<Key, Long> offsetFor) {
        this.offsetFor = offsetFor; // already immutable (Map.copyOf called by callers)
    }

    /**
     * Returns an empty frontier with no positions recorded.
     *
     * @return an empty {@code CausalFrontier}
     */
    public static CausalFrontier empty() {
        return new CausalFrontier(Map.of());
    }

    /**
     * Returns a new frontier with {@code pos} recorded at {@code max(current, pos.offset())}.
     *
     * @param pos the observed position; must not be {@code null}
     * @return a new {@code CausalFrontier} with the updated position
     */
    public CausalFrontier observe(CausalPosition pos) {
        Map<Key, Long> next = new HashMap<>(offsetFor);
        next.merge(new Key(pos.topicId(), pos.partition()), pos.offset(), Math::max);
        return new CausalFrontier(Map.copyOf(next));
    }

    /**
     * Returns the causal union of this frontier and {@code other}: the per-position maximum.
     *
     * @param other the frontier to merge with; must not be {@code null}
     * @return a new {@code CausalFrontier} dominating both operands
     */
    public CausalFrontier merge(CausalFrontier other) {
        Map<Key, Long> merged = new HashMap<>(offsetFor);
        other.offsetFor.forEach((k, v) -> merged.merge(k, v, Math::max));
        return new CausalFrontier(Map.copyOf(merged));
    }

    /**
     * Converts this frontier to a {@link CausalDependencies} that asserts "I require at least
     * everything I have seen." Use this to propagate the full consumer horizon as a causal
     * token; prefer {@link CausalDependencies#fromRecord} when only one record's context matters.
     *
     * @return a {@code CausalDependencies} equivalent to this frontier
     */
    public CausalDependencies toDependencies() {
        CausalDependencies.Builder builder = CausalDependencies.builder();
        for (CausalPosition pos : positions()) {
            builder.require(pos);
        }
        return builder.build();
    }

    /**
     * Returns the current positions as an unordered list of {@link CausalPosition}s.
     *
     * @return the positions; never {@code null}
     */
    public List<CausalPosition> positions() {
        List<CausalPosition> list = new ArrayList<>(offsetFor.size());
        offsetFor.forEach((k, v) -> list.add(new CausalPosition(k.topicId(), k.partition(), v)));
        return list;
    }

    /**
     * Returns the highest observed offset for {@code (topicId, partition)}, or {@code -1} if the
     * position has never been observed. Package-private; used by
     * {@link CausalDependencies#isSatisfiedBy}.
     */
    long offsetFor(Uuid topicId, int partition) {
        return offsetFor.getOrDefault(new Key(topicId, partition), -1L);
    }

    /**
     * Serialises this frontier to a compact binary form compatible with
     * {@link CausalDependencies#toBytes()}.
     *
     * <h2>Wire format</h2>
     * <pre>
     * [byte  version=0x01]
     * [int   count]
     * for each entry:
     *   [long  topicId.mostSignificantBits]
     *   [long  topicId.leastSignificantBits]
     *   [int   partition]
     *   [long  offset]
     * </pre>
     *
     * @return the serialised frontier
     */
    public byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
            dos.writeInt(offsetFor.size());
            for (Map.Entry<Key, Long> e : offsetFor.entrySet()) {
                dos.writeLong(e.getKey().topicId().getMostSignificantBits());
                dos.writeLong(e.getKey().topicId().getLeastSignificantBits());
                dos.writeInt(e.getKey().partition());
                dos.writeLong(e.getValue());
            }
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("CausalFrontier serialisation failed", ex);
        }
    }

    /**
     * Reconstructs a frontier from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised frontier; must not be {@code null}
     * @return the deserialised {@code CausalFrontier}
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    public static CausalFrontier fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported CausalFrontier wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            int count = dis.readInt();
            Map<Key, Long> map = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                long msb = dis.readLong();
                long lsb = dis.readLong();
                int partition = dis.readInt();
                long offset = dis.readLong();
                map.put(new Key(new Uuid(msb, lsb), partition), offset);
            }
            return new CausalFrontier(Map.copyOf(map));
        } catch (IOException ex) {
            throw new IllegalStateException("CausalFrontier deserialisation failed", ex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalFrontier other)) return false;
        return offsetFor.equals(other.offsetFor);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offsetFor);
    }

    @Override
    public String toString() {
        return "CausalFrontier" + positions();
    }
}
