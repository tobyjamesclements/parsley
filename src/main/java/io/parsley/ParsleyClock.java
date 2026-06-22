package io.parsley;

import org.apache.kafka.common.Uuid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The one causal vector clock. A clock maps each topic-partition to the highest offset recorded on
 * it, keyed by the topic's Kafka UUID so that topic deletion and recreation produce a different
 * identity even when the name is reused.
 *
 * <p>It serves two roles, distinguished only by how the engine uses it:
 * <ul>
 *   <li>as the node <em>frontier</em> — the highest offset the causal processor has admitted on each
 *       coordinate ({@link #observe} advances it);
 *   <li>as a record's <em>dependencies</em> — the positions a consumer must have observed before the
 *       record may be delivered (a write-time snapshot of some producer's frontier).
 * </ul>
 * "Satisfied" is then just clock dominance: a frontier {@link #dominates} a dependency clock when it
 * has observed at least as far on every coordinate the dependencies name.
 *
 * <p>Instances are immutable. Internally the clock keys on primitives — a nested
 * {@code Map<Uuid, Map<Integer, Long>>} (topicId → partition → offset) — so there is no coordinate
 * wrapper object and no per-lookup key allocation on the hot gating path.
 */
final class ParsleyClock {

    /** Leading byte of the wire format. */
    static final byte WIRE_VERSION = 1;

    /** A receiver for each {@code (topicId, partition, offset)} entry; see {@link #forEach}. */
    @FunctionalInterface
    interface EntryConsumer {
        void accept(Uuid topicId, int partition, long offset);
    }

    private final Map<Uuid, Map<Integer, Long>> offsets; // always deeply immutable

    private ParsleyClock(Map<Uuid, Map<Integer, Long>> offsets) {
        this.offsets = offsets;
    }

    /**
     * Returns an empty clock with no positions recorded.
     */
    static ParsleyClock empty() {
        return new ParsleyClock(Map.of());
    }

    /**
     * Returns the highest offset recorded for {@code (topicId, partition)}, or {@code -1} if the
     * coordinate has never been recorded.
     */
    long offsetFor(Uuid topicId, int partition) {
        Map<Integer, Long> byPartition = offsets.get(topicId);
        return byPartition == null ? -1L : byPartition.getOrDefault(partition, -1L);
    }

    /**
     * Returns a new clock with {@code (topicId, partition)} recorded at {@code max(current, offset)}.
     */
    ParsleyClock observe(Uuid topicId, int partition, long offset) {
        Map<Uuid, Map<Integer, Long>> next = mutableCopy();
        next.computeIfAbsent(topicId, k -> new HashMap<>()).merge(partition, offset, Math::max);
        return new ParsleyClock(freeze(next));
    }

    /**
     * Returns the causal union of this clock and {@code other}: the per-coordinate maximum.
     */
    ParsleyClock merge(ParsleyClock other) {
        Map<Uuid, Map<Integer, Long>> merged = mutableCopy();
        other.forEach((topicId, partition, offset) ->
                merged.computeIfAbsent(topicId, k -> new HashMap<>()).merge(partition, offset, Math::max));
        return new ParsleyClock(freeze(merged));
    }

    /**
     * Returns a copy of this clock with the {@code (topicId, partition)} coordinate removed, if
     * present. Used to strip a record's self-reference before the admissibility check.
     */
    ParsleyClock without(Uuid topicId, int partition) {
        if (offsetFor(topicId, partition) < 0) {
            return this;
        }
        Map<Uuid, Map<Integer, Long>> next = mutableCopy();
        // Present by the offsetFor guard above: a non-negative offset means this coordinate exists.
        Map<Integer, Long> byPartition = Objects.requireNonNull(next.get(topicId));
        byPartition.remove(partition);
        if (byPartition.isEmpty()) {
            next.remove(topicId);
        }
        return new ParsleyClock(freeze(next));
    }

    /**
     * Returns {@code true} if this clock (a frontier) has recorded at least everything {@code deps}
     * require — for every coordinate in {@code deps}, this clock's offset is ≥ the required offset.
     */
    boolean dominates(ParsleyClock deps) {
        for (Map.Entry<Uuid, Map<Integer, Long>> byTopic : deps.offsets.entrySet()) {
            Map<Integer, Long> here = offsets.get(byTopic.getKey());
            for (Map.Entry<Integer, Long> byPartition : byTopic.getValue().entrySet()) {
                long observed = here == null ? -1L : here.getOrDefault(byPartition.getKey(), -1L);
                if (observed < byPartition.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the causal gap between this clock (treated as dependencies) and {@code frontier}: for
     * every coordinate this clock requires a higher offset than {@code frontier} has observed, the
     * result records the <em>shortfall</em> ({@code required − observed}, counting an absent frontier
     * coordinate as {@code -1} so the gap is {@code required + 1}). Empty exactly when
     * {@code frontier.dominates(this)}. For diagnostics (eviction logging).
     */
    ParsleyClock missing(ParsleyClock frontier) {
        Map<Uuid, Map<Integer, Long>> gap = new HashMap<>();
        forEach((topicId, partition, required) -> {
            long observed = frontier.offsetFor(topicId, partition);
            if (observed < required) {
                gap.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition, required - observed);
            }
        });
        return new ParsleyClock(freeze(gap));
    }

    /**
     * Invokes {@code consumer} once per recorded {@code (topicId, partition, offset)} entry, in no
     * particular order.
     */
    void forEach(EntryConsumer consumer) {
        offsets.forEach((topicId, byPartition) ->
                byPartition.forEach((partition, offset) -> consumer.accept(topicId, partition, offset)));
    }

    /**
     * Returns {@code true} if this clock records no positions.
     */
    boolean isEmpty() {
        return offsets.isEmpty();
    }

    /**
     * Returns the number of {@code (topicId, partition)} coordinates recorded.
     */
    int size() {
        int count = 0;
        for (Map<Integer, Long> byPartition : offsets.values()) {
            count += byPartition.size();
        }
        return count;
    }

    /**
     * Serialises to a compact binary form.
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
     */
    byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
            dos.writeInt(size());
            for (Map.Entry<Uuid, Map<Integer, Long>> byTopic : offsets.entrySet()) {
                for (Map.Entry<Integer, Long> byPartition : byTopic.getValue().entrySet()) {
                    dos.writeLong(byTopic.getKey().getMostSignificantBits());
                    dos.writeLong(byTopic.getKey().getLeastSignificantBits());
                    dos.writeInt(byPartition.getKey());
                    dos.writeLong(byPartition.getValue());
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyClock serialisation failed", e);
        }
    }

    /**
     * Reconstructs a clock from its {@link #toBytes() serialised} form.
     *
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    static ParsleyClock fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported ParsleyClock wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            int count = dis.readInt();
            Map<Uuid, Map<Integer, Long>> map = new HashMap<>();
            for (int i = 0; i < count; i++) {
                long msb = dis.readLong();
                long lsb = dis.readLong();
                int partition = dis.readInt();
                long offset = dis.readLong();
                map.computeIfAbsent(new Uuid(msb, lsb), k -> new HashMap<>()).put(partition, offset);
            }
            return new ParsleyClock(freeze(map));
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyClock deserialisation failed", e);
        }
    }

    private Map<Uuid, Map<Integer, Long>> mutableCopy() {
        Map<Uuid, Map<Integer, Long>> copy = new HashMap<>(offsets.size());
        offsets.forEach((topicId, byPartition) -> copy.put(topicId, new HashMap<>(byPartition)));
        return copy;
    }

    private static Map<Uuid, Map<Integer, Long>> freeze(Map<Uuid, Map<Integer, Long>> mutable) {
        Map<Uuid, Map<Integer, Long>> frozen = new HashMap<>(mutable.size());
        mutable.forEach((topicId, byPartition) -> frozen.put(topicId, Map.copyOf(byPartition)));
        return Map.copyOf(frozen);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParsleyClock other)) return false;
        return offsets.equals(other.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offsets);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ParsleyClock{");
        boolean[] first = {true};
        forEach((topicId, partition, offset) -> {
            if (!first[0]) sb.append(", ");
            first[0] = false;
            sb.append(topicId).append('-').append(partition).append('@').append(offset);
        });
        return sb.append('}').toString();
    }
}
