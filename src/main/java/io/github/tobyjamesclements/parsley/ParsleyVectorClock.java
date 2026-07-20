package io.github.tobyjamesclements.parsley;

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
 * The one causal vector clock (Fidge 1988; Mattern 1988, "Virtual Time and Global States of
 * Distributed Systems") — a stated variant of the classical structure: indexed by <em>channel</em>
 * (topic-partition) rather than by process, with Kafka's broker-assigned offsets as the
 * per-component counters. A clock maps each topic-partition to the highest offset recorded on
 * it, keyed by the topic's Kafka UUID so that topic deletion and recreation produce a different
 * identity even when the name is reused.
 *
 * <p>It serves two roles, distinguished only by how the causal-broadcast core uses it:
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
final class ParsleyVectorClock {

    /** Leading byte of the wire format. */
    static final byte WIRE_VERSION = 1;

    /** A receiver for each {@code (topicId, partition, offset)} entry; see {@link #forEach}. */
    @FunctionalInterface
    interface EntryConsumer {
        void accept(Uuid topicId, int partition, long offset);
    }

    /** A predicate over a {@code (topicId, partition)} coordinate; see {@link #retaining}. */
    @FunctionalInterface
    interface CoordinatePredicate {
        boolean test(Uuid topicId, int partition);
    }

    private final Map<Uuid, Map<Integer, Long>> offsets; // always deeply immutable

    private ParsleyVectorClock(Map<Uuid, Map<Integer, Long>> offsets) {
        this.offsets = offsets;
    }

    /**
     * Returns an empty clock with no positions recorded.
     */
    static ParsleyVectorClock empty() {
        return new ParsleyVectorClock(Map.of());
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
    ParsleyVectorClock observe(Uuid topicId, int partition, long offset) {
        Map<Uuid, Map<Integer, Long>> next = mutableCopy();
        next.computeIfAbsent(topicId, k -> new HashMap<>()).merge(partition, offset, Math::max);
        return new ParsleyVectorClock(freeze(next));
    }

    /**
     * Returns the causal union of this clock and {@code other}: the per-coordinate maximum — the
     * lattice <em>join</em>, the same component-wise-max merge CRDTs name {@code merge}.
     */
    ParsleyVectorClock merge(ParsleyVectorClock other) {
        Map<Uuid, Map<Integer, Long>> merged = mutableCopy();
        other.forEach((topicId, partition, offset) ->
                merged.computeIfAbsent(topicId, k -> new HashMap<>()).merge(partition, offset, Math::max));
        return new ParsleyVectorClock(freeze(merged));
    }

    /**
     * Returns the per-coordinate minimum of this clock and {@code other}, treating an absent
     * coordinate as unknown rather than zero: a coordinate present in just one clock is kept at its
     * value (the absent side does not constrain it, and never drags it to zero); a coordinate present
     * in both takes {@code Math.min}. Used to fold a topology-epoch floor
     * across the running members' published completeness clocks ({@code ParsleyEpochLog}) — the
     * committed floor is bounded by the slowest member.
     */
    ParsleyVectorClock mergeMin(ParsleyVectorClock other) {
        Map<Uuid, Map<Integer, Long>> merged = mutableCopy();
        other.forEach((topicId, partition, offset) ->
                merged.computeIfAbsent(topicId, k -> new HashMap<>()).merge(partition, offset, Math::min));
        return new ParsleyVectorClock(freeze(merged));
    }

    /**
     * Returns a copy of this clock with the {@code (topicId, partition)} coordinate removed, if
     * present. Used to strip a record's self-reference before the admissibility check.
     */
    ParsleyVectorClock without(Uuid topicId, int partition) {
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
        return new ParsleyVectorClock(freeze(next));
    }

    /**
     * Returns a copy of this clock keeping only the coordinates for which {@code inScope} holds, or
     * {@code this} unchanged when every coordinate is kept (so the common all-in-scope case allocates
     * nothing). Two callers, both view-only: recorded state a processor owns — pruning a restored
     * frontier clock down to the coordinates currently in scope after a topic UUID change or scope
     * narrowing ({@link ParsleyChannels#rescope}, which re-homes what it prunes into the
     * carried-ancestry clock rather than dropping it) — and the gate's view of an inbound
     * dependency clock ({@code ParsleyCausalBroadcast#consumedDependencies}), whose restriction to
     * consumed coordinates <em>is</em> the D1 ignore branch: sound by I2 + I9 (a consumed causal
     * ancestor is always claimed directly in the same clock), counted by the out-of-scope-ignored
     * metric, and never applied to the clock that is folded into channel state or the outbound
     * stamp (I9: the gate may ignore; the merge may not).
     */
    ParsleyVectorClock retaining(CoordinatePredicate inScope) {
        boolean anyDropped = false;
        outer:
        for (Map.Entry<Uuid, Map<Integer, Long>> byTopic : offsets.entrySet()) {
            for (int partition : byTopic.getValue().keySet()) {
                if (!inScope.test(byTopic.getKey(), partition)) {
                    anyDropped = true;
                    break outer;
                }
            }
        }
        if (!anyDropped) {
            return this;
        }
        Map<Uuid, Map<Integer, Long>> kept = new HashMap<>();
        offsets.forEach((topicId, byPartition) -> byPartition.forEach((partition, offset) -> {
            if (inScope.test(topicId, partition)) {
                kept.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition, offset);
            }
        }));
        return new ParsleyVectorClock(freeze(kept));
    }

    /**
     * Returns {@code true} if this clock (a frontier) has recorded at least everything {@code deps}
     * require — for every coordinate in {@code deps}, this clock's offset is ≥ the required offset.
     * This is vector-clock <em>dominance</em>, the component-wise ≤ order of the literature.
     */
    boolean dominates(ParsleyVectorClock deps) {
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
                    ParsleyByteUtils.writeUuid(dos, byTopic.getKey());
                    dos.writeInt(byPartition.getKey());
                    dos.writeLong(byPartition.getValue());
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyVectorClock serialisation failed", e);
        }
    }

    /**
     * Reconstructs a clock from its {@link #toBytes() serialised} form.
     *
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    static ParsleyVectorClock fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported ParsleyVectorClock wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            int count = dis.readInt();
            Map<Uuid, Map<Integer, Long>> map = new HashMap<>();
            for (int i = 0; i < count; i++) {
                Uuid topicId = ParsleyByteUtils.readUuid(dis);
                int partition = dis.readInt();
                long offset = dis.readLong();
                map.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition, offset);
            }
            return new ParsleyVectorClock(freeze(map));
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyVectorClock deserialisation failed", e);
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
        if (!(o instanceof ParsleyVectorClock other)) return false;
        return offsets.equals(other.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offsets);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ParsleyVectorClock{");
        boolean[] first = {true};
        forEach((topicId, partition, offset) -> {
            if (!first[0]) sb.append(", ");
            first[0] = false;
            sb.append(topicId).append('-').append(partition).append('@').append(offset);
        });
        return sb.append('}').toString();
    }
}
