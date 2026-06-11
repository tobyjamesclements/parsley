package io.parsley;

import java.util.Map;

/**
 * A snapshot of causal progress across a set of Kafka partitions.
 *
 * <p>A {@code VectorClock} maps each {@link Partition} to the highest message offset the holder
 * has observed from that partition. A service that has consumed messages up to these offsets is
 * said to be causally "caught up" with this clock.
 *
 * <p>Clocks are compared using {@link #dominates} / {@link #dominatedBy}: clock A <em>dominates</em>
 * clock B when A has seen at least everything B has seen (i.e. A is at least as causally advanced).
 * This is the predicate used to decide whether a buffered record's dependencies are satisfied.
 */
public interface VectorClock {

    /**
     * Returns the offset map: for each partition, the highest observed offset.
     *
     * @return an unmodifiable (or effectively unmodifiable) view of the clock positions
     */
    Map<Partition, Long> positions();

    /**
     * Returns {@code true} if this clock is at least as causally advanced as {@code other}.
     *
     * <p>Formally: for every partition in {@code other}, this clock's offset for that partition
     * must be greater than or equal to {@code other}'s offset. Partitions absent from this clock
     * are treated as offset {@code -1}.
     *
     * @param other the clock to compare against
     * @return {@code true} if this clock dominates (or equals) {@code other}
     */
    default boolean dominates(VectorClock other) {
        for (Map.Entry<Partition, Long> entry : other.positions().entrySet()) {
            if (positions().getOrDefault(entry.getKey(), -1L) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if {@code other} dominates this clock.
     *
     * <p>Convenience inverse of {@link #dominates}: {@code a.dominatedBy(b)} is equivalent to
     * {@code b.dominates(a)}.
     *
     * @param other the clock to compare against
     * @return {@code true} if {@code other} is at least as causally advanced as this clock
     */
    default boolean dominatedBy(VectorClock other) {
        return other.dominates(this);
    }
}
