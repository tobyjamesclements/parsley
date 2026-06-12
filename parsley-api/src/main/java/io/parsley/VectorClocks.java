package io.parsley;

import java.util.HashMap;
import java.util.Map;

/**
 * The standard comparison and merge algebra for map-based {@link VectorClock} implementations.
 *
 * <p>A broker module typically represents its clock as an immutable map from a partition
 * identifier to a {@link Comparable} position — e.g. {@code Map<TopicPartition, Long>} for
 * Kafka, or shard-id to sequence number for Kinesis — and delegates its
 * {@link VectorClock#satisfiedBy satisfiedBy}/{@link VectorClock#merge merge} implementations
 * to these helpers.
 */
public final class VectorClocks {

    private VectorClocks() {}

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything
     * {@code required} demands: for every partition in {@code required}, the frontier
     * contains that partition at an equal or later position. A partition absent from the
     * frontier is unsatisfied.
     *
     * @param <P>      the partition identifier type
     * @param <O>      the position type
     * @param required the positions that must have been observed
     * @param frontier the positions actually observed
     * @return {@code true} if every required position is covered by the frontier
     */
    public static <P, O extends Comparable<? super O>> boolean satisfied(
            Map<P, O> required, Map<P, O> frontier) {
        for (Map.Entry<P, O> entry : required.entrySet()) {
            O observed = frontier.get(entry.getKey());
            if (observed == null || observed.compareTo(entry.getValue()) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the causal union of {@code a} and {@code b}: the per-partition maximum of the
     * two position maps.
     *
     * @param <P> the partition identifier type
     * @param <O> the position type
     * @param a   one position map
     * @param b   the other position map
     * @return an immutable map dominating both operands
     */
    public static <P, O extends Comparable<? super O>> Map<P, O> merge(Map<P, O> a, Map<P, O> b) {
        Map<P, O> merged = new HashMap<>(a);
        for (Map.Entry<P, O> entry : b.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(),
                    (x, y) -> x.compareTo(y) >= 0 ? x : y);
        }
        return Map.copyOf(merged);
    }

    /**
     * Returns {@code positions} with {@code partition} advanced to the later of its current
     * position and {@code position}.
     *
     * @param <P>       the partition identifier type
     * @param <O>       the position type
     * @param positions the current position map
     * @param partition the partition to advance
     * @param position  the newly observed position
     * @return an immutable map with the partition advanced
     */
    public static <P, O extends Comparable<? super O>> Map<P, O> advance(
            Map<P, O> positions, P partition, O position) {
        Map<P, O> advanced = new HashMap<>(positions);
        advanced.merge(partition, position, (x, y) -> x.compareTo(y) >= 0 ? x : y);
        return Map.copyOf(advanced);
    }
}
