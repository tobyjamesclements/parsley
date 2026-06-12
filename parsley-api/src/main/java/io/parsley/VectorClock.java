package io.parsley;

/**
 * An opaque snapshot of causal progress.
 *
 * <p>A {@code VectorClock} encodes the set of causal events a service has observed. The
 * concrete representation — e.g. topic-partition offsets for Kafka, shard sequence numbers
 * for Kinesis — is defined by the broker module; the core engine treats it as opaque.
 *
 * <p>The interface is self-typed: a concrete clock declares
 * {@code record MyClock(...) implements VectorClock<MyClock>}, so {@link #satisfiedBy} and
 * {@link #merge} only ever accept clocks of the same concrete type. The {@link VectorClocks}
 * helpers provide the standard per-partition comparison and merge algebra for map-based
 * implementations.
 *
 * <p>Use {@link #satisfiedBy} to test whether a frontier clock has seen everything this clock
 * requires, and {@link #merge} to compute the causal union of two clocks.
 *
 * @param <T> the concrete clock type (self type)
 */
public interface VectorClock<T extends VectorClock<T>> {

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything this clock
     * requires.
     *
     * <p>This is the core predicate used by the causal buffer: a buffered record is released
     * when its embedded clock is satisfied by the consumer's current frontier.
     *
     * @param frontier the current causal frontier to test against; must not be {@code null}
     * @return {@code true} if the frontier has caught up with this clock's requirements
     */
    boolean satisfiedBy(T frontier);

    /**
     * Returns a new clock that is the causal union of this and {@code other}.
     *
     * <p>The resulting clock dominates both operands: it has observed at least everything
     * either operand has observed.
     *
     * @param other the clock to merge with; must not be {@code null}
     * @return a new clock that is the causal union
     */
    T merge(T other);
}
