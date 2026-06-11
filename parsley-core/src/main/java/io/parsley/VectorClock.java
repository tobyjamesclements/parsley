package io.parsley;

/**
 * An opaque snapshot of causal progress.
 *
 * <p>A {@code VectorClock} encodes the set of causal events a service has observed. The
 * concrete representation — e.g. Kafka topic-partition offsets — is defined by the broker
 * module; {@code parsley-core} treats it as opaque.
 *
 * <p>Use {@link #satisfiedBy} to test whether a frontier clock has seen everything this clock
 * requires, and {@link #merge} to compute the causal union of two clocks.
 */
public interface VectorClock {

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
    boolean satisfiedBy(VectorClock frontier);

    /**
     * Returns a new clock that is the causal union of this and {@code other}.
     *
     * <p>The resulting clock dominates both operands: it has observed at least everything
     * either operand has observed.
     *
     * @param other the clock to merge with; must not be {@code null}
     * @return a new {@code VectorClock} that is the causal union
     */
    VectorClock merge(VectorClock other);
}
