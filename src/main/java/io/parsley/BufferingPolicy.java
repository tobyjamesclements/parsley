package io.parsley;

/**
 * Defines what happens to a record when its {@link BufferLimit} fires.
 *
 * <p>{@code BufferingPolicy} is a sealed interface with three variants. Each variant pairs a
 * {@link BufferLimit} (the <em>when</em>) with an eviction strategy (the <em>what</em>):
 *
 * <ul>
 *   <li>{@link ForwardUnsafe} — forward the evicted record downstream out-of-order and report a
 *       {@link CausalViolationReason#LIMIT_REACHED} violation via the violation handler
 *   <li>{@link Drop} — silently discard the record and report a violation
 *   <li>{@link DeadLetter} — route the record to a named dead-letter destination and report
 *       a violation
 * </ul>
 *
 * <h2>Strict vs lenient</h2>
 * {@link Drop} and {@link DeadLetter} are <strong>strict</strong> ({@link #strict()} is
 * {@code true}): an un-satisfiable record is diverted away rather than delivered, so the causal
 * guarantee holds for every record that <em>is</em> delivered. {@link ForwardUnsafe} is
 * <strong>lenient</strong>: it preserves delivery by forwarding un-satisfied records, which
 * suspends the guarantee for exactly those records (each flagged as a violation).
 *
 * <p>Use the static factory methods to construct instances:
 * {@link #forwardUnsafe}, {@link #drop}, {@link #deadLetter}.
 */
public sealed interface BufferingPolicy permits
        BufferingPolicy.ForwardUnsafe,
        BufferingPolicy.Drop,
        BufferingPolicy.DeadLetter {

    /**
     * Returns whether this policy is <strong>strict</strong> — i.e. it never forwards an
     * un-satisfiable record downstream, so the causal guarantee is preserved for every delivered
     * record. {@link Drop} and {@link DeadLetter} are strict; {@link ForwardUnsafe} is not.
     *
     * @return {@code true} for {@link Drop} and {@link DeadLetter}; {@code false} for
     *         {@link ForwardUnsafe}
     */
    boolean strict();

    /**
     * Creates a {@link ForwardUnsafe} policy: forward evicted records out-of-order with a
     * violation, accepting availability over consistency for those records.
     *
     * @param limit the eviction trigger
     * @return a new {@code ForwardUnsafe} policy
     */
    static BufferingPolicy forwardUnsafe(BufferLimit limit) {
        return new ForwardUnsafe(limit);
    }

    /**
     * Creates a {@link Drop} policy: discard evicted records with a violation.
     *
     * @param limit the eviction trigger
     * @return a new {@code Drop} policy
     */
    static BufferingPolicy drop(BufferLimit limit) {
        return new Drop(limit);
    }

    /**
     * Creates a {@link DeadLetter} policy: route evicted records to {@code destination} with
     * a violation.
     *
     * @param limit       the eviction trigger
     * @param destination the dead-letter destination name (e.g. a Kafka topic)
     * @return a new {@code DeadLetter} policy
     */
    static BufferingPolicy deadLetter(BufferLimit limit, String destination) {
        return new DeadLetter(limit, destination);
    }

    /**
     * Forwards the evicted record downstream out-of-causal-order and reports a
     * {@link CausalViolationReason#LIMIT_REACHED} violation. This is the lenient,
     * availability-over-consistency policy: the record is delivered even though its causal premise
     * is not yet satisfied, so the causal guarantee is <strong>suspended</strong> for it.
     *
     * <p>Choose this policy when message loss is unacceptable and occasional causal disorder
     * is preferable to dropping data.
     *
     * @param limit the limit that triggers eviction
     */
    record ForwardUnsafe(BufferLimit limit) implements BufferingPolicy {
        @Override
        public boolean strict() {
            return false;
        }
    }

    /**
     * Discards the evicted record and reports a {@link CausalViolationReason#LIMIT_REACHED}
     * violation. The record is never forwarded downstream.
     *
     * <p>Choose this policy when causal ordering is strictly required and message loss is
     * acceptable.
     *
     * @param limit the limit that triggers eviction
     */
    record Drop(BufferLimit limit) implements BufferingPolicy {
        @Override
        public boolean strict() {
            return true;
        }
    }

    /**
     * Routes the evicted record to {@code destination} (the dead-letter destination) and
     * reports a {@link CausalViolationReason#LIMIT_REACHED} violation. The record is not
     * forwarded on the primary stream.
     *
     * @param limit       the limit that triggers eviction
     * @param destination the destination name to route evicted records to (e.g. a Kafka topic)
     */
    record DeadLetter(BufferLimit limit, String destination) implements BufferingPolicy {
        @Override
        public boolean strict() {
            return true;
        }
    }
}
