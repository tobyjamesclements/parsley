package io.parsley;

/**
 * Defines what happens to a record when its {@link CausalBufferLimit} fires. Pair a limit (the
 * <em>when</em>) with one of these eviction strategies (the <em>what</em>) using the static factory
 * methods:
 *
 * <ul>
 *   <li>{@link #forwardUnsafe} — forward the evicted record downstream out-of-order and report a
 *       {@link CausalViolationReason#LIMIT_REACHED} violation
 *   <li>{@link #drop} — silently discard the record and report a violation
 *   <li>{@link #deadLetter} — route the record to a named dead-letter destination and report a
 *       violation
 * </ul>
 *
 * <h2>Strict vs lenient</h2>
 * {@link #drop} and {@link #deadLetter} are <strong>strict</strong> ({@link #strict()} is
 * {@code true}): an un-satisfiable record is diverted away rather than delivered, so the causal
 * guarantee holds for every record that <em>is</em> delivered. {@link #forwardUnsafe} is
 * <strong>lenient</strong>: it preserves delivery by forwarding un-satisfied records, which suspends
 * the guarantee for exactly those records (each flagged as a violation).
 */
public sealed interface CausalBufferPolicy permits ParsleyForwardUnsafePolicy, ParsleyDropPolicy, ParsleyDeadLetterPolicy {

    /**
     * Returns whether this policy is <strong>strict</strong> — i.e. it never forwards an
     * un-satisfiable record downstream, so the causal guarantee is preserved for every delivered
     * record. {@link #drop} and {@link #deadLetter} are strict; {@link #forwardUnsafe} is not.
     *
     * @return {@code true} for {@code drop} and {@code deadLetter}; {@code false} for
     *         {@code forwardUnsafe}
     */
    boolean strict();

    /**
     * Creates a policy that forwards evicted records out-of-causal-order with a
     * {@link CausalViolationReason#LIMIT_REACHED} violation — the lenient,
     * availability-over-consistency choice. Choose this when message loss is unacceptable and
     * occasional causal disorder is preferable to dropping data.
     *
     * @param limit the eviction trigger
     * @return a new {@code forwardUnsafe} policy
     */
    static CausalBufferPolicy forwardUnsafe(CausalBufferLimit limit) {
        return new ParsleyForwardUnsafePolicy(limit);
    }

    /**
     * Creates a policy that discards evicted records with a violation; the record is never forwarded
     * downstream. Choose this when causal ordering is strictly required and message loss is
     * acceptable.
     *
     * @param limit the eviction trigger
     * @return a new {@code drop} policy
     */
    static CausalBufferPolicy drop(CausalBufferLimit limit) {
        return new ParsleyDropPolicy(limit);
    }

    /**
     * Creates a policy that routes evicted records to {@code destination} with a violation, rather
     * than forwarding them on the primary stream.
     *
     * @param limit       the eviction trigger
     * @param destination the dead-letter destination name (e.g. a Kafka topic)
     * @return a new {@code deadLetter} policy
     */
    static CausalBufferPolicy deadLetter(CausalBufferLimit limit, String destination) {
        return new ParsleyDeadLetterPolicy(limit, destination);
    }
}
