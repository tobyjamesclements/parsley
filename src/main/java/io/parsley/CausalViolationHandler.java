package io.parsley;

/**
 * Callback invoked when a decorating causal processor evicts a record from its buffer before the
 * record's causal dependencies were satisfied, handed a {@link CausalViolation} that includes the
 * causal gap.
 *
 * <p>Used throughout Parsley (the engine, {@code CausalProcessors}, and the consumer) to make the
 * always-forward, never-drop delivery model observable: the handler learns not just <em>that</em> a
 * record was forwarded without its causal premise, but <em>which</em> dependencies were missing
 * and by how much (via {@link CausalViolation#gap()}).
 */
@FunctionalInterface
public interface CausalViolationHandler {

    /**
     * Called when a record is evicted from the causal buffer before its dependencies were
     * satisfied.
     *
     * @param violation the violation, including the offending record and the causal gap; never
     *                  {@code null}
     */
    void onViolation(CausalViolation violation);

    /**
     * Returns a handler that throws a {@link CausalViolationException} on every violation.
     *
     * <p>Use when any causal disorder should be treated as a fatal error.
     *
     * @return a throwOnViolation {@code CausalViolationHandler}
     */
    static CausalViolationHandler throwOnViolation() {
        return violation -> { throw new CausalViolationException(violation.record()); };
    }
}
