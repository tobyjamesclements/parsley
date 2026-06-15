package io.parsley;

/**
 * Callback invoked when a decorating causal processor detects an ordering violation, handed a
 * {@link CausalViolation} that includes the causal gap.
 *
 * <p>Used throughout Parsley (the engine, {@code CausalProcessors}, and the consumer): where the
 * strict policies produce a recoverable artifact (the dead-letter destination), this makes the
 * lenient {@code forwardUnsafe} path observable to a comparable standard — the handler learns not
 * just <em>that</em> a record was forwarded without its causal premise, but <em>which</em>
 * dependencies were missing and by how much (via {@link CausalViolation#gap()}).
 */
@FunctionalInterface
public interface CausalViolationHandler {

    /**
     * Called when a causal violation occurs.
     *
     * @param violation the violation, including the offending record, the reason, and the causal
     *                  gap; never {@code null}
     */
    void onViolation(CausalViolation violation);

    /**
     * Returns a handler that throws a {@link CausalViolationException} on every violation.
     *
     * <p>Use when any causal disorder should be treated as a fatal error.
     *
     * @return a throwing {@code CausalViolationHandler}
     */
    static CausalViolationHandler throwing() {
        return violation -> { throw new CausalViolationException(violation.record(), violation.reason()); };
    }
}
