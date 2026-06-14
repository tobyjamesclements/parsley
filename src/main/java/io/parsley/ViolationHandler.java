package io.parsley;

/**
 * Callback invoked when a decorating causal processor detects an ordering violation, handed a
 * {@link Violation} that includes the causal gap.
 *
 * <p>This is the richer counterpart to {@link CausalViolationHandler} used by
 * {@code Parsley.causal(...)}: where the strict policies produce a recoverable artifact (the
 * dead-letter destination), this makes the lenient {@code forwardUnsafe} path observable to a
 * comparable standard — the handler learns not just <em>that</em> a record was forwarded without
 * its causal premise, but <em>which</em> dependencies were missing and by how much.
 */
@FunctionalInterface
public interface ViolationHandler {

    /**
     * Called when a causal violation occurs.
     *
     * @param violation the violation, including the offending record, the reason, and the causal
     *                  gap; never {@code null}
     */
    void onViolation(Violation violation);

    /**
     * Returns a handler that silently ignores all violations.
     *
     * @return a no-op {@code ViolationHandler}
     */
    static ViolationHandler noop() {
        return violation -> {};
    }
}
