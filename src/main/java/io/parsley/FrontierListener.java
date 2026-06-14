package io.parsley;

/**
 * Callback invoked when a decorating causal processor advances its causal frontier, handed the new
 * {@linkplain VectorClock frontier} — the highest offset the processor has admitted on each
 * partition so far.
 *
 * <p>This is the public way to observe causal progress out of a {@code CausalProcessorSupplier}
 * without reaching into its internal state stores; it is, for example, how {@link CausalConsumer}
 * exposes {@link CausalConsumer#frontier()}.
 *
 * <p>The listener is invoked:
 * <ul>
 *   <li>once when the processor starts, with the frontier restored from the previous run (the empty
 *       clock on a cold start), so an observer's view is correct before the first new record; and
 *   <li>after every subsequent advance, <em>after</em> the new frontier has been persisted (so the
 *       persist-frontier-before-forward invariant still holds).
 * </ul>
 *
 * <p>The frontier passed is always the <strong>absolute</strong> frontier, not a delta. A single
 * supplier may back several processor tasks (one per assigned partition), each with its own frontier
 * over its own partitions, and they may run on different Streams threads — so an implementation that
 * aggregates across tasks must be <strong>thread-safe</strong> and should
 * {@linkplain VectorClock#merge merge} rather than overwrite.
 */
@FunctionalInterface
public interface FrontierListener {

    /**
     * Called when the causal frontier advances (and once at startup with the restored frontier).
     *
     * @param frontier the new absolute frontier; never {@code null}
     */
    void onFrontierAdvanced(VectorClock frontier);

    /**
     * Returns a listener that ignores all frontier advances.
     *
     * @return a no-op {@code FrontierListener}
     */
    static FrontierListener noop() {
        return frontier -> {};
    }
}
