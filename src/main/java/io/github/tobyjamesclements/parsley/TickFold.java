package io.github.tobyjamesclements.parsley;

/**
 * Stateful tick logic: a pure step function over the stage's tick state. The tick state is
 * one reserved per-partition slot of the stage's declared state type — distinct from every
 * per-key slot, including the null key's — resolved to the stage's initial value when unseen,
 * and persisted transactionally with the delivery, exactly as a {@link Fold}'s key state.
 * Returning {@code Step.of(null, ...)} deletes it.
 *
 * <p>A tick carries no entity key, so tick logic never sees per-key state; it holds
 * tick-to-tick memory instead — the last time a policy fired, counters, rate limits.
 * Channel-scoped policy emissions need no entity state: the tick's stamp already names the
 * consumed history they quantify over.
 *
 * <p>An exception fails the task; the transaction aborts and the retry redelivers the same
 * tick — a tick is a durable record, so a failed policy evaluation is retried, never lost.
 */
@FunctionalInterface
public interface TickFold<S> {

    Step<S> apply(S state, Tick tick);
}
