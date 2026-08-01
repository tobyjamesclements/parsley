package io.github.tobyjamesclements.parsley;

/**
 * Stateful tick logic, a pure step function over the stage's tick state.
 *
 * <p>The tick state is one reserved per-partition slot of the stage's declared state type,
 * distinct from every per-key slot, including the null key's. It resolves to the stage's
 * initial value when unseen, and persists transactionally with the delivery, exactly as a
 * {@link Fold}'s key state. Returning {@code Step.of(null, ...)} deletes it.
 *
 * <p>A tick carries no entity key, so tick logic never sees per-key state. It holds
 * tick-to-tick memory instead, such as the last time a policy fired, counters, or rate limits.
 * Channel-scoped policy emissions need no entity state, because the tick's stamp already names
 * the consumed history they quantify over.
 *
 * <p>An exception fails the task. The transaction aborts and the retry redelivers the same
 * tick. A tick is a durable record, so a failed policy evaluation is retried, never lost.
 *
 * @param <S> the stage's tick state type, the same type as its per-key state
 */
@FunctionalInterface
public interface TickFold<S> {

    /**
     * Folds one delivered tick over the tick state.
     *
     * @param state the current tick state, the stage's initial value when unseen
     * @param tick the delivered tick
     * @return the state to persist and the emissions the tick caused
     */
    Step<S> apply(S state, Tick tick);
}
