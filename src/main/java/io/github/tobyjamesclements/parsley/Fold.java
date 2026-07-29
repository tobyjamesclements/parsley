package io.github.tobyjamesclements.parsley;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Stateful user logic: a pure step function over per-key state. The runtime resolves the
 * state for the message's key (the stage's initial value when the key is unseen), applies
 * the fold, persists the returned state, and applies the returned emissions — all
 * transactionally with the delivery. Causal order plus a pure fold makes the state a
 * deterministic function of the delivered history.
 *
 * <p>State is scoped to the stage and keyed by the message key's encoded bytes, so messages
 * from different source topics that agree on key bytes fold into the same state — the same
 * agreement co-partitioning already requires. Returning {@code Step.of(null, ...)} deletes
 * the key's state.
 *
 * <p>An exception from a fold fails the task: the transaction aborts and the retry redelivers
 * the same message in the same causal order. {@link #recovering} converts that outcome into
 * emissions for folds whose failures are part of the domain.
 */
@FunctionalInterface
public interface Fold<S, K, V> {

    Step<S> apply(S state, Message<K, V> message);

    /**
     * Decorates a fold so a {@link RuntimeException} becomes a step that keeps the prior state
     * unchanged and carries the emissions {@code onFailure} yields for it, typically to a
     * declared error sink, instead of failing the task. An {@link Error} still propagates.
     *
     * <p>The recovery commits exactly once, in the same transaction as the delivery, and is
     * never retried. Wrap only logic whose throws are deterministic in the state and message: a
     * transient failure caught here routes a good message to the error sink irrevocably, where
     * the undecorated fold's abort-and-retry would have delivered it. Leave transient
     * infrastructure failure to the default.
     */
    static <S, K, V> Fold<S, K, V> recovering(
            Fold<S, K, V> fold,
            BiFunction<Message<K, V>, RuntimeException, List<Emission>> onFailure) {
        return (state, message) -> {
            try {
                return fold.apply(state, message);
            } catch (RuntimeException e) {
                return new Step<>(state, onFailure.apply(message, e));
            }
        };
    }
}
