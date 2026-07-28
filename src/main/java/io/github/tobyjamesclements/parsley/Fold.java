package io.github.tobyjamesclements.parsley;

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
 */
@FunctionalInterface
public interface Fold<S, K, V> {

    Step<S> apply(S state, Message<K, V> message);
}
