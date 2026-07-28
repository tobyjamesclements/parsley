package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * Stateless user logic: a pure function from a delivered message to the emissions it causes.
 * The runtime invokes it once per message, in causal order, and applies the returned
 * emissions transactionally with the delivery. Return {@link List#of()} to emit nothing.
 *
 * <p>Purity is the contract that makes the causal guarantee meaningful: a handler's effects
 * must flow through its return value, not through shared mutable state or external calls
 * (the closed-effects precondition, {@code docs/foundations/causal-model.md}).
 */
@FunctionalInterface
public interface Handler<K, V> {

    List<Emission> handle(Message<K, V> message);
}
