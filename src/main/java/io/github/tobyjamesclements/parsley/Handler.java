package io.github.tobyjamesclements.parsley;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Stateless user logic: a pure function from a delivered message to the emissions it causes.
 * The runtime invokes it once per message, in causal order, and applies the returned
 * emissions transactionally with the delivery. Return {@link List#of()} to emit nothing.
 *
 * <p>Purity is the contract that makes the causal guarantee meaningful: a handler's effects
 * must flow through its return value, not through shared mutable state or external calls
 * (the closed-effects precondition, {@code docs/foundations/causal-model.md}).
 *
 * <p>An exception from a handler fails the task: the transaction aborts and the retry
 * redelivers the same message in the same causal order. {@link #recovering} converts that
 * outcome into emissions for handlers whose failures are part of the domain.
 */
@FunctionalInterface
public interface Handler<K, V> {

    List<Emission> handle(Message<K, V> message);

    /**
     * Decorates a handler so a {@link RuntimeException} becomes the emissions {@code onFailure}
     * yields for it, typically to a declared error sink, instead of failing the task. An
     * {@link Error} still propagates.
     *
     * <p>The recovery commits exactly once, in the same transaction as the delivery, and is
     * never retried. Wrap only logic whose throws are deterministic in the message: a transient
     * failure caught here routes a good message to the error sink irrevocably, where the
     * undecorated handler's abort-and-retry would have delivered it. Leave transient
     * infrastructure failure to the default.
     */
    static <K, V> Handler<K, V> recovering(
            Handler<K, V> handler,
            BiFunction<Message<K, V>, RuntimeException, List<Emission>> onFailure) {
        return message -> {
            try {
                return handler.handle(message);
            } catch (RuntimeException e) {
                return onFailure.apply(message, e);
            }
        };
    }
}
