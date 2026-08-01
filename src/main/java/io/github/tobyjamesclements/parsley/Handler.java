package io.github.tobyjamesclements.parsley;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Stateless user logic, a pure function from a delivered message to the emissions it causes.
 *
 * <p>The runtime invokes it once per message, in causal order, and applies the returned
 * emissions transactionally with the delivery. Return {@link List#of()} to emit nothing.
 *
 * <p>A handler's effects must flow through its return value, not through shared mutable state
 * or external calls (the closed-effects precondition). The causal guarantee is meaningful only
 * for handlers that meet it.
 *
 * <p>An exception from a handler fails the task. The transaction aborts and the retry
 * redelivers the same message in the same causal order. {@link #recovering} converts that
 * outcome into emissions for handlers whose failures are part of the domain.
 *
 * @param <K> the key type of the messages it handles
 * @param <V> the value type of the messages it handles
 */
@FunctionalInterface
public interface Handler<K, V> {

    /**
     * Handles one delivered message.
     *
     * @param message the delivered message
     * @return the emissions it causes, possibly empty
     */
    List<Emission> handle(Message<K, V> message);

    /**
     * Decorates a handler so a {@link RuntimeException} becomes the emissions {@code onFailure}
     * yields for it, instead of failing the task. An {@link Error} still propagates.
     *
     * <p>The recovery commits exactly once, in the same transaction as the delivery, and is
     * never retried. Wrap only logic whose throws are deterministic in the message. A
     * transient failure caught here routes a good message to the error sink irrevocably, where
     * the undecorated handler's abort-and-retry would have delivered it. Leave transient
     * infrastructure failure to the default.
     *
     * @param <K> the key type of the messages it handles
     * @param <V> the value type of the messages it handles
     * @param handler the handler to decorate
     * @param onFailure the emissions to apply for a message and the exception it raised,
     *         typically routing to a declared error sink
     * @return the decorated handler
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
