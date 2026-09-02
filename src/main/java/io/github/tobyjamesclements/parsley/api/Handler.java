package io.github.tobyjamesclements.parsley.api;

/**
 * Application logic for one channel of one process.
 *
 * <p>A handler receives the delivered message and a read view of application state, and
 * returns everything it wishes to change. It is given no producer, no timer and no clock, so
 * a handler cannot emit outside the transaction that commits its state.
 *
 * <p>Implementations must be pure functions of their arguments. The runtime may invoke a
 * handler again for the same message after a failure, and the effects must be identical.
 *
 * <p>A handler that throws fails its step: the process stops, and on restart it is fed the
 * same message and fails again. Parsley never skips a message. To continue past an
 * application failure, catch it and return effects that record it deterministically — for
 * example an emission to a declared dead-letter channel.
 *
 * @param <K> delivered key type
 * @param <V> delivered value type
 * @see Effects
 */
@FunctionalInterface
public interface Handler<K, V> {
    /**
     * Computes the effects of one delivery.
     *
     * @param delivery the message, already established as causally deliverable
     * @param state    read access to the stores this process declared
     * @return the state changes and sends to commit with this step, never {@code null}
     */
    Effects handle(Delivery<K, V> delivery, StateReader state);
}
