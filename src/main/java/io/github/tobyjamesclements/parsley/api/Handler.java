package io.github.tobyjamesclements.parsley.api;

/**
 * The seam through which the implementation invokes application logic (SPEC Structural 3): the delivered message and
 * the process's application state in; the messages to send and the state to persist out, as the returned
 * {@link Effects}. Nothing else crosses — no context, no producer, no clock, no timers. Logic should be a pure
 * function of its two arguments (SPEC Assumption 16): the host may re-invoke it for the same delivery after an
 * aborted step, and only the committed invocation's effects ever happen.
 */
@FunctionalInterface
public interface Handler<K, V> {

    Effects handle(Delivery<K, V> delivery, StateReader state);
}
