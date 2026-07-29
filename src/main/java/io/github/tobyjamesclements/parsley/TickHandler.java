package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * Stateless tick logic: a pure function from a delivered {@link Tick} to the emissions it
 * causes. The runtime invokes it once per delivered tick and applies the returned emissions
 * transactionally with the delivery, exactly as for a {@link Handler}. Return
 * {@link List#of()} to emit nothing.
 *
 * <p>An exception fails the task; the transaction aborts and the retry redelivers the same
 * tick — a tick is a durable record, so a failed policy evaluation is retried, never lost.
 */
@FunctionalInterface
public interface TickHandler {

    List<Emission> onTick(Tick tick);
}
