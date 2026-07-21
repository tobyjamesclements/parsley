package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.jspecify.annotations.Nullable;

/**
 * Base of every exception Parsley's causal machinery throws out of a processing task. Parsley is
 * fail-closed: when the causal protocol cannot proceed soundly — an undecodable clock header, a
 * poisoned buffered record, a timed-out own-output acknowledgement, a causal topic recreated
 * mid-run — the task fails rather than delivering out of order, and the exception propagates out
 * of Kafka Streams into the application's {@link StreamsUncaughtExceptionHandler} (see
 * {@link CausalStreams#setUncaughtExceptionHandler}). This hierarchy exists so that handler can
 * decide by type rather than by message text; the subtypes state whether a restart can heal the
 * condition.
 *
 * <p>Only Parsley constructs these. The concrete subtypes are final and their constructors are not
 * public.
 */
public abstract class CausalDeliveryException extends RuntimeException {

    CausalDeliveryException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
