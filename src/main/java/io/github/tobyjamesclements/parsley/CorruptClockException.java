package io.github.tobyjamesclements.parsley;

/**
 * A present but undecodable clock header. Fails the task: treating a corrupt clock as empty
 * would silently drop the sender's claims and let a downstream effect precede its cause.
 */
public final class CorruptClockException extends RuntimeException {
    CorruptClockException(String message) {
        super(message);
    }

    CorruptClockException(String message, Throwable cause) {
        super(message, cause);
    }
}
