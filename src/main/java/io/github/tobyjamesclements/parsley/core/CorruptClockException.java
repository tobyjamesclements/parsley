package io.github.tobyjamesclements.parsley.core;

/**
 * A present but undecodable clock header. Fails the task: treating a corrupt clock as empty
 * would silently drop the sender's claims and let a downstream effect precede its cause.
 */
public class CorruptClockException extends RuntimeException {
    public CorruptClockException(String message) {
        super(message);
    }

    public CorruptClockException(String message, Throwable cause) {
        super(message, cause);
    }
}
