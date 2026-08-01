package io.github.tobyjamesclements.parsley;

/**
 * A present but undecodable clock header.
 *
 * <p>Fails the task. Treating a corrupt clock as empty would silently drop the sender's claims
 * and let a downstream effect precede its cause.
 */
public final class CorruptClockException extends RuntimeException {

    /**
     * Creates an exception describing the malformed header.
     *
     * @param message what was malformed
     */
    CorruptClockException(String message) {
        super(message);
    }

    /**
     * Creates an exception describing the malformed header, wrapping the decode failure.
     *
     * @param message what was malformed
     * @param cause the underlying decode failure
     */
    CorruptClockException(String message, Throwable cause) {
        super(message, cause);
    }
}
