package io.github.tobyjamesclements.parsley.core;

/** A send whose acknowledgement failed or timed out while a stamp needed its offset. */
public class CausalSendException extends RuntimeException {
    public CausalSendException(String message) {
        super(message);
    }

    public CausalSendException(String message, Throwable cause) {
        super(message, cause);
    }
}
