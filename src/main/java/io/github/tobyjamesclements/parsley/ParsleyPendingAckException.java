package io.github.tobyjamesclements.parsley;

/**
 * Thrown by the crossing-wait primitive ({@link ParsleyOwnOutputRegistry#awaitQuiescentExcept})
 * when a pending own-output acknowledgement times out, fails, or the wait is interrupted. The
 * throw is the point (T3.0 A8's implementation invariant): a stamp taken while an own-output send
 * is unacknowledged could under-claim this node's own coordinates, so the wait must never resolve
 * by proceeding — propagating this exception out of {@code process()} is what makes the EOS
 * transaction abort, and the unstamped forward dies with it.
 */
final class ParsleyPendingAckException extends IllegalStateException {

    ParsleyPendingAckException(String message) {
        super(message);
    }

    ParsleyPendingAckException(String message, Throwable cause) {
        super(message, cause);
    }
}
