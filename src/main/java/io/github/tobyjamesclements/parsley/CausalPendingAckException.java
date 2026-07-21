package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by the crossing-wait primitive when a pending own-output acknowledgement times out,
 * fails, or the wait is interrupted. The throw is the point: a stamp taken while an own-output
 * send is unacknowledged could under-claim this node's own coordinates, so the wait must never
 * resolve by proceeding — propagating this exception out of {@code process()} is what makes the
 * EOS transaction abort, and the unstamped forward dies with it.
 *
 * <p>Unlike {@link CausalTopicRecreatedException}, the condition is usually transient (a slow or
 * failed produce, bounded by the producer's {@code delivery.timeout.ms}); the aborted transaction
 * is retried from its consumed offsets after a thread replacement or restart.
 */
public final class CausalPendingAckException extends CausalDeliveryException {

    CausalPendingAckException(String message) {
        super(message, null);
    }

    CausalPendingAckException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
