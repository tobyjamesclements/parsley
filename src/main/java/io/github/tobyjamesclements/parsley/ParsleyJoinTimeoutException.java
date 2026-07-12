package io.github.tobyjamesclements.parsley;

import java.time.Duration;

/**
 * Thrown when a task's topology-epoch join does not complete within its bounded wait (see
 * {@link ParsleyCoordination#awaitJoinCommit}). A node deployed into an already-running coordinated
 * topology must not begin consuming until an epoch computed with it — the consistent cut that fixes a
 * new logical time-0 the whole topology agrees on — commits and admits it. That wait runs on the Kafka
 * Streams {@code StreamThread} during {@code init()}, so it is bounded below {@code max.poll.interval.ms}:
 * it fails loudly here rather than silently outliving the poll deadline, being evicted, and re-running
 * {@code init()} in a rebalance crash-loop.
 *
 * <p>Reaching this exception means one of: the coordinated domain cannot currently commit an epoch (an
 * existing member is down or partitioned, so the round never completes), the epoch-events log could not
 * be folded to its end, or admission legitimately needs longer than the bound — in which case raise
 * {@code max.poll.interval.ms} so the bounded wait can span it.
 */
final class ParsleyJoinTimeoutException extends RuntimeException {

    ParsleyJoinTimeoutException(String memberId, Duration budget, String reason) {
        super("topology-epoch join for member '" + memberId + "' did not complete within " + budget
                + ": " + reason + ". The join wait is bounded below max.poll.interval.ms so it fails here "
                + "rather than letting the broker silently evict this consumer into a rebalance crash-loop. "
                + "Either the coordinated domain cannot currently commit an epoch (an existing member is "
                + "down or partitioned), or admission needs longer than the bound — raise "
                + "max.poll.interval.ms so the bounded wait can span it.");
    }
}
