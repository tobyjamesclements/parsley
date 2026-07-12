package io.github.tobyjamesclements.parsley;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which of a registered set of epoch members are currently drained (causal buffer empty), so
 * {@link ParsleyCoordination#leave()} can wait until every local member is drained before removing it
 * from the epoch domain — "only a drained node is excluded". Structurally the drain half of
 * {@link ParsleyQuiesce}, but without the graceful-shutdown arming: an epoch leave always cares about
 * drain state, never gated on an external request, so there is nothing to request and
 * {@link #allDrained()} is simply "every registered member is currently drained".
 *
 * <p>A member keeps processing normally throughout; it only reports itself drained once its causal
 * buffer has emptied through the ordinary delivery path (a held record's dependencies becoming
 * satisfied by a later message), so no synthetic completeness is ever fabricated to force it.
 *
 * <p><strong>Thread-safety:</strong> safe to update from every member's own Kafka Streams thread
 * (via {@link ParsleyEpochRuntime#reportDrained}) and to poll from the caller's {@code leave()} thread.
 */
final class ParsleyQuiesceTracker {

    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private final Set<String> drained = ConcurrentHashMap.newKeySet();

    ParsleyQuiesceTracker() {}

    /** Registers a member, called when its task starts participating. */
    void register(String memberId) {
        registered.add(memberId);
    }

    /** Unregisters a member, called when its task stops participating. */
    void unregister(String memberId) {
        registered.remove(memberId);
        drained.remove(memberId);
    }

    /** Marks a member drained (buffer empty) or not, called after every buffer-depth-changing event. */
    void setDrained(String memberId, boolean isDrained) {
        if (isDrained) {
            drained.add(memberId);
        } else {
            drained.remove(memberId);
        }
    }

    /**
     * Whether every registered member (possibly none) is currently drained.
     *
     * @return {@code true} once every registered member's causal buffer is empty
     */
    boolean allDrained() {
        return drained.containsAll(registered);
    }
}
