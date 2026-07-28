package io.github.tobyjamesclements.parsley;

import java.util.Map;
import java.util.Set;

/**
 * The protocol's window onto broker offset facts — the two append-time queries the protocol
 * needs. Both are about ranges, not records: what the broker has assigned (end offsets, for
 * the init-time own-outputs seed) and what retention has deleted (log starts, the
 * coordination-free stability bound for truncation).
 *
 * <p>Nothing here observes individual sends. The node's own in-flight outputs are claimed in
 * sequence space, assigned synchronously at the stamping site, and resolved by receivers from
 * the sender tag each record carries — no acknowledgement feed exists.
 *
 * <p>Package-private on purpose: both queries carry soundness obligations (strict end-offset
 * resolution, definitive absence) that make this a seam users must not substitute. Tests use
 * {@link Parsley#testTopology()}, which wires a disconnected view internally.
 */
interface BrokerOffsets {

    /**
     * Current end offsets (next offset to be assigned) for every partition of the given sink
     * topics, used for the init-time own-outputs seed. Strict: resolution failure must throw,
     * because starting with the seed silently off would under-claim the node's own outputs for
     * the task's whole lifetime.
     */
    Map<Channel, Long> endOffsets(Set<java.util.UUID> sinkTopics);

    /** The log-start view of a set of channels, for the truncation driver. */
    record EarliestOffsets(Map<Channel, Long> logStarts, Set<Channel> confirmedAbsent) {}

    /**
     * Log-start offsets for the given channels — the coordination-free stability source:
     * records deleted by retention sit below every reachable baseline, so claims at or below
     * {@code logStart - 1} are globally out of scope. {@code confirmedAbsent} carries only
     * channels whose topic definitively no longer exists (a recreated topic is a different
     * channel, so an absent topic's claims are unclaimable forever); a transient resolution
     * failure must throw or omit the channel, never report absence (fail closed).
     */
    EarliestOffsets earliestOffsets(Set<Channel> channels);
}
