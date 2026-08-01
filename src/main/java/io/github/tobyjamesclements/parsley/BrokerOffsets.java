package io.github.tobyjamesclements.parsley;

import java.util.Map;
import java.util.Set;

/**
 * The protocol's window onto broker offset facts, being the two append-time queries the
 * protocol needs.
 *
 * <p>Both are about ranges, not records: what the broker has assigned, as end offsets for the
 * init-time own-outputs seed, and what retention has deleted, as log starts for the
 * coordination-free stability bound truncation uses.
 *
 * <p>Nothing here observes individual sends. The node's own in-flight outputs are claimed in
 * sequence space, assigned synchronously at the stamping site, and resolved by receivers from
 * the sender tag each record carries.
 *
 * <p>Package-private on purpose. Both queries carry soundness obligations, namely strict
 * end-offset resolution and definitive absence, that make this a seam users must not
 * substitute. Tests use {@link Parsley#testTopology()}, which wires a disconnected view
 * internally.
 */
interface BrokerOffsets {

    /**
     * Returns current end offsets, the next offset to be assigned, for every partition of the
     * given sink topics.
     *
     * <p>Used for the init-time own-outputs seed. Strict, so resolution failure must throw,
     * because starting with the seed silently off would under-claim the node's own outputs for
     * the task's whole lifetime.
     *
     * @param sinkTopics the stage's declared sink topics, by stable identity
     * @return the end offset of every partition of those topics
     */
    Map<Channel, Long> endOffsets(Set<java.util.UUID> sinkTopics);

    /**
     * The log-start view of a set of channels, for the truncation driver.
     *
     * @param logStarts the log-start offset of each channel that resolved
     * @param confirmedAbsent the channels whose topic definitively no longer exists
     */
    record EarliestOffsets(Map<Channel, Long> logStarts, Set<Channel> confirmedAbsent) {}

    /**
     * Returns log-start offsets for the given channels, the coordination-free stability source.
     *
     * <p>Records deleted by retention sit below every reachable baseline, so claims at or below
     * {@code logStart - 1} are globally out of scope.
     *
     * <p>A recreated topic is a different channel, so an absent topic's claims are unclaimable
     * forever. A transient resolution failure must throw or omit the channel, never report
     * absence, so that the sweep fails closed.
     *
     * @param channels the channels the stamp-side clocks still mention
     * @return the log starts that resolved, and only the channels whose topic definitively no
     *         longer exists
     */
    EarliestOffsets earliestOffsets(Set<Channel> channels);
}
