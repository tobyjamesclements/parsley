package io.github.tobyjamesclements.parsley;

/**
 * The seam by which a processor publishes its completeness frontier to the Topology Co-ordinator during
 * an epoch snapshot (the first marker of the Mattern two-marker cut). On consuming a
 * {@link ParsleyHeader#EPOCH_SNAPSHOT} marker, {@code ParsleyProcessor} calls {@link #publish} with the
 * task/member id and the node's current {@link ParsleyEngine#completeness() completeness} frontier; the
 * coordinator merge-mins the published clocks across all nodes into the next epoch's {@code lowerBounds}
 * (the greatest lower bound every node has passed, so nothing un-delivered sits below it).
 *
 * <p>This is a narrow seam so the transport — a Kafka Streams sink to a coordinator topic, a dedicated
 * producer, or otherwise — is decided when the coordinator itself is built, not baked into the engine.
 * The default {@link #NOOP} disables publication (no coordinator deployed), so a node runs in epoch 0
 * unchanged: a snapshot marker only ever arrives when a coordinator is broadcasting one.
 */
@FunctionalInterface
interface ParsleyEpochSnapshotPublisher {

    /**
     * Publishes {@code completeness} — the node's delivered frontier at the snapshot point — tagged with
     * {@code memberId} (the Kafka Streams task id), for the coordinator to merge-min into the next
     * epoch's lower bounds.
     */
    void publish(String memberId, ParsleyClock completeness);

    /** The no-op publisher used when no coordinator is deployed: a snapshot marker publishes nothing. */
    ParsleyEpochSnapshotPublisher NOOP = (memberId, completeness) -> {};
}
