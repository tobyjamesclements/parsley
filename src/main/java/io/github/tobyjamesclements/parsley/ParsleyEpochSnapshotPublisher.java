package io.github.tobyjamesclements.parsley;

/**
 * The seam by which a processor publishes its completeness frontier for an open snapshot round. On
 * consuming a {@link ParsleyHeader#EPOCH_SNAPSHOT} marker (or deriving from the coordination log that
 * it owes an open round a publication), {@code ParsleyProcessor} calls {@link #publish} with the
 * member id and the node's current {@link ParsleyEngine#completeness() completeness} frontier; every
 * node's deterministic fold over the shared epoch-events log ({@link ParsleyEpochLog}) merge-mins the
 * published clocks into the next epoch's {@code lowerBounds} (the greatest lower bound every node has
 * passed, so nothing un-delivered sits below it). Leaderless: there is no coordinator process — the
 * log's total order plus the identical fold on every node is the coordinator.
 *
 * <p>This is a narrow seam so the transport is decided by the runtime wiring, not baked into the
 * engine. The default {@link #NOOP} disables publication (coordination not configured), so a node
 * runs in epoch 0 unchanged. When coordination is configured this is backed by
 * {@link ParsleyEpochRuntime#publishFrontier}, appending a
 * {@link ParsleyEpochEvent.FrontierPublished} to the epoch-events log.
 *
 * <p><strong>Known limitation under {@code exactly_once_v2}, still open:</strong> the published
 * {@code completeness} is read from the in-memory frontier (see {@code ParsleyProcessor}'s
 * {@code snapshotPublisher.publish(memberId, engine().completeness())} calls), which under EOS may
 * momentarily lead the durably-committed frontier store (a crash before the transaction commits reverts
 * it). Because {@code lowerBounds} is the merge-min of published frontiers, a floor could then exceed a
 * node's durable progress and strip a dependency that node still owns after replay. The safe rule is to
 * publish only completeness as of the last Streams commit; that hardening has not been applied yet.
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
