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
 * <p><strong>Only committed completeness is ever published.</strong> The epoch-events append rides an
 * idempotent side-channel producer, deliberately outside the task's EOS transaction — so it would not
 * roll back with a crashed transaction whose deliveries it reflects. Every publish therefore reads
 * {@link ParsleyCommittedCompleteness#committed()} — the completeness as of the task's last
 * <em>committed</em> transaction — never the live in-memory clock; see that class for the two-slot
 * commit-cycle mechanism and the joiner-floor hazard this closes.
 */
@FunctionalInterface
interface ParsleyEpochSnapshotPublisher {

    /**
     * Publishes {@code completeness} — the node's delivered frontier at the snapshot point — tagged with
     * {@code memberId} ({@code application.id/taskId}), for every node's deterministic fold to merge-min
     * into the next epoch's lower bounds.
     */
    void publish(String memberId, ParsleyVectorClock completeness);

    /** The no-op publisher used when coordination is not configured: a snapshot marker publishes nothing. */
    ParsleyEpochSnapshotPublisher NOOP = (memberId, completeness) -> {};
}
