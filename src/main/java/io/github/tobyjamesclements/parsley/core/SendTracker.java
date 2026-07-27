package io.github.tobyjamesclements.parsley.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The core's window onto the node's own produced records. The broker performs the sender's
 * clock increment (offset assignment), learned asynchronously from acknowledgements; the core
 * claims unacknowledged sends in sequence space, so nothing here ever blocks. Acknowledgements
 * only upgrade sequence claims to offset claims.
 *
 * <p>Per-channel acknowledgement order must match send order (Kafka's per-partition
 * guarantee), because the core aligns acknowledgements with its per-channel send counters to
 * normalise its own claims.
 */
public interface SendTracker {

    /** One acknowledged own send. */
    record Ack(Channel channel, long offset) {}

    /** Returns and clears every acknowledgement received since the last drain. */
    List<Ack> drainAcks();

    /**
     * Current end offsets (next offset to be assigned) for every partition of the given sink
     * topics, used for the init-time own-outputs seed. Strict: resolution failure must throw,
     * because starting with the seed silently off would under-claim the node's own outputs for
     * the task's whole lifetime.
     */
    Map<Channel, Long> endOffsets(Set<java.util.UUID> sinkTopics);
}
