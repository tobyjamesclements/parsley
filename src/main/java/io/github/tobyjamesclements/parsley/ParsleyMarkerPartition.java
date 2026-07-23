package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

/**
 * A thread-local side channel that lets a shared {@link ParsleyMarkerPartitioner} — one {@link
 * org.apache.kafka.streams.processor.StreamPartitioner} instance registered once at topology-build time,
 * reused across every parallel task instance of a sink node — know which partition to route a Parsley
 * protocol marker (a null message) to for the task currently forwarding one.
 * {@link ParsleyProcessor#forwardToSinks} sets this immediately before, and clears it immediately after,
 * every marker forward.
 *
 * <p>Safe because a Kafka Streams task processes one record at a time, synchronously, on the single
 * {@code StreamThread} that owns it — there is never a second marker forward in flight on the same
 * thread while this value is set. A {@link org.apache.kafka.streams.processor.StreamPartitioner} has no
 * other way to learn which task's forward it is currently partitioning for: its {@code partitions(topic,
 * key, value, numPartitions)} signature carries no task identity and no headers, only what the record
 * itself carries — which is exactly the routing gap this exists to close (a marker must reach the
 * forwarding task's own owned partition regardless of whether a usable business key was ever observed).
 */
final class ParsleyMarkerPartition {

    private static final ThreadLocal<Integer> TARGET = new ThreadLocal<>();

    private ParsleyMarkerPartition() {
    }

    /** Sets the partition the next sink write on this thread must land on. */
    static void set(int partition) {
        TARGET.set(partition);
    }

    /** Clears the override, so a subsequent (business) forward on this thread uses the sink's own partitioner. */
    static void clear() {
        TARGET.remove();
    }

    /** The overridden partition for the sink write in progress on this thread, or {@code null} if none. */
    static @Nullable Integer get() {
        return TARGET.get();
    }
}
