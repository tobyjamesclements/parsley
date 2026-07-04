package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * A durable record of every coordinate this node has proven can never legitimately advance past a
 * given offset again — because the record that would have closed the gap was dead-lettered instead of
 * delivered. {@link ParsleyEngine} consults this, at ingest and during the drain/propagate cascade, to
 * decide whether a buffered or incoming record is itself now unrecoverable (its dependencies require an
 * offset at or above an orphaned coordinate's floor) and must be dead-lettered rather than held forever.
 *
 * <p>Monotonic per coordinate, like {@link ParsleyForwardedIndex}: a coordinate's floor only ever
 * increases — a later dead-letter on the same coordinate can only prove a stronger (higher) floor.
 *
 * <p>{@link #prune} is the hook a future topology-epoch-exclusion mechanism uses: once a coordinate's
 * floor has legitimately advanced past an orphan entry (e.g. an epoch transition strips any dependency
 * referencing it), the entry is moot and removable, keeping this index bounded rather than growing
 * forever.
 */
interface ParsleyOrphanIndex {

    /**
     * Records that {@code (topicId, partition)} can never legitimately advance past {@code floor - 1}
     * again. A no-op if this coordinate is already orphaned at a floor {@code >= floor}.
     *
     * @param topicId   the orphaned coordinate's topic UUID
     * @param partition the orphaned coordinate's partition
     * @param floor     the offset the coordinate can never advance past (exclusive lower bound of the gap)
     * @return {@code true} if this call newly established or raised the floor (the caller should cascade)
     */
    boolean markOrphaned(Uuid topicId, int partition, long floor);

    /**
     * Returns the orphan floor recorded for {@code (topicId, partition)}, or {@code -1} if the
     * coordinate is not orphaned.
     *
     * @param topicId   the coordinate's topic UUID
     * @param partition the coordinate's partition
     * @return the orphan floor, or {@code -1} if not orphaned
     */
    long orphanFloor(Uuid topicId, int partition);

    /**
     * Removes the orphan entry for {@code (topicId, partition)}, once the frontier has legitimately
     * advanced past it and the entry is moot.
     *
     * @param topicId   the coordinate's topic UUID
     * @param partition the coordinate's partition
     */
    void prune(Uuid topicId, int partition);
}
