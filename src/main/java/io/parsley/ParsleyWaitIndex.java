package io.parsley;

import org.apache.kafka.common.Uuid;

import java.util.List;

/**
 * A secondary index over the causal buffer that maps coordinate advances to candidate records.
 *
 * <p>When a record is buffered, its unsatisfied coordinates (those where the required offset exceeds
 * the current frontier) are indexed here. When the frontier advances on coordinate C, {@link
 * #candidatesFor} returns the records waiting on C so the engine can check them for full causal
 * satisfaction without scanning the entire buffer.
 *
 * <p>The index is <em>not</em> the source of truth: the buffer store and the current frontier are
 * authoritative. The index only reduces the search space. Stale entries (pointing to
 * already-released or evicted records) are tolerated and cleaned up lazily via {@link #tombstone}.
 */
interface ParsleyWaitIndex {

    /**
     * An index hit: the record waiting on a coordinate, plus the key information needed to
     * tombstone this entry if the record is no longer in the buffer.
     */
    record Candidate(Uuid topicId, int partition, long requiredOffset, long recordId) {}

    /**
     * Indexes every coordinate in {@code required} that is not yet satisfied by {@code frontier}.
     *
     * @param recordId the buffer sequence number of the buffered record
     * @param required the record's causal dependencies
     * @param frontier the frontier at the time of buffering; unsatisfied = required offset &gt; observed
     */
    void index(long recordId, CausalDependencies required, CausalFrontier frontier);

    /**
     * Returns all index entries for {@code (topicId, partition)} whose required offset is ≤
     * {@code newOffset}. The result may include stale entries; callers must verify with the buffer
     * store and call {@link #tombstone} for any that no longer have a buffer entry.
     *
     * @param topicId   the topic UUID whose frontier just advanced
     * @param partition the partition whose frontier just advanced
     * @param newOffset the new observed offset on this coordinate
     * @return candidates; empty if none are indexed on this coordinate within the offset range
     */
    List<Candidate> candidatesFor(Uuid topicId, int partition, long newOffset);

    /**
     * Removes a single index entry. Called to lazily clean up stale entries discovered during a
     * scan.
     *
     * @param candidate the stale candidate to tombstone
     */
    void tombstone(Candidate candidate);
}
