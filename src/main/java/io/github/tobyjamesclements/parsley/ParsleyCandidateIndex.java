package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.List;

/**
 * A secondary index over the causal buffer that maps coordinate advances to candidate records.
 *
 * <p>When a record is buffered, its unsatisfied coordinates (those where the required offset exceeds
 * the current frontier) are indexed here. When the frontier advances on coordinate C, {@link
 * #findCandidates} returns the records waiting on C so the engine can check them for full causal
 * satisfaction without scanning the entire buffer.
 *
 * <p>The index is <em>not</em> the source of truth: the buffer store and the current frontier are
 * authoritative. The index only reduces the search space. Stale entries (pointing to
 * already-released or evicted records) are tolerated and cleaned up lazily via {@link #prune}.
 */
interface ParsleyCandidateIndex {

    /**
     * An index hit: the record waiting on a coordinate, plus the key information needed to
     * prune this entry if the record is no longer in the buffer.
     */
    record Candidate(Uuid topicId, int partition, long requiredOffset, long recordId) {}

    /**
     * Indexes every coordinate in {@code required} that is not yet satisfied by {@code frontier}.
     *
     * @param recordId the buffer sequence number of the buffered record
     * @param required the record's causal dependency clock
     * @param frontier the frontier at the time of buffering; unsatisfied means the required offset
     *                 exceeds {@code frontier}'s offset for that coordinate
     */
    void index(long recordId, ParsleyClock required, ParsleyClock frontier);

    /**
     * Returns all index entries for {@code (topicId, partition)} whose required offset is ≤
     * {@code newOffset}. The result may include stale entries; callers must verify with the buffer
     * store and call {@link #prune} for any that no longer have a buffer entry.
     *
     * @param topicId   the topic UUID whose frontier just advanced
     * @param partition the partition whose frontier just advanced
     * @param newOffset the coordinate's newly advanced frontier offset
     * @return candidates; empty if none are indexed on this coordinate within the offset range
     */
    List<Candidate> findCandidates(Uuid topicId, int partition, long newOffset);

    /**
     * Removes a single index entry. Called to lazily clean up stale entries discovered during a
     * scan.
     *
     * @param candidate the stale candidate to prune
     */
    void prune(Candidate candidate);
}
