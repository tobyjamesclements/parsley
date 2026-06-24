package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.List;

/**
 * A durable record of every offset the engine has forwarded (whether by immediate admission,
 * release, eviction, or poison-drop) that has not yet been absorbed into the contiguous frontier.
 *
 * <p>The engine does not head-of-line block: a later offset on a partition may forward before an
 * earlier offset on the same partition, if the earlier one is still held. Each forward marks its
 * coordinate here; {@link #forwardedAfter} lets the engine walk forward from the current frontier
 * to find the longest run of consecutive offsets now forwarded, so the frontier only ever advances
 * over a coordinate once every offset up to it has actually gone out — never past a gap. Unlike
 * {@link ParsleyCandidateIndex}, this index <em>is</em> the source of truth for what has been
 * forwarded but not yet folded into the frontier: an entry exists exactly as long as its offset is
 * marked-but-unabsorbed.
 */
interface ParsleyForwardedIndex {

    /**
     * Marks {@code (topicId, partition, offset)} as forwarded. The entry persists until {@link
     * #unmark} removes it, once the engine has folded it into the contiguous frontier.
     *
     * @param topicId   the forwarded record's source topic UUID
     * @param partition the forwarded record's source partition
     * @param offset    the forwarded record's source offset
     */
    void mark(Uuid topicId, int partition, long offset);

    /**
     * Returns the marked offsets for {@code (topicId, partition)} strictly greater than {@code
     * frontierOffset}, in ascending order, for the engine to walk while extending the contiguous
     * frontier.
     *
     * @param topicId        the coordinate's topic UUID
     * @param partition      the coordinate's partition
     * @param frontierOffset the coordinate's current frontier offset
     * @return the marked offsets above {@code frontierOffset}, ascending; empty if none
     */
    List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset);

    /**
     * Removes a single marked offset, once the engine has folded it into the contiguous frontier.
     *
     * @param topicId   the coordinate's topic UUID
     * @param partition the coordinate's partition
     * @param offset    the offset to unmark
     */
    void unmark(Uuid topicId, int partition, long offset);
}
