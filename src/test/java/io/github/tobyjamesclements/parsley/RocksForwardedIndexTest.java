package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link RocksForwardedIndex} — the {@link org.apache.kafka.streams.state.KeyValueStore}-backed
 * {@link ParsleyForwardedIndex} implementation — against a real {@link org.apache.kafka.streams.state.KeyValueStore}
 * (via {@link TestKeyValueStore}), exercising the actual big-endian byte-key range scan rather than
 * {@link MockForwardedIndex}'s in-memory structure.
 */
class RocksForwardedIndexTest {

    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    /**
     * {@code forwardedAfter} returns marked offsets for a coordinate strictly greater than the given
     * frontier offset, in ascending order — exactly the range {@link ParsleyEngine} needs to walk a
     * contiguous run forward.
     *
     * Asserts offsets at or below the frontier are excluded, and offsets above it are returned
     * ascending.
     */
    @Test
    void forwardedAfterScansTheRealRangeAscendingAndExcludesAtOrBelowTheFrontier() {
        RocksForwardedIndex index = new RocksForwardedIndex(newRocksStore());
        index.mark(TOPIC_ID, 0, 6);
        index.mark(TOPIC_ID, 0, 8);
        index.mark(TOPIC_ID, 0, 7);
        index.mark(TOPIC_ID, 0, 4); // at-or-below the frontier below — must be excluded

        List<Long> above = index.forwardedAfter(TOPIC_ID, 0, 4);

        assertEquals(List.of(6L, 7L, 8L), above, "offsets above the frontier must be returned ascending");
    }

    /**
     * {@code unmark} removes the underlying store entry for an offset, so a later
     * {@code forwardedAfter} scan over the same range no longer returns it — this is how an offset
     * absorbed into the contiguous frontier is retired from the index.
     *
     * Asserts the offset is present before unmarking and absent after.
     */
    @Test
    void unmarkRemovesTheOffsetFromTheRealStore() {
        RocksForwardedIndex index = new RocksForwardedIndex(newRocksStore());
        index.mark(TOPIC_ID, 0, 5);

        assertEquals(List.of(5L), index.forwardedAfter(TOPIC_ID, 0, 0),
                "the offset must be present before unmarking");

        index.unmark(TOPIC_ID, 0, 5);

        assertTrue(index.forwardedAfter(TOPIC_ID, 0, 0).isEmpty(),
                "the offset must be gone from the store after unmarking");
    }

    /**
     * Marked offsets for distinct {@code (topicId, partition)} coordinates occupy disjoint key
     * ranges (the coordinate is the leading portion of the big-endian key), so a scan for one
     * partition never returns another partition's — or another topic's — marked offsets, and
     * unmarking one coordinate's offset never deletes a neighboring coordinate's identical offset.
     *
     * Asserts a scan for one partition returns only that partition's offsets, and that unmarking
     * one coordinate leaves an identical offset on a different coordinate untouched.
     */
    @Test
    void forwardedAfterAndUnmarkAreScopedToTheirCoordinateWithNoOverDeletion() {
        RocksForwardedIndex index = new RocksForwardedIndex(newRocksStore());
        Uuid otherTopicId = Uuid.randomUuid();
        index.mark(TOPIC_ID, 0, 5);
        index.mark(TOPIC_ID, 1, 5);
        index.mark(otherTopicId, 0, 5);

        assertEquals(List.of(5L), index.forwardedAfter(TOPIC_ID, 0, 0),
                "a scan must not cross partition or topic boundaries");

        index.unmark(TOPIC_ID, 0, 5);

        assertTrue(index.forwardedAfter(TOPIC_ID, 0, 0).isEmpty(), "the unmarked coordinate's offset must be gone");
        assertEquals(List.of(5L), index.forwardedAfter(TOPIC_ID, 1, 0),
                "unmarking one partition must not delete the identical offset on a neighboring partition");
        assertEquals(List.of(5L), index.forwardedAfter(otherTopicId, 0, 0),
                "unmarking one topic must not delete the identical offset on a neighboring topic");
    }

    /**
     * {@code pruneAtOrBelow} deletes marked offsets at or below the watermark and nothing else —
     * in particular never the offset at {@code watermark + 1}, which is a legitimately marked entry
     * above the contiguous frontier and the very next candidate the absorb walk would consume.
     * {@code range} is inclusive of both bounds, so an upper key of {@code watermark + 1} (the old
     * off-by-one) would have deleted it.
     *
     * Asserts offsets at and below the watermark are gone, the offset exactly one above survives,
     * and a negative watermark is a no-op.
     */
    @Test
    void pruneAtOrBelowDeletesOnlyAtOrBelowTheWatermarkNeverTheNextCandidateAbove() {
        RocksForwardedIndex index = new RocksForwardedIndex(newRocksStore());
        index.mark(TOPIC_ID, 0, 3);
        index.mark(TOPIC_ID, 0, 5);
        index.mark(TOPIC_ID, 0, 6); // watermark + 1: must survive the prune
        index.mark(TOPIC_ID, 0, 8);

        index.pruneAtOrBelow(TOPIC_ID, 0, 5);

        assertEquals(List.of(6L, 8L), index.forwardedAfter(TOPIC_ID, 0, -1),
                "only offsets at or below the watermark (3 and 5) may be pruned; the marked offset at "
                        + "watermark + 1 is the absorb walk's next candidate and must survive");

        index.pruneAtOrBelow(TOPIC_ID, 0, -1);

        assertEquals(List.of(6L, 8L), index.forwardedAfter(TOPIC_ID, 0, -1),
                "a negative watermark (coordinate never observed) must be a no-op");
    }

    private static org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> newRocksStore() {
        return new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned);
    }
}
