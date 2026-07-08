package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link RocksOrphanIndex} — the {@link org.apache.kafka.streams.state.KeyValueStore}-backed
 * {@link ParsleyOrphanIndex} implementation — against a real
 * {@link org.apache.kafka.streams.state.KeyValueStore} (via {@link TestKeyValueStore}).
 */
class RocksOrphanIndexTest {

    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    /**
     * A coordinate with no recorded orphan floor reports {@code -1} and is never orphaned.
     *
     * Asserts {@code orphanFloor} returns {@code -1} for an unrecorded coordinate.
     */
    @Test
    void orphanFloorIsMinusOneForAnUnrecordedCoordinate() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());

        assertEquals(-1L, index.orphanFloor(TOPIC_ID, 0), "an unrecorded coordinate has no orphan floor");
    }

    /**
     * The first {@code markOrphaned} call for a coordinate establishes its floor and reports the
     * establishment via its {@code true} return.
     *
     * Asserts {@code markOrphaned} returns {@code true} and {@code orphanFloor} reflects the new floor.
     */
    @Test
    void markOrphanedEstablishesTheFloorOnFirstCall() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());

        assertTrue(index.markOrphaned(TOPIC_ID, 0, 5), "the first call for a coordinate must establish its floor");
        assertEquals(5L, index.orphanFloor(TOPIC_ID, 0), "the recorded floor must be the one just established");
    }

    /**
     * A coordinate's orphan floor is monotonic: a later call with a strictly lower floor lowers it and
     * reports the change, but a call at or above the existing floor is a no-op — the floor can only ever
     * strengthen. A coordinate's contiguous frontier freezes at the earliest offset ever proven
     * unreachable, so a later dead-letter on the same coordinate can only prove a stronger, lower floor;
     * a higher one is a strictly weaker claim and must never overwrite it.
     *
     * Asserts a lower floor lowers and returns {@code true}; an equal or higher floor is a no-op
     * returning {@code false}, leaving the floor unchanged.
     */
    @Test
    void markOrphanedIsMonotonicLoweringButNeverRaisingTheFloor() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());
        index.markOrphaned(TOPIC_ID, 0, 5);

        assertFalse(index.markOrphaned(TOPIC_ID, 0, 5), "an equal floor must be a no-op");
        assertFalse(index.markOrphaned(TOPIC_ID, 0, 9), "a higher floor must be a no-op");
        assertEquals(5L, index.orphanFloor(TOPIC_ID, 0), "the floor must remain at its established value");

        assertTrue(index.markOrphaned(TOPIC_ID, 0, 3), "a strictly lower floor must lower and report the change");
        assertEquals(3L, index.orphanFloor(TOPIC_ID, 0), "the floor must have lowered to the new value");
    }

    /**
     * Regression guard for the {@code -1} "not orphaned" sentinel: the first {@code markOrphaned} call
     * for a coordinate must establish its floor even when that floor is {@code 0} — a naive
     * "no-op if the existing floor is {@code <=} the new one" comparison would wrongly treat the
     * sentinel ({@code -1}) as already lower than a legitimate floor of {@code 0} and skip the
     * establish entirely.
     *
     * Asserts {@code markOrphaned} returns {@code true} and {@code orphanFloor} reports {@code 0}.
     */
    @Test
    void markOrphanedEstablishesTheFloorOnFirstCallEvenAtOffsetZero() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());

        assertTrue(index.markOrphaned(TOPIC_ID, 0, 0), "the first call must establish the floor even at offset 0");
        assertEquals(0L, index.orphanFloor(TOPIC_ID, 0), "the recorded floor must be the one just established");
    }

    /**
     * Orphan floors for distinct {@code (topicId, partition)} coordinates are independent — marking one
     * coordinate's floor never affects a different coordinate's.
     *
     * Asserts marking one partition's floor leaves a different partition's and a different topic's
     * floors unrecorded.
     */
    @Test
    void orphanFloorsAreScopedToTheirCoordinate() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());
        Uuid otherTopicId = Uuid.randomUuid();
        index.markOrphaned(TOPIC_ID, 0, 5);

        assertEquals(-1L, index.orphanFloor(TOPIC_ID, 1), "a different partition must be unaffected");
        assertEquals(-1L, index.orphanFloor(otherTopicId, 0), "a different topic must be unaffected");
    }

    /**
     * {@code prune} removes a coordinate's orphan entry entirely — the hook a future epoch-exclusion
     * floor advance uses once the entry is moot.
     *
     * Asserts the coordinate reports no orphan floor after pruning.
     */
    @Test
    void pruneRemovesTheOrphanEntry() {
        RocksOrphanIndex index = new RocksOrphanIndex(newRocksStore());
        index.markOrphaned(TOPIC_ID, 0, 5);

        index.prune(TOPIC_ID, 0);

        assertEquals(-1L, index.orphanFloor(TOPIC_ID, 0), "a pruned coordinate must report no orphan floor");
    }

    private static org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> newRocksStore() {
        return new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned);
    }
}
