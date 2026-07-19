package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyBufferStoreTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();

    private final ParsleyBufferStore<String, String> store = new MockBufferStore<>();

    /**
     * {@code add} assigns strictly increasing insertion sequences, and {@code get(sequence)} returns
     * the buffered record with the dependencies that were stamped on it when it was buffered.
     *
     * Asserts sequences increase monotonically and each sequence resolves to its own record and
     * original dependencies.
     */
    @Test
    void addAssignsMonotonicSequencesAndGetReturnsEachRecordWithItsDependencies() {
        long first = store.add(bufferedRecord(T1, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 9)), 0L);
        long second = store.add(bufferedRecord(T1, 1, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)), 0L);

        assertTrue(first < second, "earlier-added record must carry the lower sequence number");
        assertEquals(0L, store.get(first).record().offset(), "first sequence must resolve to the first record");
        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 9),
                store.get(first).dependencies(), "first record must carry its original dependencies");
        assertEquals(1L, store.get(second).record().offset(), "second sequence must resolve to the second record");
    }

    /**
     * {@code remove(sequence)} deletes the entry with the given sequence number and
     * decrements the store size.
     *
     * Asserts that the store size decreases to 1 and that the remaining entry is the
     * one that was not removed.
     */
    @Test
    void removeDropsTheEntryAndDecrementsSize() {
        long first = store.add(bufferedRecord(T1, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 1)), 0L);
        long second = store.add(bufferedRecord(T1, 1, ParsleyVectorClock.empty().observe(T1_ID, 0, 5)), 0L);
        assertEquals(2, store.size(), "both records must be in the store before removal");

        store.remove(first);

        assertEquals(1, store.size(), "store size must decrement after removal");
        assertEquals(null, store.get(first), "the removed entry must be gone");
        assertEquals(1L, store.get(second).record().offset(), "removing the first entry must leave the second");
    }

    /**
     * A freshly created buffer store contains no entries and reports a size of zero.
     *
     * Asserts that {@code indexEntries()} is empty and {@code size()} returns 0.
     */
    @Test
    void emptyStoreHasNoEntries() {
        assertTrue(store.indexEntries().isEmpty(), "new store must have no entries");
        assertEquals(0, store.size(), "new store must report size 0");
    }

    /**
     * The {@code bufferedAt} timestamp passed to {@code add} is preserved and returned by both
     * {@code get} and {@code indexEntries}.
     *
     * Asserts that the stored entry's {@code bufferedAt} matches what was passed to {@code add}.
     */
    @Test
    void bufferedAtRoundTripsThroughAddGetAndIndexEntries() {
        long bufferedAt = 12_345L;
        long seq = store.add(bufferedRecord(T1, 0,
                ParsleyVectorClock.empty().observe(T1_ID, 0, 1)), bufferedAt);

        assertEquals(bufferedAt, store.get(seq).bufferedAt(), "get() must return the bufferedAt passed to add()");
        assertEquals(bufferedAt, store.indexEntries().get(0).bufferedAt(),
                "indexEntries() must return the bufferedAt passed to add()");
    }

    /**
     * A freshly created buffer store has no oldest record to report.
     *
     * Asserts that {@code oldestBufferedAt()} is empty on an empty store.
     */
    @Test
    void oldestBufferedAtIsEmptyWhenStoreIsEmpty() {
        assertEquals(OptionalLong.empty(), store.oldestBufferedAt(),
                "an empty store must report no oldest record");
    }

    /**
     * {@code oldestBufferedAt()} tracks the lowest surviving insertion sequence's {@code
     * bufferedAt}, not just the first record ever added: removing the current oldest must advance
     * it to the next-oldest survivor, and removing a younger record must leave it unchanged.
     *
     * Asserts the oldest timestamp updates after the oldest entry is removed, and is unaffected
     * by removing a younger entry.
     */
    @Test
    void oldestBufferedAtTracksTheLowestSurvivingSequence() {
        long firstSeq = store.add(bufferedRecord(T1, 0, ParsleyVectorClock.empty()), 100L);
        long secondSeq = store.add(bufferedRecord(T1, 1, ParsleyVectorClock.empty()), 200L);
        long thirdSeq = store.add(bufferedRecord(T1, 2, ParsleyVectorClock.empty()), 300L);

        assertEquals(OptionalLong.of(100L), store.oldestBufferedAt(),
                "oldest must be the first-added record's bufferedAt");

        store.remove(thirdSeq);
        assertEquals(OptionalLong.of(100L), store.oldestBufferedAt(),
                "removing a younger record must not change the oldest");

        store.remove(firstSeq);
        assertEquals(OptionalLong.of(200L), store.oldestBufferedAt(),
                "removing the oldest record must advance to the next-oldest survivor");

        store.remove(secondSeq);
        assertEquals(OptionalLong.empty(), store.oldestBufferedAt(),
                "removing the last record must leave no oldest record");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyMessage<String, String> bufferedRecord(TopicPartition tp, long offset,
                                                                  ParsleyVectorClock deps) {
        return new ParsleyMessage<>(tp.topic(), T1_ID, tp.partition(), offset, 0L, "k", "v", List.of(), deps);
    }
}
