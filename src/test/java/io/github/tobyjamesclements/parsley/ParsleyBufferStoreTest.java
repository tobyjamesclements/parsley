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
     * Records are returned by {@code entries()} in insertion order, each carrying the
     * decoded dependencies that were stamped on the record when it was buffered.
     *
     * Asserts that entries are returned in insertion order, that source offsets and
     * dependencies match the originals, and that sequence numbers increase monotonically.
     */
    @Test
    void entriesAreReturnedInInsertionOrderWithTheirDependencies() {
        store.add(bufferedRecord(T1, 0, ParsleyClock.empty().observe(T1_ID, 0, 9)), 0L);
        store.add(bufferedRecord(T1, 1, ParsleyClock.empty().observe(T1_ID, 0, 3)), 0L);

        List<ParsleyBufferStore.Entry<String, String>> entries = store.entries();

        assertEquals(2, entries.size(), "store must return both added entries");
        assertEquals(0L, entries.get(0).record().offset(), "first entry must be the first added record");
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 9),
                entries.get(0).dependencies(), "first entry must carry its original dependencies");
        assertEquals(1L, entries.get(1).record().offset(), "second entry must be the second added record");
        assertTrue(entries.get(0).sequence() < entries.get(1).sequence(),
                "earlier-added entry must carry the lower sequence number");
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
        store.add(bufferedRecord(T1, 0, ParsleyClock.empty().observe(T1_ID, 0, 1)), 0L);
        store.add(bufferedRecord(T1, 1, ParsleyClock.empty().observe(T1_ID, 0, 5)), 0L);
        assertEquals(2, store.size(), "both records must be in the store before removal");

        long firstSeq = store.entries().get(0).sequence();
        store.remove(firstSeq);

        assertEquals(1, store.size(), "store size must decrement after removal");
        assertEquals(1L, store.entries().get(0).record().offset(),
                "removing the first entry must leave the second");
    }

    /**
     * A freshly created buffer store contains no entries and reports a size of zero.
     *
     * Asserts that {@code entries()} is empty and {@code size()} returns 0.
     */
    @Test
    void emptyStoreHasNoEntries() {
        assertTrue(store.entries().isEmpty(), "new store must have no entries");
        assertEquals(0, store.size(), "new store must report size 0");
    }

    /**
     * The {@code bufferedAt} timestamp passed to {@code add} is preserved and returned by both
     * {@code get} and {@code entries}.
     *
     * Asserts that the stored entry's {@code bufferedAt} matches what was passed to {@code add}.
     */
    @Test
    void bufferedAtRoundTripsThroughAddGetAndEntries() {
        long bufferedAt = 12_345L;
        long seq = store.add(bufferedRecord(T1, 0,
                ParsleyClock.empty().observe(T1_ID, 0, 1)), bufferedAt);

        assertEquals(bufferedAt, store.get(seq).bufferedAt(), "get() must return the bufferedAt passed to add()");
        assertEquals(bufferedAt, store.entries().get(0).bufferedAt(),
                "entries() must return the bufferedAt passed to add()");
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
        long firstSeq = store.add(bufferedRecord(T1, 0, ParsleyClock.empty()), 100L);
        long secondSeq = store.add(bufferedRecord(T1, 1, ParsleyClock.empty()), 200L);
        long thirdSeq = store.add(bufferedRecord(T1, 2, ParsleyClock.empty()), 300L);

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
                                                                  ParsleyClock deps) {
        return new ParsleyMessage<>(tp.topic(), T1_ID, tp.partition(), offset, 0L, "k", "v", List.of(), deps);
    }
}
