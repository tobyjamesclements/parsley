package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyBufferStoreTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final Uuid T1_ID = CausalPosition.deriveUuid(T1.topic());

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
        store.add(bufferedRecord(T1, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 9)).build()), 0L);
        store.add(bufferedRecord(T1, 1, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()), 0L);

        List<ParsleyBufferStore.Entry<String, String>> entries = store.entries();

        assertEquals(2, entries.size(), "store must return both added entries");
        assertEquals(0L, entries.get(0).record().sourceOffset(), "first entry must be the first added record");
        assertEquals(CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 9)).build(),
                entries.get(0).dependencies(), "first entry must carry its original dependencies");
        assertEquals(1L, entries.get(1).record().sourceOffset(), "second entry must be the second added record");
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
        store.add(bufferedRecord(T1, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 1)).build()), 0L);
        store.add(bufferedRecord(T1, 1, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 5)).build()), 0L);
        assertEquals(2, store.size(), "both records must be in the store before removal");

        long firstSeq = store.entries().get(0).sequence();
        store.remove(firstSeq);

        assertEquals(1, store.size(), "store size must decrement after removal");
        assertEquals(1L, store.entries().get(0).record().sourceOffset(),
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
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 1)).build()), bufferedAt);

        assertEquals(bufferedAt, store.get(seq).bufferedAt(), "get() must return the bufferedAt passed to add()");
        assertEquals(bufferedAt, store.entries().get(0).bufferedAt(),
                "entries() must return the bufferedAt passed to add()");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyRecord<String, String> bufferedRecord(TopicPartition tp, long offset,
                                                                  CausalDependencies deps) {
        return new ParsleyRecord<>("k", "v", 0L, List.of(
                new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()),
                new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)),
                new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())),
                new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset))));
    }
}
