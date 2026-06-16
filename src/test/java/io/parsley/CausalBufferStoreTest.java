package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferStoreTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final CausalBufferStore<String, String> store = new InMemoryBufferStore<>();

    // The record's VECTOR_CLOCK header carries the dependency clock; the store derives the decoded
    // clock from it, so build the record with the clock it depends on.
    private static ParsleyRecord<String, String> rec(TopicPartition tp, long offset, CausalDependencies deps) {
        return new ParsleyRecord<>("k", "v", 0L, List.of(
                new ParsleyHeader(ParsleyAttributes.VECTOR_CLOCK, deps.toBytes()),
                new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)),
                new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())),
                new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset))));
    }

    @Test
    void entriesAreReturnedInInsertionOrderWithTheirDependencies() {
        store.add(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 9)));
        store.add(rec(ORDERS, 1, CausalDependencies.empty().advance(PRICES, 3)));

        List<CausalBufferStore.Entry<String, String>> entries = store.entries();

        assertEquals(2, entries.size());
        assertEquals(0L, entries.get(0).record().sourceOffset());
        assertEquals(CausalDependencies.empty().advance(PRICES, 9), entries.get(0).dependencies());
        assertEquals(1L, entries.get(1).record().sourceOffset());
        assertTrue(entries.get(0).sequence() < entries.get(1).sequence(),
                "earlier-added entry must carry the lower sequence");
    }

    @Test
    void removeDropsTheEntryAndDecrementsSize() {
        store.add(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 1)));
        store.add(rec(ORDERS, 1, CausalDependencies.empty().advance(PRICES, 5)));
        assertEquals(2, store.size());

        long firstSeq = store.entries().get(0).sequence();
        store.remove(firstSeq);

        assertEquals(1, store.size());
        assertEquals(1L, store.entries().get(0).record().sourceOffset(),
                "removing the first entry must leave the second");
    }

    @Test
    void emptyStoreHasNoEntries() {
        assertTrue(store.entries().isEmpty());
        assertEquals(0, store.size());
    }
}
