package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferStoreTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final BufferStore<String, String> store = new InMemoryBufferStore<>();

    private static CausalRecord<String, String> rec(TopicPartition tp, long offset) {
        return new CausalRecord<>("k", "v", 0L, List.of(), new byte[0], tp, offset);
    }

    @Test
    void entriesAreReturnedInInsertionOrderWithTheirDependencies() {
        store.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 9));
        store.add(rec(ORDERS, 1), VectorClock.empty().advance(PRICES, 3));

        List<BufferStore.Entry<String, String>> entries = store.entries();

        assertEquals(2, entries.size());
        assertEquals(0L, entries.get(0).record().sourceOffset());
        assertEquals(VectorClock.empty().advance(PRICES, 9), entries.get(0).dependencies());
        assertEquals(1L, entries.get(1).record().sourceOffset());
        assertTrue(entries.get(0).sequence() < entries.get(1).sequence(),
                "earlier-added entry must carry the lower sequence");
    }

    @Test
    void removeDropsTheEntryAndDecrementsSize() {
        store.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 1));
        store.add(rec(ORDERS, 1), VectorClock.empty().advance(PRICES, 5));
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
