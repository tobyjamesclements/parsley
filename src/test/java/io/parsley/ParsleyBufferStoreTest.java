package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyBufferStoreTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final Uuid PRICES_ID = CausalPosition.deriveUuid("prices");
    private static final Uuid ORDERS_ID = CausalPosition.deriveUuid("orders");

    private final ParsleyBufferStore<String, String> store = new MockBufferStore<>();

    // The record's CAUSAL_DEPENDENCIES header carries the dependencies; the store derives the decoded
    // dependencies from it, so build the record with the dependencies it requires.
    private static ParsleyRecord<String, String> rec(TopicPartition tp, long offset, CausalDependencies deps) {
        return new ParsleyRecord<>("k", "v", 0L, List.of(
                new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()),
                new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)),
                new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())),
                new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset))));
    }

    @Test
    void entriesAreReturnedInInsertionOrderWithTheirDependencies() {
        store.add(rec(ORDERS, 0, CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 9)).build()));
        store.add(rec(ORDERS, 1, CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 3)).build()));

        List<ParsleyBufferStore.Entry<String, String>> entries = store.entries();

        assertEquals(2, entries.size());
        assertEquals(0L, entries.get(0).record().sourceOffset());
        assertEquals(CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 9)).build(), entries.get(0).dependencies());
        assertEquals(1L, entries.get(1).record().sourceOffset());
        assertTrue(entries.get(0).sequence() < entries.get(1).sequence(),
                "earlier-added entry must carry the lower sequence");
    }

    @Test
    void removeDropsTheEntryAndDecrementsSize() {
        store.add(rec(ORDERS, 0, CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 1)).build()));
        store.add(rec(ORDERS, 1, CausalDependencies.builder().require(new CausalPosition(PRICES_ID, 0, 5)).build()));
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
