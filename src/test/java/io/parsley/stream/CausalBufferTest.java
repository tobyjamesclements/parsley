package io.parsley.stream;

import io.parsley.VectorClock;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final CausalBuffer<String, String> buffer = new CausalBuffer<>();

    private static CausalRecord<String, String> rec(TopicPartition tp, long offset) {
        return new CausalRecord<>("k", "v", 0L, List.of(), new byte[0], tp, offset);
    }

    @Test
    void drainReleasesOnlySatisfiedRecordsInInsertionOrder() {
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 1));
        buffer.add(rec(ORDERS, 1), VectorClock.empty().advance(PRICES, 5));

        List<CausalRecord<String, String>> released = buffer.drain(VectorClock.empty().advance(PRICES, 1));

        assertEquals(1, released.size());
        assertEquals(0L, released.get(0).sourceOffset());
        assertEquals(1, buffer.size());
    }

    @Test
    void drainCascadesAreDrivenByTheCaller() {
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 1));
        // A frontier that satisfies nothing releases nothing.
        assertTrue(buffer.drain(VectorClock.empty()).isEmpty());
        assertEquals(1, buffer.size());
    }

    @Test
    void evictAllRemovesEveryEntryWithItsDependenciesInInsertionOrder() {
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 9));
        buffer.add(rec(ORDERS, 1), VectorClock.empty().advance(PRICES, 3));

        List<CausalBuffer.Buffered<String, String>> evicted = buffer.evictAll();

        assertEquals(2, evicted.size());
        assertEquals(0L, evicted.get(0).record().sourceOffset());
        assertEquals(VectorClock.empty().advance(PRICES, 9), evicted.get(0).dependencies());
        assertEquals(1L, evicted.get(1).record().sourceOffset());
        assertEquals(0, buffer.size());
    }

    @Test
    void evictAllOnAnEmptyBufferReturnsEmpty() {
        assertTrue(buffer.evictAll().isEmpty());
    }
}
