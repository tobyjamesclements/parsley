package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final List<CausalViolationReason> violations = new ArrayList<>();
    private final CausalViolationHandler handler = (record, reason) -> violations.add(reason);
    private final List<CausalRecord<String, String>> removed = new ArrayList<>();

    private static CausalRecord<String, String> rec(TopicPartition tp, long offset) {
        return new CausalRecord<>("k", "v", 0L, List.of(), new byte[0], tp, offset);
    }

    @Test
    void drainReleasesOnlySatisfiedRecordsInInsertionOrder() {
        ForwardUnsafeBuffer<String, String> buffer = new ForwardUnsafeBuffer<>();
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 1));
        buffer.add(rec(ORDERS, 1), VectorClock.empty().advance(PRICES, 5));

        List<CausalRecord<String, String>> released = buffer.drain(VectorClock.empty().advance(PRICES, 1));

        assertEquals(1, released.size());
        assertEquals(0L, released.get(0).sourceOffset());
        assertEquals(1, buffer.size());
    }

    @Test
    void forwardUnsafeBufferReturnsEvictedRecordsForForwarding() {
        ForwardUnsafeBuffer<String, String> buffer = new ForwardUnsafeBuffer<>();
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 9));

        List<CausalRecord<String, String>> evicted = buffer.evict(BufferLimit.ofSize(1), handler, removed::add);

        assertEquals(1, evicted.size());
        assertEquals(0, buffer.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
        assertEquals(evicted, removed, "every evicted record must be reported to onRemoved");
    }

    @Test
    void dropBufferDiscardsEvictedRecords() {
        DropBuffer<String, String> buffer = new DropBuffer<>();
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 9));

        List<CausalRecord<String, String>> evicted = buffer.evict(BufferLimit.ofSize(1), handler, removed::add);

        assertTrue(evicted.isEmpty());
        assertEquals(0, buffer.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
        assertEquals(1, removed.size(), "dropped records must still be reported to onRemoved");
        assertEquals(0L, removed.get(0).sourceOffset());
    }

    @Test
    void deadLetterBufferRoutesEvictedRecordsToSink() {
        List<CausalRecord<String, String>> sink = new ArrayList<>();
        DeadLetterBuffer<String, String> buffer = new DeadLetterBuffer<>(sink::add);
        buffer.add(rec(ORDERS, 0), VectorClock.empty().advance(PRICES, 9));

        List<CausalRecord<String, String>> evicted = buffer.evict(BufferLimit.ofSize(1), handler, removed::add);

        assertTrue(evicted.isEmpty());
        assertEquals(1, sink.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
        assertEquals(sink, removed, "dead-lettered records must also be reported to onRemoved");
    }

    @Test
    void factoryRejectsDeadLetterWithoutSink() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> CausalBuffers.create(
                        io.parsley.BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dlq")));
    }
}
