package io.parsley.core;

import io.parsley.CausalRecord;
import io.parsley.CausalViolationReason;
import io.parsley.core.internal.buffer.DeadLetterBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterBufferTest {

    @Test
    void drainReleasesRecordWithSatisfiedClock() {
        var buffer = new DeadLetterBuffer<String, String, TestClock>(r -> {});
        buffer.add(record("k"), TestClock.at("input-0", 5L));

        var released = buffer.drain(TestClock.at("input-0", 5L));

        assertEquals(1, released.size());
        assertEquals("k", released.getFirst().key());
    }

    @Test
    void drainKeepsRecordWithUnsatisfiedClock() {
        var buffer = new DeadLetterBuffer<String, String, TestClock>(r -> {});
        buffer.add(record("k"), TestClock.at("input-0", 5L));

        assertTrue(buffer.drain(TestClock.at("input-0", 3L)).isEmpty());
    }

    @Test
    void evictCallsDeadLetterSinkForEachRecord() {
        List<CausalRecord<String, String, TestClock>> sink = new ArrayList<>();
        var buffer = new DeadLetterBuffer<String, String, TestClock>(sink::add);
        buffer.add(record("k1"), TestClock.at("input-0", 99L));
        buffer.add(record("k2"), TestClock.at("input-0", 100L));

        buffer.evict(null, (r, reason) -> {});

        assertEquals(2, sink.size());
    }

    @Test
    void evictCallsHandlerWithLimitReachedForEachRecord() {
        var buffer = new DeadLetterBuffer<String, String, TestClock>(r -> {});
        buffer.add(record("k1"), TestClock.at("input-0", 99L));
        buffer.add(record("k2"), TestClock.at("input-0", 100L));

        List<CausalViolationReason> violations = new ArrayList<>();
        buffer.evict(null, (r, reason) -> violations.add(reason));

        assertEquals(2, violations.size());
        assertTrue(violations.stream().allMatch(v -> v == CausalViolationReason.LIMIT_REACHED));
    }

    @Test
    void evictReturnsEmptyList() {
        var buffer = new DeadLetterBuffer<String, String, TestClock>(r -> {});
        buffer.add(record("k"), TestClock.at("input-0", 99L));

        assertTrue(buffer.evict(null, (r, reason) -> {}).isEmpty());
    }

    @Test
    void bufferIsEmptyAfterEvict() {
        var buffer = new DeadLetterBuffer<String, String, TestClock>(r -> {});
        buffer.add(record("k"), TestClock.at("input-0", 99L));

        buffer.evict(null, (r, reason) -> {});

        assertTrue(buffer.drain(TestClock.at("input-0", 99L)).isEmpty());
    }

    private static CausalRecord<String, String, TestClock> record(String key) {
        return new CausalRecord<>(key, "v", 0L, List.of(), null, TestClock.empty(), "input-0@0");
    }
}
