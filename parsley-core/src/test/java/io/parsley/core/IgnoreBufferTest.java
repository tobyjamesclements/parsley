package io.parsley.core;

import io.parsley.CausalRecord;
import io.parsley.CausalViolationReason;
import io.parsley.core.internal.buffer.IgnoreBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IgnoreBufferTest {

    @Test
    void drainReleasesRecordWithSatisfiedClock() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("k"), TestClock.at("input-0", 5L));

        var released = buffer.drain(TestClock.at("input-0", 5L));

        assertEquals(1, released.size());
        assertEquals("k", released.getFirst().key());
    }

    @Test
    void drainKeepsRecordWithUnsatisfiedClock() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("k"), TestClock.at("input-0", 5L));

        assertTrue(buffer.drain(TestClock.at("input-0", 3L)).isEmpty());
    }

    @Test
    void drainReleasesOnlySatisfiedRecords() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("early"), TestClock.at("input-0", 2L));
        buffer.add(record("late"), TestClock.at("input-0", 10L));

        var released = buffer.drain(TestClock.at("input-0", 2L));

        assertEquals(1, released.size());
        assertEquals("early", released.getFirst().key());
    }

    @Test
    void drainEmptyDependenciesAlwaysReleased() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("k"), TestClock.empty());

        assertEquals(1, buffer.drain(TestClock.empty()).size());
    }

    @Test
    void evictForwardsAllAndCallsHandler() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("k1"), TestClock.at("input-0", 99L));
        buffer.add(record("k2"), TestClock.at("input-0", 100L));

        List<CausalViolationReason> violations = new ArrayList<>();
        var evicted = buffer.evict(null, (r, reason) -> violations.add(reason));

        assertEquals(2, evicted.size());
        assertEquals(2, violations.size());
        assertTrue(violations.stream().allMatch(r -> r == CausalViolationReason.LIMIT_REACHED));
    }

    @Test
    void drainAfterEvictIsEmpty() {
        var buffer = new IgnoreBuffer<String, String, TestClock>();
        buffer.add(record("k"), TestClock.at("input-0", 99L));

        buffer.evict(null, (r, reason) -> {});
        assertTrue(buffer.drain(TestClock.at("input-0", 99L)).isEmpty());
        assertEquals(0, buffer.size());
    }

    private static CausalRecord<String, String, TestClock> record(String key) {
        return new CausalRecord<>(key, "v", 0L, List.of(), null, TestClock.empty(), "input-0@0");
    }
}
