package io.parsley.buffer;

import io.parsley.CausalViolationReason;
import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.internal.ImmutableVectorClock;
import io.parsley.serialisation.DefaultVectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class IgnoreBufferTest {

    private static final DefaultVectorClockSerialiser SERIALISER = new DefaultVectorClockSerialiser();
    private static final Partition P0 = new Partition("input", 0);

    @Test
    void drainReleasesRecordWithSatisfiedClock() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("k", new ImmutableVectorClock(Map.of(P0, 5L))));

        List<ConsumerRecord<String, String>> released = buffer.drain(new ImmutableVectorClock(Map.of(P0, 5L)));

        assertEquals(1, released.size());
        assertEquals("k", released.get(0).key());
    }

    @Test
    void drainKeepsRecordWithUnsatisfiedClock() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("k", new ImmutableVectorClock(Map.of(P0, 5L))));

        assertTrue(buffer.drain(new ImmutableVectorClock(Map.of(P0, 3L))).isEmpty());
    }

    @Test
    void drainReleasesOnlySatisfiedRecords() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("early", new ImmutableVectorClock(Map.of(P0, 2L))));
        buffer.add(record("late", new ImmutableVectorClock(Map.of(P0, 10L))));

        List<ConsumerRecord<String, String>> released = buffer.drain(new ImmutableVectorClock(Map.of(P0, 2L)));

        assertEquals(1, released.size());
        assertEquals("early", released.get(0).key());
    }

    @Test
    void drainNoHeaderTreatedAsEmptyClock() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("k", null));

        List<ConsumerRecord<String, String>> released = buffer.drain(ImmutableVectorClock.empty());
        assertEquals(1, released.size());
    }

    @Test
    void evictForwardsAllAndCallsHandler() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("k1", new ImmutableVectorClock(Map.of(P0, 99L))));
        buffer.add(record("k2", new ImmutableVectorClock(Map.of(P0, 100L))));

        List<CausalViolationReason> violations = new ArrayList<>();
        List<ConsumerRecord<String, String>> evicted = buffer.evict(null, (r, reason) -> violations.add(reason));

        assertEquals(2, evicted.size());
        assertEquals(2, violations.size());
        assertTrue(violations.stream().allMatch(r -> r == CausalViolationReason.LIMIT_REACHED));
    }

    @Test
    void drainAfterEvictIsEmpty() {
        IgnoreBuffer<String, String> buffer = new IgnoreBuffer<>(SERIALISER);
        buffer.add(record("k", new ImmutableVectorClock(Map.of(P0, 99L))));

        buffer.evict(null, (r, reason) -> {});
        assertTrue(buffer.drain(new ImmutableVectorClock(Map.of(P0, 99L))).isEmpty());
    }

    private static ConsumerRecord<String, String> record(String key, VectorClock clock) {
        Headers headers = new RecordHeaders();
        if (clock != null) {
            headers.add(new RecordHeader(IgnoreBuffer.CLOCK_HEADER, SERIALISER.serialise(clock)));
        }
        return new ConsumerRecord<>(
                "input", 0, 0L,
                0L, TimestampType.NO_TIMESTAMP_TYPE,
                -1, -1,
                key, "v",
                headers, Optional.empty());
    }
}
