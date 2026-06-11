package io.parsley.kafka.buffer;

import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.internal.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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

class DropBufferTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final TopicPartition P0 = new TopicPartition("input", 0);

    @Test
    void drainReleasesRecordWithSatisfiedClock() {
        DropBuffer<String, String> buffer = new DropBuffer<>(SERIALISER);
        buffer.add(record("k", new KafkaVectorClock(Map.of(P0, 5L))));

        List<ConsumerRecord<String, String>> released = buffer.drain(new KafkaVectorClock(Map.of(P0, 5L)));

        assertEquals(1, released.size());
        assertEquals("k", released.get(0).key());
    }

    @Test
    void drainKeepsRecordWithUnsatisfiedClock() {
        DropBuffer<String, String> buffer = new DropBuffer<>(SERIALISER);
        buffer.add(record("k", new KafkaVectorClock(Map.of(P0, 5L))));

        assertTrue(buffer.drain(new KafkaVectorClock(Map.of(P0, 3L))).isEmpty());
    }

    @Test
    void evictDropsAllAndCallsHandler() {
        DropBuffer<String, String> buffer = new DropBuffer<>(SERIALISER);
        buffer.add(record("k1", new KafkaVectorClock(Map.of(P0, 99L))));
        buffer.add(record("k2", new KafkaVectorClock(Map.of(P0, 100L))));

        List<CausalViolationReason> violations = new ArrayList<>();
        List<ConsumerRecord<String, String>> evicted = buffer.evict(null, (r, reason) -> violations.add(reason));

        assertTrue(evicted.isEmpty(), "drop policy must not forward evicted records");
        assertEquals(2, violations.size());
        assertTrue(violations.stream().allMatch(r -> r == CausalViolationReason.LIMIT_REACHED));
    }

    @Test
    void evictClearsBuffer() {
        DropBuffer<String, String> buffer = new DropBuffer<>(SERIALISER);
        buffer.add(record("k", new KafkaVectorClock(Map.of(P0, 99L))));

        buffer.evict(null, (r, reason) -> {});
        assertTrue(buffer.drain(new KafkaVectorClock(Map.of(P0, 99L))).isEmpty());
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
