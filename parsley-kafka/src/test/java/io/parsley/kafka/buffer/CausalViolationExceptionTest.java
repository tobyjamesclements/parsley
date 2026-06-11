package io.parsley.kafka.buffer;

import io.parsley.CausalViolationReason;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CausalViolationExceptionTest {

    private static final ConsumerRecord<String, String> RECORD = new ConsumerRecord<>(
            "orders", 2, 100L,
            0L, TimestampType.NO_TIMESTAMP_TYPE,
            -1, -1,
            "k", "v",
            new RecordHeaders(), Optional.empty());

    @Test
    void exceptionMessageContainsReasonTopicPartitionAndOffset() {
        CausalViolationException ex = new CausalViolationException(RECORD, CausalViolationReason.MISSING_HEADER);
        assertTrue(ex.getMessage().contains("MISSING_HEADER"));
        assertTrue(ex.getMessage().contains("orders"));
        assertTrue(ex.getMessage().contains("2"));
        assertTrue(ex.getMessage().contains("100"));
    }

    @Test
    void recordAccessorReturnsOriginalRecord() {
        CausalViolationException ex = new CausalViolationException(RECORD, CausalViolationReason.LIMIT_REACHED);
        assertSame(RECORD, ex.record());
    }

    @Test
    void reasonAccessorReturnsOriginalReason() {
        CausalViolationException ex = new CausalViolationException(RECORD, CausalViolationReason.UNRESOLVABLE_CLOCK);
        assertEquals(CausalViolationReason.UNRESOLVABLE_CLOCK, ex.reason());
    }

    @Test
    void exceptionIsRuntimeException() {
        assertInstanceOf(RuntimeException.class,
                new CausalViolationException(RECORD, CausalViolationReason.MISSING_HEADER));
    }
}
