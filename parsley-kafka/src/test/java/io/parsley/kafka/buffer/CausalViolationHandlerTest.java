package io.parsley.kafka.buffer;

import io.parsley.CausalViolationReason;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CausalViolationHandlerTest {

    private static final ConsumerRecord<String, String> RECORD = new ConsumerRecord<>(
            "orders", 0, 42L,
            0L, TimestampType.NO_TIMESTAMP_TYPE,
            -1, -1,
            "k", "v",
            new RecordHeaders(), Optional.empty());

    @Test
    void throwingHandlerThrowsCausalViolationException() {
        CausalViolationHandler handler = CausalViolationHandler.throwing();
        assertThrows(CausalViolationException.class,
                () -> handler.onViolation(RECORD, CausalViolationReason.MISSING_HEADER));
    }

    @Test
    void throwingHandlerExceptionContainsRecordAndReason() {
        CausalViolationHandler handler = CausalViolationHandler.throwing();
        CausalViolationException ex = assertThrows(CausalViolationException.class,
                () -> handler.onViolation(RECORD, CausalViolationReason.LIMIT_REACHED));
        assertSame(RECORD, ex.record());
        assertEquals(CausalViolationReason.LIMIT_REACHED, ex.reason());
    }

    @Test
    void noopHandlerDoesNotThrow() {
        CausalViolationHandler handler = CausalViolationHandler.noop();
        assertDoesNotThrow(() -> handler.onViolation(RECORD, CausalViolationReason.UNRESOLVABLE_CLOCK));
    }
}
