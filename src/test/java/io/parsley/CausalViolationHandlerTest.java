package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalViolationHandlerTest {

    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("orders", 2, 5L, "k", "v");

    @Test
    void throwingThrowsWithRecordAndReason() {
        CausalViolationException ex = assertThrows(CausalViolationException.class,
                () -> CausalViolationHandler.throwing()
                        .onViolation(record, CausalViolationReason.MISSING_HEADER));
        assertSame(record, ex.record());
        assertEquals(CausalViolationReason.MISSING_HEADER, ex.reason());
    }

    @Test
    void noopIgnores() {
        assertDoesNotThrow(() -> CausalViolationHandler.noop()
                .onViolation(record, CausalViolationReason.LIMIT_REACHED));
    }

    @Test
    void lambdaReceivesRecordAndReason() {
        AtomicReference<CausalViolationReason> seen = new AtomicReference<>();
        CausalViolationHandler handler = (r, reason) -> seen.set(reason);
        handler.onViolation(record, CausalViolationReason.UNRESOLVABLE_CLOCK);
        assertEquals(CausalViolationReason.UNRESOLVABLE_CLOCK, seen.get());
    }
}
