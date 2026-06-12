package io.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CausalViolationHandlerTest {

    private static final CausalRecord<String, String, TestClock> RECORD = new CausalRecord<>(
            "k", "v", 0L, List.of(), null,
            TestClock.empty().advance("orders-0", 42L), "orders-0@42");

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
