package io.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CausalViolationExceptionTest {

    private static final CausalRecord<String, String, TestClock> RECORD = new CausalRecord<>(
            "k", "v", 0L, List.of(), null,
            TestClock.empty().advance("orders-2", 100L), "orders-2@100");

    @Test
    void exceptionMessageContainsReasonAndSource() {
        CausalViolationException ex = new CausalViolationException(RECORD, CausalViolationReason.MISSING_HEADER);
        assertTrue(ex.getMessage().contains("MISSING_HEADER"));
        assertTrue(ex.getMessage().contains("orders-2@100"));
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
