package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalViolationExceptionTest {

    /**
     * {@code CausalViolationException} is constructed with a record and a reason.
     * Its message includes the source coordinate (topic-partition@offset) and the reason
     * name so callers can diagnose failures without unwrapping the exception.
     *
     * Asserts that {@code record()} and {@code reason()} return the original values, and
     * that the message contains both the source coordinate and the reason name.
     */
    @Test
    void messageIncludesSourceCoordinateAndReason() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t1", 3, 27L, "k", "v");
        CausalViolationException ex =
                new CausalViolationException(record, CausalViolationReason.LIMIT_REACHED);

        assertSame(record, ex.record(), "exception must carry the original record");
        assertEquals(CausalViolationReason.LIMIT_REACHED, ex.reason(),
                "exception must carry the violation reason");
        assertTrue(ex.getMessage().contains("t1-3@27"),
                "message must contain the source coordinate; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("LIMIT_REACHED"),
                "message must contain the reason name; got: " + ex.getMessage());
    }
}
