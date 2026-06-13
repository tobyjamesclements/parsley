package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalViolationExceptionTest {

    @Test
    void messageIncludesSourceCoordinateAndReason() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 3, 27L, "k", "v");
        CausalViolationException ex =
                new CausalViolationException(record, CausalViolationReason.LIMIT_REACHED);

        assertSame(record, ex.record());
        assertEquals(CausalViolationReason.LIMIT_REACHED, ex.reason());
        assertTrue(ex.getMessage().contains("orders-3@27"), ex.getMessage());
        assertTrue(ex.getMessage().contains("LIMIT_REACHED"), ex.getMessage());
    }
}
