package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalViolationExceptionTest {

    /**
     * {@code CausalViolationException} is constructed with the offending record. Its message
     * includes the source coordinate (topic-partition@offset) and the literal {@code EVICTED}
     * marker — eviction is the only thing that can produce this exception — so callers can
     * diagnose failures without unwrapping the exception.
     *
     * Asserts that {@code record()} returns the original instance, and that the message
     * contains both the source coordinate and {@code EVICTED}.
     */
    @Test
    void messageIncludesSourceCoordinateAndEvictedMarker() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t1", 3, 27L, "k", "v");
        CausalViolationException ex = new CausalViolationException(record);

        assertSame(record, ex.record(), "exception must carry the original record");
        assertTrue(ex.getMessage().contains("t1-3@27"),
                "message must contain the source coordinate; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("EVICTED"),
                "message must contain the EVICTED marker; got: " + ex.getMessage());
    }
}
