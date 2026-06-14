package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ViolationHandlerTest {

    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("orders", 2, 5L, "k", "v");
    private final Violation violation = new Violation(
            record, CausalViolationReason.MISSING_HEADER,
            VectorClock.empty(), VectorClock.empty(), Map.of());

    @Test
    void throwingThrowsWithRecordAndReason() {
        CausalViolationException ex = assertThrows(CausalViolationException.class,
                () -> ViolationHandler.throwing().onViolation(violation));
        assertSame(record, ex.record());
        assertEquals(CausalViolationReason.MISSING_HEADER, ex.reason());
    }

    @Test
    void noopIgnores() {
        assertDoesNotThrow(() -> ViolationHandler.noop().onViolation(violation));
    }

    @Test
    void lambdaReceivesTheViolationWithItsGap() {
        TopicPartition prices = new TopicPartition("prices", 0);
        Violation limit = new Violation(
                record, CausalViolationReason.LIMIT_REACHED,
                VectorClock.empty(), VectorClock.empty().advance(prices, 4), Map.of(prices, 5L));

        AtomicReference<Violation> seen = new AtomicReference<>();
        ViolationHandler handler = seen::set;
        handler.onViolation(limit);

        assertEquals(CausalViolationReason.LIMIT_REACHED, seen.get().reason());
        assertEquals(Map.of(prices, 5L), seen.get().gap());
    }
}
