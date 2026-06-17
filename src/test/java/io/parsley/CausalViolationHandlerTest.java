package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalViolationHandlerTest {

    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("orders", 2, 5L, "k", "v");
    private final CausalViolation violation = new CausalViolation(
            record, CausalViolationReason.MISSING_HEADER,
            CausalFrontier.empty(), CausalDependencies.empty(), List.of());

    @Test
    void throwingThrowsWithRecordAndReason() {
        CausalViolationException ex = assertThrows(CausalViolationException.class,
                () -> CausalViolationHandler.throwOnViolation().onViolation(violation));
        assertSame(record, ex.record());
        assertEquals(CausalViolationReason.MISSING_HEADER, ex.reason());
    }

    @Test
    void lambdaReceivesTheViolationWithItsGap() {
        CausalViolation limit = new CausalViolation(
                record, CausalViolationReason.LIMIT_REACHED,
                CausalFrontier.empty(), CausalDependencies.empty().advance(CausalPosition.deriveUuid("prices"), 0, 4),
                List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 5L)));

        AtomicReference<CausalViolation> seen = new AtomicReference<>();
        CausalViolationHandler handler = seen::set;
        handler.onViolation(limit);

        assertEquals(CausalViolationReason.LIMIT_REACHED, seen.get().reason());
        assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 5L)), seen.get().gap());
    }
}
