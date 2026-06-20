package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalViolationHandlerTest {

    private static final Uuid T1_ID = CausalPosition.deriveUuid("t1");

    private final ConsumerRecord<String, String> record =
            new ConsumerRecord<>("t1", 2, 5L, "k", "v");
    private final CausalViolation violation = new CausalViolation(
            record, CausalFrontier.empty(), CausalDependencies.empty(), List.of());

    /**
     * {@code CausalViolationHandler.throwOnViolation()} returns a handler that throws a
     * {@code CausalViolationException} carrying the originating record.
     *
     * Asserts that the thrown exception holds the same record instance.
     */
    @Test
    void throwOnViolationThrowsExceptionCarryingRecord() {
        CausalViolationException ex = assertThrows(CausalViolationException.class,
                () -> CausalViolationHandler.throwOnViolation().onViolation(violation));
        assertSame(record, ex.record(), "exception must carry the original record");
    }

    /**
     * A lambda handler receives the full {@code CausalViolation} including the gap list
     * that describes the shortfall between the required and current frontier positions.
     *
     * Asserts that the handler receives the correct gap.
     */
    @Test
    void lambdaHandlerReceivesViolationWithGap() {
        CausalViolation limitViolation = new CausalViolation(
                record,
                CausalFrontier.empty(),
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 4)).build(),
                List.of(new CausalPosition(T1_ID, 0, 5L)));

        AtomicReference<CausalViolation> seen = new AtomicReference<>();
        CausalViolationHandler handler = seen::set;
        handler.onViolation(limitViolation);

        assertEquals(List.of(new CausalPosition(T1_ID, 0, 5L)), seen.get().gap(),
                "handler must receive the correct gap");
    }
}
