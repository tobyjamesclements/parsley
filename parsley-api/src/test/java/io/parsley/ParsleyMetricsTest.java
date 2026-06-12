package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ParsleyMetricsTest {

    @Test
    void noopAcceptsAllCallbacksWithoutEffect() {
        ParsleyMetrics noop = ParsleyMetrics.noop();
        assertNotNull(noop);
        assertDoesNotThrow(() -> {
            noop.onMessageBuffered();
            noop.onMessageReleased(Duration.ofMillis(5));
            noop.onViolation(CausalViolationReason.LIMIT_REACHED);
            noop.onFrontierAdvanced(TestClock.empty());
        });
    }
}
