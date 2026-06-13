package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class MetricsTest {

    @Test
    void noopAcceptsEveryCallback() {
        Metrics metrics = Metrics.noop();
        assertDoesNotThrow(() -> {
            metrics.onMessageBuffered();
            metrics.onMessageReleased(Duration.ofMillis(10));
            metrics.onViolation(CausalViolationReason.MISSING_HEADER);
            metrics.onFrontierAdvanced(VectorClock.empty());
        });
    }
}
