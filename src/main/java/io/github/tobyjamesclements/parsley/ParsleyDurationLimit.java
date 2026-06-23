package io.github.tobyjamesclements.parsley;

import java.time.Duration;

/**
 * Evicts records buffered longer than {@code duration}.
 */
record ParsleyDurationLimit(Duration duration) implements CausalBufferLimit {
    ParsleyDurationLimit {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("duration must be positive, was " + duration);
        }
    }
}
