package io.parsley;

import java.time.Duration;

/**
 * Evicts records buffered longer than {@code duration}.
 */
record DurationLimit(Duration duration) implements CausalBufferLimit {
    DurationLimit {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("duration must be positive, was " + duration);
        }
    }
}
