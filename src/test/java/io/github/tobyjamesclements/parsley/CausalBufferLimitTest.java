package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalBufferLimitTest {

    /**
     * {@code CausalBufferLimit.ofDuration()} rejects a zero duration, a negative duration,
     * and {@code null}.
     *
     * Asserts that {@code IllegalArgumentException} is thrown for each invalid input.
     */
    @Test
    void ofDurationRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(Duration.ZERO),
                "zero duration must be rejected");
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(Duration.ofSeconds(-1)),
                "negative duration must be rejected");
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(null),
                "null duration must be rejected");
    }

    /**
     * {@code CausalBufferLimit.ofDuration()} creates a {@code ParsleyDurationLimit} that
     * holds the configured duration.
     *
     * Asserts that the returned instance is a {@code ParsleyDurationLimit} and that
     * {@code duration()} returns the configured value.
     */
    @Test
    void ofDurationHoldsDuration() {
        ParsleyDurationLimit limit =
                assertInstanceOf(ParsleyDurationLimit.class, CausalBufferLimit.ofDuration(Duration.ofSeconds(5)));
        assertEquals(Duration.ofSeconds(5), limit.duration(), "configured duration must be preserved");
    }

    /**
     * {@code CausalBufferLimit.ofSize()} rejects zero and negative sizes.
     *
     * Asserts that {@code IllegalArgumentException} is thrown for each invalid input.
     */
    @Test
    void ofSizeRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofSize(0),
                "size of zero must be rejected");
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofSize(-3),
                "negative size must be rejected");
    }

    /**
     * {@code CausalBufferLimit.ofSize()} creates a {@code ParsleySizeLimit} that holds the
     * configured message count.
     *
     * Asserts that the returned instance is a {@code ParsleySizeLimit} and that
     * {@code messages()} returns the configured count.
     */
    @Test
    void ofSizeHoldsCount() {
        ParsleySizeLimit limit =
                assertInstanceOf(ParsleySizeLimit.class, CausalBufferLimit.ofSize(10));
        assertEquals(10, limit.messages(), "configured message count must be preserved");
    }

    /**
     * {@code CausalBufferLimit.first()} rejects an empty argument list.
     *
     * Asserts that {@code IllegalArgumentException} is thrown when called with no arguments.
     */
    @Test
    void firstRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, CausalBufferLimit::first,
                "first() with no arguments must be rejected");
    }

    /**
     * {@code CausalBufferLimit.first()} creates a {@code ParsleyFirstLimit} that holds all
     * constituent limits.
     *
     * Asserts that the returned instance is a {@code ParsleyFirstLimit} containing the
     * two configured constituents.
     */
    @Test
    void firstHoldsConstituents() {
        CausalBufferLimit duration = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));
        CausalBufferLimit size = CausalBufferLimit.ofSize(5);
        ParsleyFirstLimit first =
                assertInstanceOf(ParsleyFirstLimit.class, CausalBufferLimit.first(duration, size));
        assertEquals(2, first.limits().size(), "first() must hold both configured limits");
    }
}
