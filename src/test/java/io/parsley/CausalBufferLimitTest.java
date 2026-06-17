package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalBufferLimitTest {

    @Test
    void ofDurationRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofDuration(null));
    }

    @Test
    void ofDurationHoldsDuration() {
        ParsleyDurationLimit limit =
                assertInstanceOf(ParsleyDurationLimit.class, CausalBufferLimit.ofDuration(Duration.ofSeconds(5)));
        assertEquals(Duration.ofSeconds(5), limit.duration());
    }

    @Test
    void ofSizeRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofSize(0));
        assertThrows(IllegalArgumentException.class, () -> CausalBufferLimit.ofSize(-3));
    }

    @Test
    void ofSizeHoldsCount() {
        ParsleySizeLimit limit =
                assertInstanceOf(ParsleySizeLimit.class, CausalBufferLimit.ofSize(10));
        assertEquals(10, limit.messages());
    }

    @Test
    void firstRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, CausalBufferLimit::first);
    }

    @Test
    void firstHoldsConstituents() {
        CausalBufferLimit duration = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));
        CausalBufferLimit size = CausalBufferLimit.ofSize(5);
        ParsleyFirstLimit first =
                assertInstanceOf(ParsleyFirstLimit.class, CausalBufferLimit.first(duration, size));
        assertEquals(2, first.limits().size());
    }
}
