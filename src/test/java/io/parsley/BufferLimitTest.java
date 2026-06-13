package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BufferLimitTest {

    @Test
    void ofDurationRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(null));
    }

    @Test
    void ofDurationHoldsDuration() {
        BufferLimit.DurationLimit limit =
                assertInstanceOf(BufferLimit.DurationLimit.class, BufferLimit.ofDuration(Duration.ofSeconds(5)));
        assertEquals(Duration.ofSeconds(5), limit.duration());
    }

    @Test
    void ofSizeRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofSize(0));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofSize(-3));
    }

    @Test
    void ofSizeHoldsCount() {
        BufferLimit.SizeLimit limit =
                assertInstanceOf(BufferLimit.SizeLimit.class, BufferLimit.ofSize(10));
        assertEquals(10, limit.messages());
    }

    @Test
    void firstRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, BufferLimit::first);
    }

    @Test
    void firstHoldsConstituents() {
        BufferLimit duration = BufferLimit.ofDuration(Duration.ofSeconds(1));
        BufferLimit size = BufferLimit.ofSize(5);
        BufferLimit.FirstLimit first =
                assertInstanceOf(BufferLimit.FirstLimit.class, BufferLimit.first(duration, size));
        assertEquals(2, first.limits().size());
    }
}
