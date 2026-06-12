package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BufferLimitTest {

    private static final BufferLimit DURATION = BufferLimit.ofDuration(Duration.ofSeconds(30));

    @Test
    void ofSizeRejectsZeroAndNegative() {
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofSize(0));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofSize(-1));
    }

    @Test
    void ofSizeAcceptsPositive() {
        assertEquals(new BufferLimit.SizeLimit(1), BufferLimit.ofSize(1));
    }

    @Test
    void ofDurationRejectsNullZeroAndNegative() {
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(null));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> BufferLimit.ofDuration(Duration.ofSeconds(-1)));
    }

    @Test
    void factoriesReturnTheMatchingVariant() {
        assertEquals(new BufferLimit.DurationLimit(Duration.ofSeconds(30)),
                BufferLimit.ofDuration(Duration.ofSeconds(30)));
        assertEquals(new BufferLimit.BytesLimit(1024L), BufferLimit.ofBytes(1024L));
        assertEquals(new BufferLimit.FrontierAdvancementLimit(10L),
                BufferLimit.ofFrontierAdvancement(10L));
    }

    @Test
    void firstRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, BufferLimit::first);
        assertThrows(IllegalArgumentException.class, () -> new BufferLimit.FirstLimit(List.of()));
    }

    @Test
    void firstRejectsNullElements() {
        assertThrows(NullPointerException.class, () -> BufferLimit.first(DURATION, null));
    }

    @Test
    void firstLimitHasValueEquality() {
        assertEquals(
                BufferLimit.first(DURATION, BufferLimit.ofSize(5)),
                BufferLimit.first(DURATION, BufferLimit.ofSize(5)));
    }

    @Test
    void firstLimitListIsDefensivelyCopiedAndImmutable() {
        BufferLimit.FirstLimit first =
                (BufferLimit.FirstLimit) BufferLimit.first(DURATION);
        assertThrows(UnsupportedOperationException.class,
                () -> first.limits().add(BufferLimit.ofSize(1)));
    }
}
