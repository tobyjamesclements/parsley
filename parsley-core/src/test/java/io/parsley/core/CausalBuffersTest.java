package io.parsley.core;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class CausalBuffersTest {

    private static final BufferLimit LIMIT = BufferLimit.ofDuration(Duration.ofSeconds(30));

    @Test
    void createIgnoreReturnsBuffer() {
        assertNotNull(CausalBuffers.<String, String, TestClock>create(BufferingPolicy.ignore(LIMIT)));
    }

    @Test
    void createDropReturnsBuffer() {
        assertNotNull(CausalBuffers.<String, String, TestClock>create(BufferingPolicy.drop(LIMIT)));
    }

    @Test
    void createDeadLetterWithSingleArgThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> CausalBuffers.create(BufferingPolicy.deadLetter(LIMIT, "dead-topic")));
    }

    @Test
    void createDeadLetterWithSinkReturnsBuffer() {
        var policy = new BufferingPolicy.DeadLetter(LIMIT, "dead-topic");
        assertNotNull(CausalBuffers.<String, String, TestClock>create(policy, r -> {}));
    }
}
