package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BufferingPolicyTest {

    private final BufferLimit limit = BufferLimit.ofDuration(Duration.ofSeconds(1));

    @Test
    void ignoreHoldsLimit() {
        BufferingPolicy.Ignore policy =
                assertInstanceOf(BufferingPolicy.Ignore.class, BufferingPolicy.ignore(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void dropHoldsLimit() {
        BufferingPolicy.Drop policy =
                assertInstanceOf(BufferingPolicy.Drop.class, BufferingPolicy.drop(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void deadLetterHoldsLimitAndDestination() {
        BufferingPolicy.DeadLetter policy = assertInstanceOf(
                BufferingPolicy.DeadLetter.class, BufferingPolicy.deadLetter(limit, "dlq"));
        assertEquals(limit, policy.limit());
        assertEquals("dlq", policy.destination());
    }
}
