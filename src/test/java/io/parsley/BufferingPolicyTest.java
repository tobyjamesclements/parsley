package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferingPolicyTest {

    private final BufferLimit limit = BufferLimit.ofDuration(Duration.ofSeconds(1));

    @Test
    void forwardUnsafeHoldsLimit() {
        BufferingPolicy.ForwardUnsafe policy = assertInstanceOf(
                BufferingPolicy.ForwardUnsafe.class, BufferingPolicy.forwardUnsafe(limit));
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

    @Test
    void strictnessFollowsTheTaxonomy() {
        assertFalse(BufferingPolicy.forwardUnsafe(limit).strict(), "forwardUnsafe is lenient");
        assertTrue(BufferingPolicy.drop(limit).strict(), "drop is strict");
        assertTrue(BufferingPolicy.deadLetter(limit, "dlq").strict(), "deadLetter is strict");
    }
}
