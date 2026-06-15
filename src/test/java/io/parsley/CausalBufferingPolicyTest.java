package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferingPolicyTest {

    private final CausalBufferLimit limit = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));

    @Test
    void forwardUnsafeHoldsLimit() {
        CausalBufferingPolicy.ForwardUnsafe policy = assertInstanceOf(
                CausalBufferingPolicy.ForwardUnsafe.class, CausalBufferingPolicy.forwardUnsafe(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void dropHoldsLimit() {
        CausalBufferingPolicy.Drop policy =
                assertInstanceOf(CausalBufferingPolicy.Drop.class, CausalBufferingPolicy.drop(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void deadLetterHoldsLimitAndDestination() {
        CausalBufferingPolicy.DeadLetter policy = assertInstanceOf(
                CausalBufferingPolicy.DeadLetter.class, CausalBufferingPolicy.deadLetter(limit, "dlq"));
        assertEquals(limit, policy.limit());
        assertEquals("dlq", policy.destination());
    }

    @Test
    void strictnessFollowsTheTaxonomy() {
        assertFalse(CausalBufferingPolicy.forwardUnsafe(limit).strict(), "forwardUnsafe is lenient");
        assertTrue(CausalBufferingPolicy.drop(limit).strict(), "drop is strict");
        assertTrue(CausalBufferingPolicy.deadLetter(limit, "dlq").strict(), "deadLetter is strict");
    }
}
