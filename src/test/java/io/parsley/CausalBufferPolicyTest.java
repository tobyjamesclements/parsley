package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferPolicyTest {

    private final CausalBufferLimit limit = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));

    @Test
    void forwardUnsafeHoldsLimit() {
        ForwardUnsafe policy = assertInstanceOf(
                ForwardUnsafe.class, CausalBufferPolicy.forwardUnsafe(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void dropHoldsLimit() {
        Drop policy =
                assertInstanceOf(Drop.class, CausalBufferPolicy.drop(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void deadLetterHoldsLimitAndDestination() {
        DeadLetter policy = assertInstanceOf(
                DeadLetter.class, CausalBufferPolicy.deadLetter(limit, "dlq"));
        assertEquals(limit, policy.limit());
        assertEquals("dlq", policy.destination());
    }

    @Test
    void strictnessFollowsTheTaxonomy() {
        assertFalse(CausalBufferPolicy.forwardUnsafe(limit).strict(), "forwardUnsafe is lenient");
        assertTrue(CausalBufferPolicy.drop(limit).strict(), "drop is strict");
        assertTrue(CausalBufferPolicy.deadLetter(limit, "dlq").strict(), "deadLetter is strict");
    }
}
