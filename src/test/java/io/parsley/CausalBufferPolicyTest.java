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
        ForwardUnsafePolicy policy = assertInstanceOf(
                ForwardUnsafePolicy.class, CausalBufferPolicy.forwardUnsafe(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void dropHoldsLimit() {
        DropPolicy policy =
                assertInstanceOf(DropPolicy.class, CausalBufferPolicy.drop(limit));
        assertEquals(limit, policy.limit());
    }

    @Test
    void deadLetterHoldsLimitAndDestination() {
        DeadLetterPolicy policy = assertInstanceOf(
                DeadLetterPolicy.class, CausalBufferPolicy.deadLetter(limit, "dlq"));
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
