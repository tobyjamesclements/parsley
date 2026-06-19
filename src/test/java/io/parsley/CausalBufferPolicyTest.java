package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.parsley.CausalViolationAction.DEAD_LETTER;
import static io.parsley.CausalViolationAction.DROP;
import static io.parsley.CausalViolationAction.FORWARD_UNSAFE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferPolicyTest {

    private final CausalBufferLimit limit = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));

    /**
     * {@code CausalBufferPolicy.forwardUnsafe()} creates a policy that applies
     * {@code FORWARD_UNSAFE} to all three violation types and does not require a
     * dead-letter sink.
     *
     * Asserts that all three actions are {@code FORWARD_UNSAFE}, the limit is preserved,
     * and {@code requiresDeadLetterSink()} returns {@code false}.
     */
    @Test
    void forwardUnsafePolicySetsAllActionsAndRequiresNoSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.forwardUnsafe(limit);
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.MISSING_HEADER),
                "MISSING_HEADER action must be FORWARD_UNSAFE");
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES),
                "UNRESOLVABLE_DEPENDENCIES action must be FORWARD_UNSAFE");
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.LIMIT_REACHED),
                "LIMIT_REACHED action must be FORWARD_UNSAFE");
        assertEquals(limit, policy.limit(), "limit must be preserved");
        assertFalse(policy.requiresDeadLetterSink(), "forward-unsafe policy must not require a dead-letter sink");
    }

    /**
     * {@code CausalBufferPolicy.drop()} creates a policy that applies {@code DROP} to all
     * three violation types and does not require a dead-letter sink.
     *
     * Asserts that all three actions are {@code DROP}, the limit is preserved, and
     * {@code requiresDeadLetterSink()} returns {@code false}.
     */
    @Test
    void dropPolicySetsAllActionsAndRequiresNoSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.drop(limit);
        assertEquals(DROP, policy.actionFor(CausalViolationReason.MISSING_HEADER),
                "MISSING_HEADER action must be DROP");
        assertEquals(DROP, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES),
                "UNRESOLVABLE_DEPENDENCIES action must be DROP");
        assertEquals(DROP, policy.actionFor(CausalViolationReason.LIMIT_REACHED),
                "LIMIT_REACHED action must be DROP");
        assertEquals(limit, policy.limit(), "limit must be preserved");
        assertFalse(policy.requiresDeadLetterSink(), "drop policy must not require a dead-letter sink");
    }

    /**
     * {@code CausalBufferPolicy.deadLetter()} creates a policy that applies
     * {@code DEAD_LETTER} to all three violation types and signals that a dead-letter
     * sink is required.
     *
     * Asserts that all three actions are {@code DEAD_LETTER}, the limit is preserved,
     * and {@code requiresDeadLetterSink()} returns {@code true}.
     */
    @Test
    void deadLetterPolicySetsAllActionsAndRequiresSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.deadLetter(limit);
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.MISSING_HEADER),
                "MISSING_HEADER action must be DEAD_LETTER");
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES),
                "UNRESOLVABLE_DEPENDENCIES action must be DEAD_LETTER");
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.LIMIT_REACHED),
                "LIMIT_REACHED action must be DEAD_LETTER");
        assertEquals(limit, policy.limit(), "limit must be preserved");
        assertTrue(policy.requiresDeadLetterSink(), "dead-letter policy must require a dead-letter sink");
    }

    /**
     * The builder allows a different action to be configured for each violation type
     * independently.
     *
     * Asserts that each violation type maps to the action it was configured with, and
     * that {@code requiresDeadLetterSink()} reflects the presence of a DEAD_LETTER action.
     */
    @Test
    void builderConfiguresPerViolationTypeActions() {
        CausalBufferPolicy policy = CausalBufferPolicy.builder()
                .onMissing(FORWARD_UNSAFE)
                .onUnresolvable(DROP)
                .onLimit(DEAD_LETTER)
                .setLimit(limit)
                .build();

        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.MISSING_HEADER),
                "MISSING_HEADER action must be FORWARD_UNSAFE as configured");
        assertEquals(DROP, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES),
                "UNRESOLVABLE_DEPENDENCIES action must be DROP as configured");
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.LIMIT_REACHED),
                "LIMIT_REACHED action must be DEAD_LETTER as configured");
        assertEquals(limit, policy.limit(), "limit must be preserved");
        assertTrue(policy.requiresDeadLetterSink(),
                "policy with any DEAD_LETTER action must require a dead-letter sink");
    }

    /**
     * {@code requiresDeadLetterSink()} returns {@code false} when no violation type is
     * configured with {@code DEAD_LETTER}, and {@code true} when at least one is.
     *
     * Asserts the flag value for both a policy with no dead-letter actions and one with
     * exactly one.
     */
    @Test
    void builderRequiresSinkOnlyWhenAnyActionIsDeadLetter() {
        CausalBufferPolicy noSinkNeeded = CausalBufferPolicy.builder()
                .onMissing(FORWARD_UNSAFE).onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build();
        assertFalse(noSinkNeeded.requiresDeadLetterSink(),
                "policy with no DEAD_LETTER actions must not require a sink");

        CausalBufferPolicy sinkNeeded = CausalBufferPolicy.builder()
                .onMissing(DEAD_LETTER).onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build();
        assertTrue(sinkNeeded.requiresDeadLetterSink(),
                "policy with at least one DEAD_LETTER action must require a sink");
    }

    /**
     * The builder enforces that all four settings (onMissing, onUnresolvable, onLimit,
     * setLimit) are provided before {@code build()} is called. Omitting any one throws
     * {@code IllegalStateException}.
     *
     * Asserts that each of the four possible omissions individually triggers the exception.
     */
    @Test
    void builderRejectsWhenAnySettingIsMissing() {
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build(),
                "missing onMissing must throw");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onLimit(DROP).setLimit(limit).build(),
                "missing onUnresolvable must throw");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onUnresolvable(DROP).setLimit(limit).build(),
                "missing onLimit must throw");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onUnresolvable(DROP).onLimit(DROP).build(),
                "missing setLimit must throw");
    }
}
