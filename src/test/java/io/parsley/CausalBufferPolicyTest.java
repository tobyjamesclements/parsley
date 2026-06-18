package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.parsley.ViolationAction.DEAD_LETTER;
import static io.parsley.ViolationAction.DROP;
import static io.parsley.ViolationAction.FORWARD_UNSAFE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBufferPolicyTest {

    private final CausalBufferLimit limit = CausalBufferLimit.ofDuration(Duration.ofSeconds(1));

    @Test
    void forwardUnsafe_setsAllActionsToForwardUnsafeAndDoesNotRequireSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.forwardUnsafe(limit);
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.MISSING_HEADER));
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES));
        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.LIMIT_REACHED));
        assertEquals(limit, policy.limit());
        assertFalse(policy.requiresDeadLetterSink());
    }

    @Test
    void drop_setsAllActionsToDropAndDoesNotRequireSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.drop(limit);
        assertEquals(DROP, policy.actionFor(CausalViolationReason.MISSING_HEADER));
        assertEquals(DROP, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES));
        assertEquals(DROP, policy.actionFor(CausalViolationReason.LIMIT_REACHED));
        assertEquals(limit, policy.limit());
        assertFalse(policy.requiresDeadLetterSink());
    }

    @Test
    void deadLetter_setsAllActionsToDeadLetterAndRequiresSink() {
        CausalBufferPolicy policy = CausalBufferPolicy.deadLetter(limit);
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.MISSING_HEADER));
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES));
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.LIMIT_REACHED));
        assertEquals(limit, policy.limit());
        assertTrue(policy.requiresDeadLetterSink());
    }

    @Test
    void builder_configuresPerViolationTypeActions() {
        CausalBufferPolicy policy = CausalBufferPolicy.builder()
                .onMissing(FORWARD_UNSAFE)
                .onUnresolvable(DROP)
                .onLimit(DEAD_LETTER)
                .setLimit(limit)
                .build();

        assertEquals(FORWARD_UNSAFE, policy.actionFor(CausalViolationReason.MISSING_HEADER));
        assertEquals(DROP, policy.actionFor(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES));
        assertEquals(DEAD_LETTER, policy.actionFor(CausalViolationReason.LIMIT_REACHED));
        assertEquals(limit, policy.limit());
        assertTrue(policy.requiresDeadLetterSink());
    }

    @Test
    void builder_requiresDeadLetterSinkOnlyWhenAnyActionIsDeadLetter() {
        CausalBufferPolicy noSinkNeeded = CausalBufferPolicy.builder()
                .onMissing(FORWARD_UNSAFE).onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build();
        assertFalse(noSinkNeeded.requiresDeadLetterSink());

        CausalBufferPolicy sinkNeeded = CausalBufferPolicy.builder()
                .onMissing(DEAD_LETTER).onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build();
        assertTrue(sinkNeeded.requiresDeadLetterSink());
    }

    @Test
    void builder_requiresAllSettingsToBeProvided() {
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onUnresolvable(DROP).onLimit(DROP).setLimit(limit).build(),
                "missing onMissing");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onLimit(DROP).setLimit(limit).build(),
                "missing onUnresolvable");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onUnresolvable(DROP).setLimit(limit).build(),
                "missing onLimit");
        assertThrows(IllegalStateException.class, () ->
                CausalBufferPolicy.builder().onMissing(DROP).onUnresolvable(DROP).onLimit(DROP).build(),
                "missing setLimit");
    }
}
