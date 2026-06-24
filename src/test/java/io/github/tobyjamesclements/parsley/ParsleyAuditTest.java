package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ParsleyAuditTest {

    /**
     * {@link CausalAudit#NOOP} is returned unwrapped, since there is no client behaviour to
     * protect against when no audit is registered.
     *
     * Asserts that {@code wrap(NOOP)} returns the same instance.
     */
    @Test
    void wrapReturnsNoopUnwrapped() {
        assertSame(CausalAudit.NOOP, ParsleyAudit.wrap(CausalAudit.NOOP),
                "wrapping NOOP must not allocate a decorator");
    }

    /**
     * A delegate that throws from every method must never propagate the exception — the caller
     * (the causal engine) must see every call complete normally.
     *
     * Asserts that all eight {@code CausalAudit} methods return without throwing when the
     * delegate throws from each.
     */
    @Test
    void throwingDelegateNeverPropagates() {
        CausalAudit audit = ParsleyAudit.wrap(new ThrowingAudit());

        audit.recordForwarded("t1", 0, 1L);
        audit.recordHeld("t1", 0, 1L, 1, CausalDependencies.empty());
        audit.recordReleased("t1", 0, 1L, 0);
        audit.recordViolation("t1", 0, 1L, CausalDependencies.empty());
        audit.recordDeserializationFailure("t1", 0, 1L, "reason", true);
        audit.recordEvictionLimitExceeded("t1", 0, 1L, CausalDependencies.empty());
        audit.processorInitialized("task-0", false);
        audit.processorClosing("task-0");
        // No assertion needed beyond reaching this line without an exception.
    }

    /**
     * A well-behaved delegate still receives every call with the exact arguments passed through —
     * the decorator must not alter behaviour when nothing fails.
     *
     * Asserts that the recording delegate observes one call per method, in order.
     */
    @Test
    void wellBehavedDelegateReceivesEveryCall() {
        List<String> calls = new ArrayList<>();
        CausalAudit audit = ParsleyAudit.wrap(new CausalAudit() {
            @Override public void recordForwarded(String topic, int partition, long offset) { calls.add("recordForwarded"); }
            @Override public void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) { calls.add("recordHeld"); }
            @Override public void recordReleased(String topic, int partition, long offset, int bufferDepthAfter) { calls.add("recordReleased"); }
            @Override public void recordViolation(String topic, int partition, long offset, CausalDependencies gap) { calls.add("recordViolation"); }
            @Override public void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) { calls.add("recordDeserializationFailure"); }
            @Override public void recordEvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap) { calls.add("recordEvictionLimitExceeded"); }
            @Override public void processorInitialized(String taskId, boolean frontierRestored) { calls.add("processorInitialized"); }
            @Override public void processorClosing(String taskId) { calls.add("processorClosing"); }
        });

        audit.recordForwarded("t1", 0, 1L);
        audit.recordHeld("t1", 0, 1L, 1, CausalDependencies.empty());
        audit.recordReleased("t1", 0, 1L, 0);
        audit.recordViolation("t1", 0, 1L, CausalDependencies.empty());
        audit.recordDeserializationFailure("t1", 0, 1L, "reason", true);
        audit.recordEvictionLimitExceeded("t1", 0, 1L, CausalDependencies.empty());
        audit.processorInitialized("task-0", false);
        audit.processorClosing("task-0");

        assertEquals(List.of("recordForwarded", "recordHeld", "recordReleased", "recordViolation",
                "recordDeserializationFailure", "recordEvictionLimitExceeded", "processorInitialized",
                "processorClosing"), calls,
                "every call must reach the delegate exactly once, unmodified");
    }

    private static final class ThrowingAudit implements CausalAudit {
        @Override public void recordForwarded(String topic, int partition, long offset) { throw new RuntimeException("boom"); }
        @Override public void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) { throw new RuntimeException("boom"); }
        @Override public void recordReleased(String topic, int partition, long offset, int bufferDepthAfter) { throw new RuntimeException("boom"); }
        @Override public void recordViolation(String topic, int partition, long offset, CausalDependencies gap) { throw new RuntimeException("boom"); }
        @Override public void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) { throw new RuntimeException("boom"); }
        @Override public void recordEvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap) { throw new RuntimeException("boom"); }
        @Override public void processorInitialized(String taskId, boolean frontierRestored) { throw new RuntimeException("boom"); }
        @Override public void processorClosing(String taskId) { throw new RuntimeException("boom"); }
    }
}
