package io.github.tobyjamesclements.parsley;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a client-supplied {@link CausalAudit} so a broken or slow implementation can never affect
 * record processing: every delegated call is caught, and an exception is logged (never rethrown).
 *
 * <p>{@link CausalAudit#NOOP} is returned unwrapped by {@link #wrap}, since there is nothing to
 * protect against when no audit is registered.
 */
final class ParsleyAudit implements CausalAudit {

    private static final Logger log = LoggerFactory.getLogger(ParsleyAudit.class);

    private final CausalAudit delegate;

    private ParsleyAudit(CausalAudit delegate) {
        this.delegate = delegate;
    }

    /** Wraps {@code audit} for safe delegation, or returns it unwrapped if it is {@link CausalAudit#NOOP}. */
    static CausalAudit wrap(CausalAudit audit) {
        return audit == CausalAudit.NOOP ? audit : new ParsleyAudit(audit);
    }

    @Override
    public void recordForwarded(String topic, int partition, long offset) {
        safely("recordForwarded", () -> delegate.recordForwarded(topic, partition, offset));
    }

    @Override
    public void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) {
        safely("recordHeld", () -> delegate.recordHeld(topic, partition, offset, bufferDepth, gap));
    }

    @Override
    public void recordReleased(String topic, int partition, long offset, int bufferDepthAfter) {
        safely("recordReleased", () -> delegate.recordReleased(topic, partition, offset, bufferDepthAfter));
    }

    @Override
    public void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) {
        safely("recordDeserializationFailure",
                () -> delegate.recordDeserializationFailure(topic, partition, offset, reason, dropped));
    }

    @Override
    public void recordClockResolutionFailure(String topic, int partition, long offset, String reason, boolean failed) {
        safely("recordClockResolutionFailure",
                () -> delegate.recordClockResolutionFailure(topic, partition, offset, reason, failed));
    }

    @Override
    public void processorInitialized(String taskId, boolean frontierRestored) {
        safely("processorInitialized", () -> delegate.processorInitialized(taskId, frontierRestored));
    }

    @Override
    public void processorClosing(String taskId) {
        safely("processorClosing", () -> delegate.processorClosing(taskId));
    }

    private void safely(String method, Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            log.error("CausalAudit.{} threw; the event is lost but processing continues", method, e);
        }
    }
}
