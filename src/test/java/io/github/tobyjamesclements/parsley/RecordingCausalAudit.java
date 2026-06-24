package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CausalAudit} that appends every call to a typed, per-event-kind list, for asserting
 * exact event sequences in tests. No mocking framework — a hand-rolled recorder.
 */
final class RecordingCausalAudit implements CausalAudit {

    record Forwarded(String topic, int partition, long offset) {}
    record Held(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) {}
    record Released(String topic, int partition, long offset, int bufferDepthAfter) {}
    record Violation(String topic, int partition, long offset, CausalDependencies gap) {}
    record DeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) {}
    record EvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap) {}
    record ProcessorInitialized(String taskId, boolean frontierRestored) {}
    record ProcessorClosing(String taskId) {}

    final List<Forwarded> forwarded = new ArrayList<>();
    final List<Held> held = new ArrayList<>();
    final List<Released> released = new ArrayList<>();
    final List<Violation> violations = new ArrayList<>();
    final List<DeserializationFailure> deserializationFailures = new ArrayList<>();
    final List<EvictionLimitExceeded> evictionLimitExceeded = new ArrayList<>();
    final List<ProcessorInitialized> initializations = new ArrayList<>();
    final List<ProcessorClosing> closings = new ArrayList<>();

    @Override
    public void recordForwarded(String topic, int partition, long offset) {
        forwarded.add(new Forwarded(topic, partition, offset));
    }

    @Override
    public void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) {
        held.add(new Held(topic, partition, offset, bufferDepth, gap));
    }

    @Override
    public void recordReleased(String topic, int partition, long offset, int bufferDepthAfter) {
        released.add(new Released(topic, partition, offset, bufferDepthAfter));
    }

    @Override
    public void recordViolation(String topic, int partition, long offset, CausalDependencies gap) {
        violations.add(new Violation(topic, partition, offset, gap));
    }

    @Override
    public void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) {
        deserializationFailures.add(new DeserializationFailure(topic, partition, offset, reason, dropped));
    }

    @Override
    public void recordEvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap) {
        evictionLimitExceeded.add(new EvictionLimitExceeded(topic, partition, offset, gap));
    }

    @Override
    public void processorInitialized(String taskId, boolean frontierRestored) {
        initializations.add(new ProcessorInitialized(taskId, frontierRestored));
    }

    @Override
    public void processorClosing(String taskId) {
        closings.add(new ProcessorClosing(taskId));
    }
}
