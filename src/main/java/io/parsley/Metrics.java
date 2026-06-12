package io.parsley;

import java.time.Duration;

/**
 * Observability hook for integrating Parsley events into a metrics system.
 *
 * <p>Implement this interface and supply it to the processor supplier to receive callbacks for
 * key lifecycle events. A no-op implementation is available via {@link #noop()}.
 */
public interface Metrics {

    /**
     * Called when a record is added to the causal buffer because its dependencies are not
     * yet satisfied.
     */
    void onMessageBuffered();

    /**
     * Called when a buffered record is released (drained) because its causal dependencies
     * have been satisfied by the advancing frontier.
     *
     * @param bufferDuration the time the record spent in the buffer
     */
    void onMessageReleased(Duration bufferDuration);

    /**
     * Called when a causal violation is reported (i.e. a record is forwarded, dropped, or
     * dead-lettered without its dependencies being satisfied).
     *
     * @param reason the reason for the violation
     */
    void onViolation(CausalViolationReason reason);

    /**
     * Called after the causal frontier advances, reflecting new progress on one or more
     * partitions.
     *
     * @param frontier the new frontier {@link VectorClock} after advancement
     */
    void onFrontierAdvanced(VectorClock frontier);

    /**
     * Returns a no-op {@code Metrics} that discards all events.
     *
     * @return a do-nothing implementation
     */
    static Metrics noop() {
        return new Metrics() {
            @Override public void onMessageBuffered() {}
            @Override public void onMessageReleased(Duration bufferDuration) {}
            @Override public void onViolation(CausalViolationReason reason) {}
            @Override public void onFrontierAdvanced(VectorClock frontier) {}
        };
    }
}
