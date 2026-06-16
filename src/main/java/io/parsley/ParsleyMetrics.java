package io.parsley;

/**
 * Callback interface through which {@link ParsleyEngine} reports observable events. The
 * {@link ParsleyProcessor} wires a Kafka Streams {@link org.apache.kafka.streams.StreamsMetrics}
 * backed implementation; callers that do not need metrics receive {@link #NOOP}.
 */
interface ParsleyMetrics {

    /**
     * A record was added to the causal buffer.
     *
     * @param newBufferDepth the buffer depth after the add
     */
    void recordBuffered(int newBufferDepth);

    /**
     * One or more records were released from the buffer (their dependencies are now satisfied).
     *
     * @param count          the number of records released in this drain pass
     * @param newBufferDepth the buffer depth after the release
     */
    void recordReleased(int count, int newBufferDepth);

    /**
     * One or more records were evicted because a {@link CausalBufferLimit} fired.
     *
     * @param count the number of records evicted
     */
    void recordEvicted(int count);

    /** A causal violation was detected (any {@link CausalViolationReason}). */
    void recordViolation();

    ParsleyMetrics NOOP = new ParsleyMetrics() {
        @Override public void recordBuffered(int d) {}
        @Override public void recordReleased(int c, int d) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() {}
    };
}
