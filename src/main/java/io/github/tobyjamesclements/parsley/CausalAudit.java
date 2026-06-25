package io.github.tobyjamesclements.parsley;

/**
 * Receives Parsley's per-record causal-buffering events: forwarded, held, released, evicted
 * (causal violation), eviction-limit-exceeded (fail-fast instead of a violation), undecodable, and
 * unresolvable-clock (an undecodable dependencies header at ingest).
 * Register one with
 * {@link CausalProcessors.Builder#withAudit} to route these events wherever your audit/compliance
 * trail needs them (a SIEM, a durable audit store, structured logs) — Parsley itself never decides
 * where they go.
 *
 * <p>Every method here carries record identity and causal metadata only — topic, partition,
 * offset, and the causal gap — never the record's key or value. Parsley's logs follow the same
 * rule; this interface exists for callers who need that information delivered somewhere durable
 * and queryable rather than (or in addition to) a text log line.
 *
 * <p>This is a separate, independent concern from {@code ParsleyMetrics}: metrics are
 * Parsley-internal aggregate counters wired automatically into the Kafka Streams metrics registry,
 * while a {@code CausalAudit} is optional, client-supplied, and carries per-record identity. Both
 * are notified of the same underlying events; neither depends on the other.
 *
 * <p><strong>Failure handling:</strong> an exception thrown from any method here is caught and
 * logged by Parsley — it never fails the record or the Streams task. A slow implementation will,
 * however, add latency to the calling thread, since these methods are called synchronously from the
 * processing path; keep implementations fast and non-blocking (hand off to a queue if the
 * destination is slow).
 *
 * <p><strong>Thread-safety:</strong> one instance is shared across every task/partition the
 * processor runs on, each owned by its own Kafka Streams thread. Implementations must be
 * thread-safe.
 */
public interface CausalAudit {

    /**
     * A record's causal dependencies were already satisfied; it was forwarded without entering
     * the buffer.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     */
    void recordForwarded(String topic, int partition, long offset);

    /**
     * A record's causal dependencies were not yet satisfied; it was admitted to the buffer to wait.
     *
     * @param topic       the record's source topic
     * @param partition   the record's source partition
     * @param offset      the record's source offset
     * @param bufferDepth the buffer's depth after admitting this record
     * @param gap         the dependencies not yet satisfied by the frontier
     */
    void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap);

    /**
     * A previously held record became causally satisfiable and was released from the buffer, in
     * causal order.
     *
     * @param topic            the record's source topic
     * @param partition        the record's source partition
     * @param offset           the record's source offset
     * @param bufferDepthAfter the buffer's depth after releasing this record
     */
    void recordReleased(String topic, int partition, long offset, int bufferDepthAfter);

    /**
     * A held record was evicted and forwarded out of causal order because a
     * {@link CausalBufferLimit} fired before its dependencies were satisfied — a causal violation.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param gap       the dependencies that remained unsatisfied at the time of eviction
     */
    void recordViolation(String topic, int partition, long offset, CausalDependencies gap);

    /**
     * A held record could no longer be deserialised on the forward path (e.g. an incompatible
     * Schema Registry change while buffered).
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param reason    an operator-facing diagnostic (coordinate, dependencies, lengths, schema
     *                  id — never the payload bytes)
     * @param dropped   {@code true} if the record was dropped and processing continued
     *                  ({@code parsley.buffer.deserialization.failure.policy = continue});
     *                  {@code false} if it remains in the buffer for recovery (the {@code fail}
     *                  default)
     */
    void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped);

    /**
     * An inbound record's {@code parsley-causal-dependencies} header could not be decoded into a clock
     * (a corrupt or truncated header, or one in an unsupported wire version).
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param reason    an operator-facing diagnostic (coordinate, encoded header length — never the
     *                  payload bytes)
     * @param failed    {@code true} if the task is being failed fast (the {@code fail} default of
     *                  {@code parsley.clock.resolution.failure.policy}); {@code false} if the record
     *                  was forwarded with empty dependencies ({@code continue})
     */
    void recordClockResolutionFailure(String topic, int partition, long offset, String reason, boolean failed);

    /**
     * A {@link CausalBufferLimit} fired on a held record whose dependencies were not yet satisfied,
     * and {@code parsley.buffer.eviction.failure.policy = fail} (the default) is configured: the
     * record was <strong>not</strong> evicted — it remains buffered — and the owning Streams task is
     * about to fail fast rather than deliver it out of causal order.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param gap       the dependencies that remain unsatisfied
     */
    void recordEvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap);

    /**
     * The processor for {@code taskId} initialized.
     *
     * @param taskId           the Kafka Streams task id
     * @param frontierRestored {@code true} if a persisted frontier was restored from the
     *                         changelog; {@code false} for a fresh start
     */
    void processorInitialized(String taskId, boolean frontierRestored);

    /**
     * The processor for {@code taskId} is closing.
     *
     * @param taskId the Kafka Streams task id
     */
    void processorClosing(String taskId);

    /** An audit that discards every event. The default when no {@code CausalAudit} is registered. */
    CausalAudit NOOP = new CausalAudit() {
        @Override public void recordForwarded(String topic, int partition, long offset) {}
        @Override public void recordHeld(String topic, int partition, long offset, int bufferDepth, CausalDependencies gap) {}
        @Override public void recordReleased(String topic, int partition, long offset, int bufferDepthAfter) {}
        @Override public void recordViolation(String topic, int partition, long offset, CausalDependencies gap) {}
        @Override public void recordDeserializationFailure(String topic, int partition, long offset, String reason, boolean dropped) {}
        @Override public void recordClockResolutionFailure(String topic, int partition, long offset, String reason, boolean failed) {}
        @Override public void recordEvictionLimitExceeded(String topic, int partition, long offset, CausalDependencies gap) {}
        @Override public void processorInitialized(String taskId, boolean frontierRestored) {}
        @Override public void processorClosing(String taskId) {}
    };
}
