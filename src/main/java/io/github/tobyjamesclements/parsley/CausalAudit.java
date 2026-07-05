package io.github.tobyjamesclements.parsley;

/**
 * Receives Parsley's per-record causal-buffering events: forwarded, held, released, undecodable,
 * unresolvable-clock (an undecodable dependencies header at ingest), and dead-lettered (a record
 * removed from the causal execution path because its dependencies are proven impossible to satisfy).
 * Register one with
 * {@link ParsleyProcessors.Builder#withAudit} to route these events wherever your audit/compliance
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
     * A held record could no longer be deserialised on the forward path (e.g. an incompatible
     * Schema Registry change while buffered). This is the detection signal; if a dead-letter sink is
     * configured the record is then dead-lettered ({@link #recordDeadLetter} fires too) rather than
     * failing the task — without one, the task fails fast and the record remains in the buffer
     * changelog for recovery.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param reason    an operator-facing diagnostic (coordinate, dependencies, lengths, schema
     *                  id — never the payload bytes)
     */
    void recordDeserializationFailure(String topic, int partition, long offset, String reason);

    /**
     * An inbound record's {@code parsley-causal-dependencies} header could not be decoded into a clock
     * (a corrupt or truncated header, or one in an unsupported wire version). This is the detection
     * signal; if a dead-letter sink is configured the record is then dead-lettered ({@link
     * #recordDeadLetter} fires too) rather than failing the task — without one, the task fails fast.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param reason    an operator-facing diagnostic (coordinate, encoded header length — never the
     *                  payload bytes)
     */
    void recordClockResolutionFailure(String topic, int partition, long offset, String reason);

    /**
     * A record was removed from the causal execution path onto the dead-letter sink, because its
     * dependencies are proven impossible to satisfy. Fires for every dead-lettered record, including an
     * orphan-cascade victim that was never itself a {@link #recordDeserializationFailure}/{@link
     * #recordClockResolutionFailure} occurrence — this is the one disposition signal common to all three
     * causes.
     *
     * @param topic     the record's source topic
     * @param partition the record's source partition
     * @param offset    the record's source offset
     * @param reason    the cause: {@code "POISON"}, {@code "UNRESOLVABLE_CLOCK"}, or {@code "ORPHAN_CASCADE"}
     */
    void recordDeadLetter(String topic, int partition, long offset, String reason);

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
        @Override public void recordDeserializationFailure(String topic, int partition, long offset, String reason) {}
        @Override public void recordClockResolutionFailure(String topic, int partition, long offset, String reason) {}
        @Override public void recordDeadLetter(String topic, int partition, long offset, String reason) {}
        @Override public void processorInitialized(String taskId, boolean frontierRestored) {}
        @Override public void processorClosing(String taskId) {}
    };
}
