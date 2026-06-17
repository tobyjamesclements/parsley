package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * A causal-ordering violation, reported to a {@link CausalViolationHandler} with enough context to act
 * on or audit it.
 *
 * <p>A violation arises when a record cannot be delivered in strict causal order — it carried an
 * unresolvable clock, no clock attribute, or was evicted from the buffer because a
 * {@link CausalBufferLimit} fired. The payload carries the
 * <em>causal gap</em>: what the frontier had observed ({@link #frontier}) versus what the record
 * required ({@link #required}), and the per-position shortfall ({@link #gap}) — the difference
 * between a violation you can operate around (replay, compensate, alert) and one you can only find
 * in a postmortem.
 *
 * @param record   the offending record, as the original Kafka {@link ConsumerRecord}; never
 *                 {@code null}
 * @param reason   why the record violated causal order; never {@code null}
 * @param frontier the frontier at the moment of the violation; never {@code null}
 * @param required the clock the record required to be delivered in order; never {@code null}
 *                 (empty if the record carried no resolvable clock)
 * @param gap      the per-position shortfall ({@code required − observed}) for every position the
 *                 frontier had not caught up on; the {@link CausalPosition#offset()} field of each
 *                 entry holds the shortfall amount, not an absolute log offset; empty if
 *                 {@code required.isSatisfiedBy(frontier)}
 */
public record CausalViolation(
        ConsumerRecord<?, ?> record,
        CausalViolationReason reason,
        CausalFrontier frontier,
        CausalDependencies required,
        List<CausalPosition> gap) {

    /**
     * Canonical constructor; defensively copies {@code gap}.
     */
    public CausalViolation {
        gap = List.copyOf(gap);
    }

    /**
     * Name of the header stamped on every dead-lettered record: the UTF-8 name of the
     * {@link CausalViolationReason} that caused eviction (e.g. {@code "LIMIT_REACHED"}).
     */
    public static final String DLQ_REASON_HEADER = "parsley-dlq-reason";

    /**
     * Name of the header stamped on every dead-lettered record: the required
     * {@link CausalDependencies} clock, encoded via {@link CausalDependencies#toBytes()} and
     * decodable via {@link CausalDependencies#fromBytes(byte[])}.
     *
     * <h3>Replay path</h3>
     * <ol>
     *   <li>Decode the required clock: {@code CausalDependencies.fromBytes(header.value())}.</li>
     *   <li>Wait until your consumer's frontier satisfies it (i.e. it has observed every position
     *       the original record required).</li>
     *   <li>Re-produce the original record with the same {@code parsley-causal-dependencies} value,
     *       stripping the {@code parsley-dlq-*} headers first.</li>
     * </ol>
     */
    public static final String DLQ_REQUIRED_CLOCK_HEADER = "parsley-dlq-required-clock";

    /**
     * Name of the header stamped on every dead-lettered record: the per-position causal shortfall
     * at eviction time, encoded as a {@link CausalDependencies} clock via
     * {@link CausalDependencies#toBytes()}. Each entry's {@link CausalPosition#offset()} is the
     * shortfall ({@code required − observed}). Useful for diagnostics; not needed for replay.
     */
    public static final String DLQ_GAP_HEADER = "parsley-dlq-gap";
}
