package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * A causal-ordering violation, reported to a {@link CausalViolationHandler} with enough context to act
 * on or audit it.
 *
 * <p>A violation arises when a record is evicted from the causal buffer because a
 * {@link CausalBufferLimit} fired before its dependencies were satisfied. The payload carries the
 * <em>causal gap</em>: what the frontier had observed ({@link #frontier}) versus what the record
 * required ({@link #required}), and the per-position shortfall ({@link #gap}) — the difference
 * between a violation you can operate around (replay, compensate, alert) and one you can only find
 * in a postmortem.
 *
 * @param record   the offending record, as the original Kafka {@link ConsumerRecord}; never
 *                 {@code null}
 * @param frontier the frontier at the moment of the violation; never {@code null}
 * @param required the dependencies the record required to be delivered in order; never {@code null}
 * @param gap      the per-position shortfall ({@code required − observed}) for every position the
 *                 frontier had not caught up on; the {@link CausalPosition#offset()} field of each
 *                 entry holds the shortfall amount, not an absolute log offset
 */
public record CausalViolation(
        ConsumerRecord<?, ?> record,
        CausalFrontier frontier,
        CausalDependencies required,
        List<CausalPosition> gap) {

    /**
     * Canonical constructor; defensively copies {@code gap}.
     */
    public CausalViolation {
        gap = List.copyOf(gap);
    }
}
