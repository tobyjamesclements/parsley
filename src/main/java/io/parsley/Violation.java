package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * A causal-ordering violation, reported to a {@link ViolationHandler} with enough context to act
 * on or audit it.
 *
 * <p>A violation arises when a record cannot be delivered in strict causal order — it carried an
 * unresolvable clock, no clock attribute, or was evicted from the buffer because a
 * {@link BufferLimit} fired. The payload carries the
 * <em>causal gap</em>: what the frontier had observed ({@link #frontier}) versus what the record
 * required ({@link #required}), and the per-partition shortfall ({@link #gap}) — the difference
 * between a violation you can operate around (replay, compensate, alert) and one you can only find
 * in a postmortem.
 *
 * @param record   the offending record, as the original Kafka {@link ConsumerRecord}; never
 *                 {@code null}
 * @param reason   why the record violated causal order; never {@code null}
 * @param frontier the frontier observed at the moment of the violation; never {@code null}
 * @param required the clock the record required to be delivered in order; never {@code null}
 *                 (empty if the record carried no resolvable clock)
 * @param gap      the per-partition shortfall ({@code required − observed}) for every partition the
 *                 frontier had not caught up on; empty if {@code required.satisfiedBy(frontier)}
 */
public record Violation(
        ConsumerRecord<?, ?> record,
        CausalViolationReason reason,
        VectorClock frontier,
        VectorClock required,
        Map<TopicPartition, Long> gap) {

    /**
     * Canonical constructor; defensively copies {@code gap}.
     */
    public Violation {
        gap = Map.copyOf(gap);
    }
}
