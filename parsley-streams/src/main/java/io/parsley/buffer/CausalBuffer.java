package io.parsley.buffer;

import io.parsley.BufferLimit;
import io.parsley.VectorClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * Core abstraction for holding records whose causal dependencies have not yet been satisfied.
 *
 * <p>A {@code CausalBuffer} accumulates Kafka {@link ConsumerRecord}s that carry a
 * {@link VectorClock} header requiring a later frontier than what has been observed so far.
 * As the frontier advances — because the consumer processes more records — buffered records
 * become eligible for release via {@link #drain}.
 *
 * <p>When the frontier has not advanced quickly enough and a {@link BufferLimit} fires,
 * records can be forcibly evicted via {@link #evict}.
 *
 * <p>Use {@link CausalBuffers} to create instances appropriate for a given
 * {@link io.parsley.BufferingPolicy}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalBuffer<K, V> {

    /**
     * Adds a record to the buffer.
     *
     * <p>The record will remain in the buffer until either {@link #drain} releases it
     * (because its causal dependencies are satisfied) or {@link #evict} forcibly removes it.
     *
     * @param record the record to buffer; must not be {@code null}
     */
    void add(ConsumerRecord<K, V> record);

    /**
     * Returns and removes all records whose {@link VectorClock} is dominated by
     * {@code frontier}.
     *
     * <p>A record is eligible for draining when {@code frontier.dominates(recordClock)} —
     * i.e. when the consumer has seen everything the record's clock requires.
     *
     * @param frontier the current causal frontier; must not be {@code null}
     * @return an ordered list of records whose dependencies are now satisfied; never {@code null},
     *         may be empty
     */
    List<ConsumerRecord<K, V>> drain(VectorClock frontier);

    /**
     * Forcibly removes records from the buffer because a {@link BufferLimit} has fired,
     * invoking {@code handler} for each evicted record.
     *
     * <p>The returned list contains records that should be forwarded downstream:
     * <ul>
     *   <li>For the {@link io.parsley.BufferingPolicy.Ignore Ignore} policy, returns the evicted
     *       records so they can be forwarded out-of-causal-order.
     *   <li>For {@link io.parsley.BufferingPolicy.Drop Drop} and
     *       {@link io.parsley.BufferingPolicy.DeadLetter DeadLetter} policies, returns an empty
     *       list (records are discarded or routed internally).
     * </ul>
     *
     * @param limit   the limit that triggered eviction; must not be {@code null}
     * @param handler the callback invoked for each evicted record; must not be {@code null}
     * @return records to forward downstream (non-empty only for the {@code Ignore} policy)
     */
    List<ConsumerRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler);
}
