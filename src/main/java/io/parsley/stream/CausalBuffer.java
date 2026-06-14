package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationHandler;
import io.parsley.VectorClock;

import java.util.List;
import java.util.function.Consumer;

/**
 * Buffers records whose causal dependencies are not yet satisfied.
 *
 * <p>A buffer holds {@link CausalRecord}s until the frontier satisfies their dependency clocks
 * ({@link #drain}) or until a {@link BufferLimit} fires ({@link #evict}), at which point the
 * buffer's {@link io.parsley.BufferingPolicy} determines what happens to the evicted records.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
interface CausalBuffer<K, V> {

    /**
     * Adds a record to the buffer with its decoded dependency clock.
     *
     * @param record       the record to buffer
     * @param dependencies the record's decoded causal dependencies
     */
    void add(CausalRecord<K, V> record, VectorClock dependencies);

    /**
     * Releases every buffered record whose dependencies are satisfied by {@code frontier},
     * in buffer insertion order.
     *
     * @param frontier the current causal frontier
     * @return the released records, in order; empty if none are releasable
     */
    List<CausalRecord<K, V>> drain(VectorClock frontier);

    /**
     * Evicts the buffered records because {@code limit} fired, reporting a
     * {@link io.parsley.CausalViolationReason#LIMIT_REACHED} violation per record and notifying
     * {@code onRemoved} for <em>every</em> record that leaves the buffer (regardless of whether the
     * policy forwards, drops, or dead-letters it).
     *
     * @param limit     the limit that fired
     * @param handler   the violation callback
     * @param onRemoved called once per record removed from the buffer, for buffer-persistence
     * @return records the caller must forward downstream; empty for drop/dead-letter policies
     */
    List<CausalRecord<K, V>> evict(
            BufferLimit limit, CausalViolationHandler handler, Consumer<CausalRecord<K, V>> onRemoved);

    /**
     * Returns the number of records currently buffered.
     *
     * @return the buffer size
     */
    int size();
}
