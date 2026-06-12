package io.parsley;

import java.util.List;

/**
 * SPI for buffering records whose causal dependencies are not yet satisfied.
 *
 * <p>A buffer holds {@link CausalRecord}s until the consumer's frontier satisfies their
 * dependency clocks ({@link #drain}), or until a {@link BufferLimit} fires ({@link #evict}),
 * at which point the buffer's {@link BufferingPolicy} determines what happens to the
 * evicted records.
 *
 * <p>The standard implementations (ignore, drop, dead-letter) are created via the
 * {@code io.parsley.core.CausalBuffers} factory in {@code parsley-core}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 * @param <T> the concrete {@link VectorClock} type
 */
public interface CausalBuffer<K, V, T extends VectorClock<T>> {

    /**
     * Adds a record to the buffer with its decoded dependency clock.
     *
     * @param record       the record to buffer; must not be {@code null}
     * @param dependencies the record's decoded causal dependencies; must not be {@code null}
     */
    void add(CausalRecord<K, V, T> record, T dependencies);

    /**
     * Releases every buffered record whose dependencies are satisfied by {@code frontier},
     * in buffer insertion order.
     *
     * @param frontier the current causal frontier
     * @return the released records, in order; empty if none are releasable
     */
    List<CausalRecord<K, V, T>> drain(T frontier);

    /**
     * Evicts the buffered records because {@code limit} fired, reporting a
     * {@link CausalViolationReason#LIMIT_REACHED} violation per record via {@code handler}.
     *
     * <p>The return value depends on the buffer's {@link BufferingPolicy}: an ignore buffer
     * returns the evicted records so the caller can forward them out-of-order; drop and
     * dead-letter buffers return an empty list.
     *
     * @param limit   the limit that fired
     * @param handler the violation callback; must not be {@code null}
     * @return records the caller must forward downstream; empty for drop/dead-letter policies
     */
    List<CausalRecord<K, V, T>> evict(BufferLimit limit, CausalViolationHandler handler);

    /**
     * Returns the number of records currently buffered.
     *
     * @return the buffer size
     */
    int size();
}
