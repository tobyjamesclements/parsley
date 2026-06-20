package io.parsley;

import java.util.List;

/**
 * The held-record buffer: the single, authoritative store of records whose causal dependencies are
 * not yet satisfied. Entries carry a monotonic insertion sequence, and {@link #entries()} yields
 * them in that order, so iteration reflects causal arrival order.
 *
 * <p>There is no separate in-memory copy: a durable implementation <em>is</em> the buffer, so held
 * records need no rehydration step after a restart — they are simply read back on the next drain.
 * The {@link ParsleyEngine} drives the buffer through this interface and is agnostic to whether it is
 * purely in-memory (tests) or backed by a changelog-replicated Kafka store (production).
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
interface ParsleyBufferStore<K, V> {

    /**
     * A buffered entry: its insertion sequence (an opaque handle for {@link #remove(long)}), the
     * wall-clock time it was admitted to the buffer, the record, and its decoded dependencies.
     */
    record Entry<K, V>(long sequence, long bufferedAt, ParsleyRecord<K, V> record, CausalDependencies dependencies) {}

    /**
     * Buffers a record under the next insertion sequence and returns that sequence. The sequence
     * is the opaque handle used by {@link #get} and {@link #remove}.
     *
     * @param record     the record to hold; carries valid (decodable) dependencies
     * @param bufferedAt the wall-clock time (epoch millis) at which the record is admitted
     * @return the insertion sequence assigned to the buffered record
     */
    long add(ParsleyRecord<K, V> record, long bufferedAt);

    /**
     * Returns the buffered entry for the given insertion sequence, or {@code null} if no such
     * entry exists. Used by the drain path to verify that a position-index candidate is still in the
     * buffer before attempting release.
     *
     * @param sequence the sequence of an entry previously returned by {@link #add}
     * @return the entry, or {@code null} if already removed
     */
    Entry<K, V> get(long sequence);

    /**
     * Returns every buffered entry, in ascending insertion-sequence (causal arrival) order. Since
     * entries are admitted in real-time order on a single owning thread, this is equivalently
     * ascending {@code bufferedAt} (buffer-start-time) order — callers that need oldest-first
     * iteration (e.g. duration-based eviction) can rely on this without a separate index.
     *
     * @return the buffered entries; empty if the buffer is empty
     */
    List<Entry<K, V>> entries();

    /**
     * Removes the entry with the given insertion sequence.
     *
     * @param sequence the sequence of an entry previously returned by {@link #entries()}
     */
    void remove(long sequence);

    /**
     * Returns the number of records currently buffered.
     *
     * @return the buffer size
     */
    int size();
}
