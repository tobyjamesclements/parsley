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
interface CausalBufferStore<K, V> {

    /**
     * A buffered entry: its insertion sequence (an opaque handle for {@link #remove(long)}), the
     * record, and its decoded dependency clock.
     */
    record Entry<K, V>(long sequence, ParsleyRecord<K, V> record, VectorClock dependencies) {}

    /**
     * Buffers a record under the next insertion sequence. The record's decoded dependency clock,
     * surfaced on each {@link Entry}, is derived from its {@code encodedDependencies}.
     *
     * @param record the record to hold; carries a valid (decodable) dependency clock
     */
    void add(ParsleyRecord<K, V> record);

    /**
     * Returns every buffered entry, in ascending insertion-sequence (causal arrival) order.
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
