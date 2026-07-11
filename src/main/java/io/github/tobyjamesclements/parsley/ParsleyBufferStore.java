package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.OptionalLong;

/**
 * The held-record buffer: the single, authoritative store of records whose causal dependencies are
 * not yet satisfied. Entries carry a monotonic insertion sequence — since records are admitted in
 * real-time order on the single owning thread, ascending sequence is equivalently causal arrival
 * (and {@code bufferedAt}) order.
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
    record Entry<K, V>(long sequence, long bufferedAt, ParsleyMessage<K, V> record, ParsleyClock dependencies) {}

    /**
     * The metadata decodable <em>without</em> the user serde: an entry's insertion sequence, its
     * buffer-admission time, source coordinate, and decoded dependencies — but not the deserialised
     * record. Used both to rebuild the candidate index on restart and to drive the drain scan without
     * all-or-nothing decoding, so a record whose key/value can no longer be decoded (e.g. an
     * incompatible Schema Registry change) neither blocks startup nor wedges the drain of the other
     * held records. The coordinate fields are Parsley's own framing (written ahead of the user
     * key/value bytes), so they're always decodable even for a record that is otherwise undecodable.
     */
    record IndexEntry(long sequence, long bufferedAt, String topic, Uuid topicId, int partition, long offset,
                      ParsleyClock dependencies) {}

    /**
     * Buffers a record under the next insertion sequence and returns that sequence. The sequence
     * is the opaque handle used by {@link #get} and {@link #remove}.
     *
     * @param record     the record to hold; carries valid (decodable) dependencies
     * @param bufferedAt the wall-clock time (epoch millis) at which the record is admitted
     * @return the insertion sequence assigned to the buffered record
     */
    long add(ParsleyMessage<K, V> record, long bufferedAt);

    /**
     * Returns the buffered entry for the given insertion sequence, or {@code null} if no such
     * entry exists. Used by the drain path to verify that a candidate-index candidate is still in the
     * buffer before attempting release.
     *
     * @param sequence the sequence of an entry previously returned by {@link #add}
     * @return the entry, or {@code null} if already removed
     */
    @Nullable Entry<K, V> get(long sequence);

    /**
     * Returns the {@link IndexEntry index metadata} for every buffered entry — decoding only the
     * dependency clock, not the record's key/value. Used once at construction to rebuild the candidate
     * index after a restart, immune to user-serde decode failures.
     *
     * @return the index metadata for every buffered entry
     */
    List<IndexEntry> indexEntries();

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

    /**
     * Returns the buffer-admission time of the oldest currently-held record — the lowest surviving
     * insertion sequence — or empty if the buffer holds nothing. Relies on insertion sequence order
     * coinciding with {@code bufferedAt} order (see the class Javadoc); since that ordering survives
     * arbitrary removals, implementations can answer this without scanning the buffer.
     *
     * @return the oldest held record's buffer-admission time (epoch millis), or empty if empty
     */
    OptionalLong oldestBufferedAt();
}
