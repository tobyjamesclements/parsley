package io.github.tobyjamesclements.parsley;

import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Wraps a {@link MockBufferStore}, letting a test mark chosen insertion sequences to make {@link #get}
 * throw {@link ParsleyBufferDeserializationException} instead of returning the entry — simulating a
 * poison record (undecodable on the forward path) without a real serde. The thrown exception carries
 * the poisoned entry's own coordinate (so the engine's cascade orphans the right one) and fixed,
 * recognisable raw bytes, for tests to assert a {@link ParsleyEngine.DeadLetter.Undecodable} carries
 * them through unchanged.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class PoisonableBufferStore<K, V> implements ParsleyBufferStore<K, V> {

    static final byte[] POISON_RAW_KEY = {1};
    static final byte[] POISON_RAW_VALUE = {2, 3};

    private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
    private final Set<Long> poisoned = new HashSet<>();

    /** Marks {@code sequence} to throw {@link ParsleyBufferDeserializationException} on the next {@link #get}. */
    void poison(long sequence) {
        poisoned.add(sequence);
    }

    @Override
    public long add(ParsleyMessage<K, V> record, long bufferedAt) {
        return delegate.add(record, bufferedAt);
    }

    @Override
    public Entry<K, V> get(long sequence) {
        if (poisoned.contains(sequence)) {
            IndexEntry meta = indexEntryFor(sequence);
            throw new ParsleyBufferDeserializationException(meta.topic(), meta.topicId(), meta.partition(),
                    meta.offset(), meta.bufferedAt(), List.of(), POISON_RAW_KEY, POISON_RAW_VALUE, -1,
                    "poisoned by test", new RuntimeException("simulated poison"));
        }
        return delegate.get(sequence);
    }

    @Override
    public List<Entry<K, V>> entries() {
        return delegate.entries();
    }

    @Override
    public List<IndexEntry> indexEntries() {
        return delegate.indexEntries();
    }

    @Override
    public void remove(long sequence) {
        delegate.remove(sequence);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public OptionalLong oldestBufferedAt() {
        return delegate.oldestBufferedAt();
    }

    private IndexEntry indexEntryFor(long sequence) {
        for (IndexEntry entry : delegate.indexEntries()) {
            if (entry.sequence() == sequence) {
                return entry;
            }
        }
        throw new IllegalStateException("no buffered entry for sequence " + sequence);
    }
}
