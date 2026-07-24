package io.github.tobyjamesclements.parsley;

import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Wraps a {@link MockBufferStore}, letting a test mark chosen insertion sequences to make {@link #get}
 * throw {@link CausalBufferDeserializationException} instead of returning the entry — simulating a
 * poison record (undecodable on the forward path) without a real serde.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class PoisonableBufferStore<K, V> implements ParsleyBufferStore<K, V> {

    private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
    private final Set<Long> poisoned = new HashSet<>();

    /** Marks {@code sequence} to throw {@link CausalBufferDeserializationException} on the next {@link #get}. */
    void poison(long sequence) {
        poisoned.add(sequence);
    }

    @Override
    public long add(ParsleyMessage<K, V> record, long bufferedAt) {
        return delegate.add(record, bufferedAt);
    }

    @Override
    public @Nullable Entry<K, V> get(long sequence) {
        if (poisoned.contains(sequence)) {
            IndexEntry meta = indexEntryFor(sequence);
            throw new CausalBufferDeserializationException(meta.topic(), meta.topicId(), meta.partition(),
                    meta.offset(), -1, "poisoned by test", new RuntimeException("simulated poison"));
        }
        return delegate.get(sequence);
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
