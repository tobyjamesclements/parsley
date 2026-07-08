package io.github.tobyjamesclements.parsley;

import java.util.List;
import java.util.OptionalLong;

/**
 * Wraps a {@link PoisonableBufferStore}, appending {@code "remove:seq<sequence>"} to a shared,
 * test-owned log every time {@link #remove} is called. Paired with {@link RecordingOrphanIndex} (logging
 * the same list) to let a test assert the relative order of a buffer removal versus the orphan-index
 * write for that record's own coordinate — the write-ordering invariant {@link ParsleyEngine}'s
 * "Dead-letter persistence ordering" Javadoc documents.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class RecordingBufferStore<K, V> implements ParsleyBufferStore<K, V> {

    private final PoisonableBufferStore<K, V> delegate = new PoisonableBufferStore<>();
    private final List<String> log;

    RecordingBufferStore(List<String> log) {
        this.log = log;
    }

    /** Marks {@code sequence} to throw {@link ParsleyBufferDeserializationException} on the next {@link #get}. */
    void poison(long sequence) {
        delegate.poison(sequence);
    }

    @Override
    public long add(ParsleyMessage<K, V> record, long bufferedAt) {
        return delegate.add(record, bufferedAt);
    }

    @Override
    public Entry<K, V> get(long sequence) {
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
        log.add("remove:seq" + sequence);
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
}
