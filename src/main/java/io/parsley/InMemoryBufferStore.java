package io.parsley;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * An in-memory {@link BufferStore} backed by a {@link TreeMap} keyed by insertion sequence. Used
 * where buffer durability is not required — unit tests that exercise the {@link CausalEngine}
 * without a Kafka state store, and to stand in for a restored buffer in tests. Production uses
 * {@link ParsleyBufferStore}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class InMemoryBufferStore<K, V> implements BufferStore<K, V> {

    private final TreeMap<Long, Buffered<K, V>> buffer = new TreeMap<>();
    private long sequence = 0;

    @Override
    public void add(CausalRecord<K, V> record, VectorClock dependencies) {
        buffer.put(sequence++, new Buffered<>(record, dependencies));
    }

    @Override
    public List<Entry<K, V>> entries() {
        List<Entry<K, V>> entries = new ArrayList<>(buffer.size());
        buffer.forEach((seq, held) -> entries.add(new Entry<>(seq, held.record(), held.dependencies())));
        return entries;
    }

    @Override
    public void remove(long sequence) {
        buffer.remove(sequence);
    }

    @Override
    public int size() {
        return buffer.size();
    }
}
