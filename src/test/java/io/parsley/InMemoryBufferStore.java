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

    private final TreeMap<Long, CausalRecord<K, V>> buffer = new TreeMap<>();
    private long sequence = 0;

    @Override
    public void add(CausalRecord<K, V> record) {
        buffer.put(sequence++, record);
    }

    @Override
    public List<Entry<K, V>> entries() {
        List<Entry<K, V>> entries = new ArrayList<>(buffer.size());
        buffer.forEach((seq, record) ->
                entries.add(new Entry<>(seq, record, VectorClock.fromBytes(record.encodedDependencies()))));
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
