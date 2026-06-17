package io.parsley;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * An in-memory {@link ParsleyBufferStore} backed by a {@link TreeMap} keyed by insertion sequence. Used
 * where buffer durability is not required — unit tests that exercise the {@link ParsleyEngine}
 * without a Kafka state store, and to stand in for a restored buffer in tests. Production uses
 * {@link RocksBufferStore}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class MockBufferStore<K, V> implements ParsleyBufferStore<K, V> {

    private final TreeMap<Long, ParsleyRecord<K, V>> buffer = new TreeMap<>();
    private long sequence = 0;

    @Override
    public long add(ParsleyRecord<K, V> record) {
        long seq = sequence++;
        buffer.put(seq, record);
        return seq;
    }

    @Override
    public Entry<K, V> get(long sequence) {
        ParsleyRecord<K, V> record = buffer.get(sequence);
        if (record == null) return null;
        return new Entry<>(sequence, record, CausalDependencies.fromBytes(record.encodedDependencies()));
    }

    @Override
    public List<Entry<K, V>> entries() {
        List<Entry<K, V>> entries = new ArrayList<>(buffer.size());
        buffer.forEach((seq, record) ->
                entries.add(new Entry<>(seq, record, CausalDependencies.fromBytes(record.encodedDependencies()))));
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
