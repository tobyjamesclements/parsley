package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
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

    private record Held<K, V>(ParsleyMessage<K, V> record, long bufferedAt) {}

    private final TreeMap<Long, Held<K, V>> buffer = new TreeMap<>();
    private long sequence = 0;

    @Override
    public long add(ParsleyMessage<K, V> record, long bufferedAt) {
        long seq = sequence++;
        buffer.put(seq, new Held<>(record, bufferedAt));
        return seq;
    }

    @Override
    public Entry<K, V> get(long sequence) {
        Held<K, V> held = buffer.get(sequence);
        if (held == null) return null;
        return toEntry(sequence, held);
    }

    @Override
    public List<Entry<K, V>> entries() {
        List<Entry<K, V>> entries = new ArrayList<>(buffer.size());
        buffer.forEach((seq, held) -> entries.add(toEntry(seq, held)));
        return entries;
    }

    @Override
    public List<IndexEntry> indexEntries() {
        List<IndexEntry> entries = new ArrayList<>(buffer.size());
        buffer.forEach((seq, held) -> entries.add(new IndexEntry(seq, held.bufferedAt(),
                held.record().topic(), held.record().topicId(), held.record().partition(),
                held.record().offset(), held.record().dependencies())));
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

    @Override
    public OptionalLong oldestBufferedAt() {
        Map.Entry<Long, Held<K, V>> first = buffer.firstEntry();
        return first == null ? OptionalLong.empty() : OptionalLong.of(first.getValue().bufferedAt());
    }

    private Entry<K, V> toEntry(long sequence, Held<K, V> held) {
        return new Entry<>(sequence, held.bufferedAt(), held.record(),
                held.record().dependencies());
    }
}
