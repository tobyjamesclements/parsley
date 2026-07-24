package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jspecify.annotations.Nullable;

/**
 * An in-memory {@link KeyValueStore} double, keyed and ordered by an explicit {@link Comparator} (so
 * it can stand in for either a {@code Long}-keyed or a {@code byte[]}-keyed store). Used to exercise
 * {@link StoreBackedBufferStore} and {@link StoreBackedCandidateIndex} — the real {@link KeyValueStore}-backed
 * implementations — against the actual {@link KeyValueStore} contract (in particular {@code
 * range}'s inclusive-inclusive bounds) without a real RocksDB-backed store.
 */
final class TestKeyValueStore<K, V> implements KeyValueStore<K, V> {

    private final NavigableMap<K, V> map;
    private final String name;
    private boolean open = true;

    TestKeyValueStore(Comparator<K> comparator) {
        this(comparator, "test-store");
    }

    TestKeyValueStore(Comparator<K> comparator, String name) {
        this.map = new TreeMap<>(comparator);
        this.name = name;
    }

    @Override public void put(K key, V value) { map.put(key, value); }
    @Override public V putIfAbsent(K key, V value) { return map.putIfAbsent(key, value); }
    @Override public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries) put(entry.key, entry.value);
    }
    @Override public @Nullable V delete(K key) { return map.remove(key); }
    @Override public @Nullable V get(K key) { return map.get(key); }

    @Override public KeyValueIterator<K, V> range(K from, K to) {
        return iterator(map.subMap(from, true, to, true));
    }

    @Override public KeyValueIterator<K, V> all() { return iterator(map); }
    @Override public long approximateNumEntries() { return map.size(); }

    @Override public String name() { return name; }
    @Override public void init(StateStoreContext context, StateStore root) {}
    @Override public void close() { open = false; }
    @Override public boolean persistent() { return true; }
    @Override public boolean isOpen() { return open; }

    private static <K, V> KeyValueIterator<K, V> iterator(NavigableMap<K, V> source) {
        // Snapshot so the iterator is unaffected by mutations made while it is open.
        Iterator<Map.Entry<K, V>> snapshot = new ArrayList<>(source.entrySet()).iterator();
        return new KeyValueIterator<>() {
            @Override public boolean hasNext() { return snapshot.hasNext(); }
            @Override public KeyValue<K, V> next() {
                Map.Entry<K, V> entry = snapshot.next();
                return new KeyValue<>(entry.getKey(), entry.getValue());
            }
            @Override public K peekNextKey() { throw new UnsupportedOperationException(); }
            @Override public void close() {}
        };
    }
}
