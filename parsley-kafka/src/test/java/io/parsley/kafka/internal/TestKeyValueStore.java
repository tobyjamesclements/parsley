package io.parsley.kafka.internal;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** In-memory store fixture; {@code onPut} observes every write (e.g. to assert ordering). */
final class TestKeyValueStore implements KeyValueStore<String, byte[]> {

    private final String name;
    private final Map<String, byte[]> data = new HashMap<>();
    private final BiConsumer<String, byte[]> onPut;

    TestKeyValueStore(String name) {
        this(name, (key, value) -> {});
    }

    TestKeyValueStore(String name, BiConsumer<String, byte[]> onPut) {
        this.name = name;
        this.onPut = onPut;
    }

    @Override public String name() { return name; }
    @Override public void init(StateStoreContext ctx, org.apache.kafka.streams.processor.StateStore root) {}
    @Override public void flush() {}
    @Override public void close() {}
    @Override public boolean persistent() { return false; }
    @Override public boolean isOpen() { return true; }

    @Override public byte[] get(String key) { return data.get(key); }

    @Override
    public void put(String key, byte[] value) {
        onPut.accept(key, value);
        data.put(key, value);
    }

    @Override public byte[] putIfAbsent(String key, byte[] value) { return data.putIfAbsent(key, value); }
    @Override public void putAll(List<KeyValue<String, byte[]>> entries) { entries.forEach(e -> put(e.key, e.value)); }
    @Override public byte[] delete(String key) { return data.remove(key); }

    @Override public KeyValueIterator<String, byte[]> range(String from, String to) { throw new UnsupportedOperationException(); }
    @Override public KeyValueIterator<String, byte[]> reverseRange(String from, String to) { throw new UnsupportedOperationException(); }
    @Override public KeyValueIterator<String, byte[]> all() { throw new UnsupportedOperationException(); }
    @Override public KeyValueIterator<String, byte[]> reverseAll() { throw new UnsupportedOperationException(); }
    @Override public long approximateNumEntries() { return data.size(); }
}
