package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * {@link StateStore} over a Streams {@link KeyValueStore}. Under EOS the store's mutations
 * commit with the task's transaction, which is exactly the contract the core requires. Keys are
 * UTF-8; prefix iteration uses byte-wise range semantics, which match UTF-8 lexicographic
 * ordering.
 */
final class KafkaStateStore implements StateStore {

    private final KeyValueStore<Bytes, byte[]> store;

    KafkaStateStore(KeyValueStore<Bytes, byte[]> store) {
        this.store = store;
    }

    private static Bytes k(String key) {
        return Bytes.wrap(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void put(String key, byte[] value) {
        store.put(k(key), value);
    }

    @Override
    public byte[] get(String key) {
        return store.get(k(key));
    }

    @Override
    public void delete(String key) {
        store.delete(k(key));
    }

    @Override
    public void forEachPrefix(String prefix, BiConsumer<String, byte[]> consumer) {
        byte[] from = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] to = from.clone();
        // The exclusive upper bound: smallest byte string greater than every prefixed key.
        int i = to.length - 1;
        while (i >= 0 && (to[i] & 0xFF) == 0xFF) i--;
        if (i < 0) throw new IllegalArgumentException("prefix has no upper bound: " + prefix);
        to[i]++;
        try (KeyValueIterator<Bytes, byte[]> it = store.range(Bytes.wrap(from), Bytes.wrap(to))) {
            while (it.hasNext()) {
                var kv = it.next();
                String key = new String(kv.key.get(), StandardCharsets.UTF_8);
                if (key.startsWith(prefix)) consumer.accept(key, kv.value);
            }
        }
    }
}
