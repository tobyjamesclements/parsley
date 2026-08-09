package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;

import io.github.tobyjamesclements.parsley.core.OrderingStore;

/**
 * An {@link OrderingStore} over a Kafka Streams key-value store.
 *
 * <p>Writes land in the store the processor owns, so they commit in the same transaction as
 * the step that made them.
 *
 * <p>Streams offers no prefix scan, so {@link #scanPrefix} is a range query between the
 * prefix and its successor, with a comparison to reject anything the range admits that the
 * prefix does not.
 */
final class StreamsOrderingStore implements OrderingStore {
    private final KeyValueStore<Bytes, byte[]> store;

    StreamsOrderingStore(KeyValueStore<Bytes, byte[]> store) {
        this.store = store;
    }

    @Override
    public byte[] get(byte[] key) {
        return store.get(Bytes.wrap(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        store.put(Bytes.wrap(key), value);
    }

    @Override
    public void delete(byte[] key) {
        store.delete(Bytes.wrap(key));
    }

    @Override
    public void scanPrefix(byte[] prefix, EntryConsumer consumer) {
        Bytes from = Bytes.wrap(prefix);
        Bytes to = upperBound(prefix);
        try (KeyValueIterator<Bytes, byte[]> iterator =
                     to == null ? store.range(from, null) : store.range(from, to)) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                byte[] key = entry.key.get();
                if (key.length < prefix.length
                        || Arrays.compareUnsigned(key, 0, prefix.length, prefix, 0, prefix.length) != 0) {
                    continue;
                }
                consumer.accept(key, entry.value);
            }
        }
    }

    /**
     * The exclusive end of a prefix range.
     *
     * @param prefix the prefix to bound
     * @return the next key after every key with this prefix, or {@code null} where the prefix
     *         is all {@code 0xFF} bytes and no such key exists
     */
    private static Bytes upperBound(byte[] prefix) {
        byte[] bound = Arrays.copyOf(prefix, prefix.length);
        for (int i = bound.length - 1; i >= 0; i--) {
            if (bound[i] != (byte) 0xFF) {
                bound[i]++;
                return Bytes.wrap(Arrays.copyOf(bound, i + 1));
            }
        }
        return null;
    }
}
