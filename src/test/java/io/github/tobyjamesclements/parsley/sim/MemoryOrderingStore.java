package io.github.tobyjamesclements.parsley.sim;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import io.github.tobyjamesclements.parsley.core.OrderingStore;

/**
 * An {@link io.github.tobyjamesclements.parsley.core.OrderingStore} in memory, with commit
 * and rollback, standing in for a transactional store.
 */
public final class MemoryOrderingStore implements OrderingStore {
    private static final java.util.Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    private TreeMap<byte[], byte[]> committed = new TreeMap<>(UNSIGNED);
    private TreeMap<byte[], byte[]> working = new TreeMap<>(UNSIGNED);

    @Override
    public byte[] get(byte[] key) {
        byte[] value = working.get(key);
        return value == null ? null : value.clone();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        working.put(key.clone(), value.clone());
    }

    @Override
    public void delete(byte[] key) {
        working.remove(key);
    }

    @Override
    public void scanPrefix(byte[] prefix, EntryConsumer consumer) {
        for (Map.Entry<byte[], byte[]> entry : working.tailMap(prefix, true).entrySet()) {
            byte[] key = entry.getKey();
            if (key.length < prefix.length || Arrays.compareUnsigned(key, 0, prefix.length, prefix, 0, prefix.length) != 0) {
                break;
            }
            consumer.accept(key.clone(), entry.getValue().clone());
        }
    }

    public void commit() {
        committed = deepCopy(working);
    }

    public void rollback() {
        working = deepCopy(committed);
    }

    private static TreeMap<byte[], byte[]> deepCopy(TreeMap<byte[], byte[]> source) {
        TreeMap<byte[], byte[]> copy = new TreeMap<>(UNSIGNED);
        source.forEach((key, value) -> copy.put(key.clone(), value.clone()));
        return copy;
    }
}
