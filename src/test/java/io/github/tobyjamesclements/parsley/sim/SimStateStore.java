package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.StateStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * In-memory {@link StateStore} with crash semantics: mutations stage in an overlay and become
 * durable only at {@link #commit()}; {@link #discard()} models a crash losing the in-flight
 * transaction. The committed map survives node restarts (the simulator keeps it and hands it to
 * the reconstructed node).
 */
final class SimStateStore implements StateStore {

    private final TreeMap<String, byte[]> committed = new TreeMap<>();
    private final Map<String, byte[]> stagedPuts = new HashMap<>();
    private final Set<String> stagedDeletes = new HashSet<>();

    @Override
    public void put(String key, byte[] value) {
        stagedDeletes.remove(key);
        stagedPuts.put(key, value.clone());
    }

    @Override
    public byte[] get(String key) {
        if (stagedDeletes.contains(key)) return null;
        byte[] staged = stagedPuts.get(key);
        if (staged != null) return staged.clone();
        byte[] v = committed.get(key);
        return v == null ? null : v.clone();
    }

    @Override
    public void delete(String key) {
        stagedPuts.remove(key);
        stagedDeletes.add(key);
    }

    @Override
    public void forEachPrefix(String prefix, BiConsumer<String, byte[]> consumer) {
        TreeMap<String, byte[]> merged = new TreeMap<>(committed.subMap(prefix, prefix + Character.MAX_VALUE));
        stagedPuts.forEach((k, v) -> {
            if (k.startsWith(prefix)) merged.put(k, v);
        });
        stagedDeletes.forEach(merged::remove);
        merged.forEach((k, v) -> consumer.accept(k, v.clone()));
    }

    void commit() {
        stagedDeletes.forEach(committed::remove);
        committed.putAll(stagedPuts);
        stagedPuts.clear();
        stagedDeletes.clear();
    }

    void discard() {
        stagedPuts.clear();
        stagedDeletes.clear();
    }
}
