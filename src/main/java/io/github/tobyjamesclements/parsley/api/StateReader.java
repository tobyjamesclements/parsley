package io.github.tobyjamesclements.parsley.api;

/**
 * Read access to the stores a process declared, scoped to the key range of the step in
 * progress.
 *
 * @see Handler
 * @see Effects
 */
public interface StateReader {
    /**
     * Reads one value.
     *
     * @param store the store to read, which the process must have declared
     * @param key   the key to read
     * @param <K>   key type
     * @param <V>   value type
     * @return the stored value, or {@code null} when the key is absent
     * @throws IllegalArgumentException if the process did not declare {@code store}
     */
    <K, V> V get(StoreDef<K, V> store, K key);
}
