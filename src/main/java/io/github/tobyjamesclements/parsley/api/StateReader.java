package io.github.tobyjamesclements.parsley.api;

/**
 * Read access to the stores a process declared.
 *
 * <p>Reads are served from the shard of each store owned by the task delivering the
 * message: partition {@code p} of every received topic shares one shard, and a key is
 * found only if the delivering topic was keyed so that the same partitioner put it on
 * {@code p}. To keep state about a different attribute than the delivered key, emit a
 * message keyed by that attribute to a topic this or another process receives — a
 * self-channel is a repartition. Producers outside Parsley must partition by the same rule.
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
    <K, V> V get(Store<K, V> store, K key);
}
