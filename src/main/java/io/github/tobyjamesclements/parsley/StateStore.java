package io.github.tobyjamesclements.parsley;

import java.util.function.BiConsumer;

/**
 * Keyed durable state, transactional with delivery.
 *
 * <p>Every mutation made while processing an input commits atomically with that input's
 * consumed offset and the sends it caused, or not at all.
 *
 * <p>The Kafka adapter backs this with a Streams state store under EOS. The simulator backs it
 * with an in-memory map that honours crash semantics.
 *
 * <p>Keys are UTF-8 strings with {@code /}-separated segments. Values are opaque bytes.
 */
interface StateStore {

    /**
     * Stores {@code value} under {@code key}, replacing any previous value.
     *
     * @param key the key to write
     * @param value the opaque bytes to store
     */
    void put(String key, byte[] value);

    /**
     * Returns the value stored under {@code key}.
     *
     * @param key the key to read
     * @return the stored value, or null when absent
     */
    byte[] get(String key);

    /**
     * Removes any value stored under {@code key}.
     *
     * @param key the key to remove
     */
    void delete(String key);

    /**
     * Visits every entry whose key starts with {@code prefix}, in unspecified order.
     *
     * @param prefix the key prefix to match
     * @param consumer receives each matching key and its value
     */
    void forEachPrefix(String prefix, BiConsumer<String, byte[]> consumer);
}
