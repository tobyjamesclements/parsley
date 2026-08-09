package io.github.tobyjamesclements.parsley.core;

/**
 * The byte-level key-value store holding a process's ordering state.
 *
 * <p>This is the engine's only route to durable storage, and the seam a host implements. It
 * names no host type, so the engine can be driven by a simulator as readily as by Kafka
 * Streams.
 *
 * <p>Writes are expected to commit atomically with the step that made them.
 *
 * @see StoreCodec
 */
public interface OrderingStore {
    /**
     * Reads one entry.
     *
     * @param key the key to read
     * @return the stored bytes, or {@code null} when the key is absent
     */
    byte[] get(byte[] key);

    /**
     * Writes one entry.
     *
     * @param key   the key to write
     * @param value the bytes to store
     */
    void put(byte[] key, byte[] value);

    /**
     * Removes one entry.
     *
     * @param key the key to remove
     */
    void delete(byte[] key);

    /**
     * Visits every entry whose key begins with {@code prefix}.
     *
     * @param prefix   the key prefix to match
     * @param consumer invoked once per matching entry
     */
    void scanPrefix(byte[] prefix, EntryConsumer consumer);

    /** Receives one entry during a prefix scan. */
    @FunctionalInterface
    interface EntryConsumer {
        /**
         * Receives one entry.
         *
         * @param key   the entry key
         * @param value the entry value
         */
        void accept(byte[] key, byte[] value);
    }
}
