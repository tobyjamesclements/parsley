package io.github.tobyjamesclements.parsley.core;

/**
 * Durable ordering state, keyed and valued in bytes. The engine is host-independent: the Kafka Streams adapter backs
 * this with a Streams {@code KeyValueStore} whose writes commit atomically with the step (SPEC Host obligation 3);
 * the test simulator backs it with a rollbackable map to simulate aborted steps and restarts.
 *
 * <p>Keys are compared unsigned-lexicographically; {@link #scanPrefix} must yield entries in ascending key order.</p>
 */
public interface OrderingStore {

    byte[] get(byte[] key);

    void put(byte[] key, byte[] value);

    void delete(byte[] key);

    /** Streams all entries whose key starts with {@code prefix}, in ascending unsigned-lexicographic key order. */
    void scanPrefix(byte[] prefix, EntryConsumer consumer);

    @FunctionalInterface
    interface EntryConsumer {
        void accept(byte[] key, byte[] value);
    }
}
