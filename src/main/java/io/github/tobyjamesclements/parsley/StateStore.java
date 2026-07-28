package io.github.tobyjamesclements.parsley;

import java.util.function.BiConsumer;

/**
 * Keyed durable state, transactional with delivery: every mutation made while processing an
 * input commits atomically with that input's consumed offset and the sends it caused, or not at
 * all. The Kafka adapter backs this with a Streams state store under EOS; the simulator backs it
 * with an in-memory map that honours crash semantics.
 *
 * <p>Keys are UTF-8 strings with {@code /}-separated segments; values are opaque bytes.
 */
interface StateStore {

    void put(String key, byte[] value);

    /** @return the stored value, or null when absent. */
    byte[] get(String key);

    void delete(String key);

    /** Visits every entry whose key starts with {@code prefix}, in unspecified order. */
    void forEachPrefix(String prefix, BiConsumer<String, byte[]> consumer);
}
