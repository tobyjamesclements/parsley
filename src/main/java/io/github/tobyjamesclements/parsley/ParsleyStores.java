package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Builders for Parsley's internal frontier, buffer, candidate-index, and forwarded-index state
 * stores — shared by every causal processor node, whatever it forwards into.
 */
final class ParsleyStores {

    /**
     * Key under which the frontier state is stored in the processor's frontier state store: a single
     * value holding the whole {@link ParsleyFrontierState} (the contiguous frontier clock, the
     * per-channel clocks, and the rest of the node's persisted causal metadata). Renamed from the
     * former opaque {@code "f"} in the same break that gave the value a wire-version byte.
     */
    static final String FRONTIER_KEY = "frontier";

    private ParsleyStores() {}

    static StoreBuilder<KeyValueStore<String, byte[]>> frontierStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.String(),
                Serdes.ByteArray());
    }

    static StoreBuilder<KeyValueStore<Long, byte[]>> bufferStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.Long(),
                Serdes.ByteArray());
    }

    static StoreBuilder<KeyValueStore<byte[], byte[]>> candidateIndexStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.ByteArray(),
                Serdes.ByteArray());
    }

    static StoreBuilder<KeyValueStore<byte[], byte[]>> forwardedIndexStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.ByteArray(),
                Serdes.ByteArray());
    }
}
