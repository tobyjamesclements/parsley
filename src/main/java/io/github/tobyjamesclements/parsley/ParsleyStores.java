package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

/**
 * Builders for Parsley's internal frontier, buffer, candidate-index, forwarded-index, and
 * commit-hook state stores — shared by every causal processor node, whatever it forwards into.
 */
final class ParsleyStores {

    /**
     * Key under which the frontier state is stored in the processor's frontier state store: a single
     * value holding both the contiguous frontier clock and the per-channel clocks (see
     * {@link ParsleyFrontier}).
     */
    static final String FRONTIER_KEY = "f";

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

    /** The commit-hook store's name for a stage whose frontier store is {@code frontierStoreName}. */
    static String commitHookName(String frontierStoreName) {
        return frontierStoreName + "-commit-hook";
    }

    /**
     * Builder for the {@link ParsleyCommittedCompleteness} commit hook: a non-persistent, non-logged
     * store registered solely so Kafka Streams invokes its {@code flush()} in every task commit cycle
     * (see that class's Javadoc). Caching/logging toggles are rejected — there is nothing to cache or
     * log.
     */
    static StoreBuilder<ParsleyCommittedCompleteness> commitHookStore(String name) {
        return new StoreBuilder<>() {
            @Override
            public StoreBuilder<ParsleyCommittedCompleteness> withCachingEnabled() {
                throw new UnsupportedOperationException("the commit hook holds no cacheable data");
            }

            @Override
            public StoreBuilder<ParsleyCommittedCompleteness> withCachingDisabled() {
                return this;
            }

            @Override
            public StoreBuilder<ParsleyCommittedCompleteness> withLoggingEnabled(Map<String, String> config) {
                throw new UnsupportedOperationException("the commit hook persists nothing to log");
            }

            @Override
            public StoreBuilder<ParsleyCommittedCompleteness> withLoggingDisabled() {
                return this;
            }

            @Override
            public ParsleyCommittedCompleteness build() {
                return new ParsleyCommittedCompleteness(name);
            }

            @Override
            public Map<String, String> logConfig() {
                return Map.of();
            }

            @Override
            public boolean loggingEnabled() {
                return false;
            }

            @Override
            public String name() {
                return name;
            }
        };
    }
}
