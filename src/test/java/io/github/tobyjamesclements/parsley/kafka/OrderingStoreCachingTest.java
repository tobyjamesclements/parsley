package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the ordering store is built with the host's write cache (D110). Every
 * received record merges its frontier and every delivery merges the delivered past, one
 * store write per channel whose position advanced, so an uncached store costs about two
 * RocksDB writes and two changelog records per frontier channel per record. The cache
 * writes the latest value per key through at commit, bounding that by the keys touched.
 */
class OrderingStoreCachingTest {
    /** The built store's wrapper chain includes the caching layer, and still the changelogging one. */
    @Test
    void theOrderingStoreIsCachedAndChangelogged() {
        StateStore store = ProcessTopology.orderingStore().build();
        List<String> layers = new ArrayList<>();
        StateStore layer = store;
        while (layer != null) {
            layers.add(layer.getClass().getSimpleName());
            layer = layer instanceof WrappedStateStore<?, ?, ?> wrapped ? wrapped.wrapped() : null;
        }
        assertTrue(layers.stream().anyMatch(name -> name.startsWith("Caching")),
                "the ordering store must be cached, so writes per commit are bounded by keys, not records; layers: "
                        + layers);
        assertTrue(layers.stream().anyMatch(name -> name.startsWith("ChangeLogging")),
                "the ordering store must stay changelogged (D57); layers: " + layers);
    }
}
