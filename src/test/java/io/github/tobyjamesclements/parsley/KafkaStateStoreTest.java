package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The seam over a Streams {@link KeyValueStore}, exercised against Kafka's in-memory store:
 * UTF-8 key mapping and the byte-range prefix scan the protocol's held-record index relies on.
 */
class KafkaStateStoreTest {

    private KeyValueStore<Bytes, byte[]> inner;
    private KafkaStateStore store;

    @BeforeEach
    void createStore() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-state-store-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        var context = new MockProcessorContext<Void, Void>(props);
        inner = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("s"), Serdes.Bytes(), Serdes.ByteArray())
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
        inner.init(context.getStateStoreContext(), inner);
        store = new KafkaStateStore(inner);
    }

    @AfterEach
    void closeStore() {
        inner.close();
    }

    /** Put, get, and delete round-trip through the UTF-8 key mapping. */
    @Test
    void putGetDeleteRoundTrip() {
        store.put("held/a", new byte[] {1});
        assertArrayEquals(new byte[] {1}, store.get("held/a"), "a stored value must read back");
        assertNull(store.get("held/b"), "an absent key must read as null");

        store.delete("held/a");
        assertNull(store.get("held/a"), "a deleted key must read as null");
    }

    /** The prefix scan visits exactly the prefixed keys, in key order, with their values. */
    @Test
    void prefixScanVisitsExactlyThePrefixedKeys() {
        store.put("held/", new byte[] {0});
        store.put("held/a", new byte[] {1});
        store.put("held/b", new byte[] {2});
        // Adjacent in byte order on both sides of the range: '.' < '/' < '0'.
        store.put("held.x", new byte[] {8});
        store.put("held0", new byte[] {9});
        store.put("other/a", new byte[] {9});

        List<String> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        store.forEachPrefix("held/", (k, v) -> {
            keys.add(k);
            values.add(v);
        });

        assertEquals(List.of("held/", "held/a", "held/b"), keys,
                "exactly the prefixed keys must be visited, in key order");
        assertArrayEquals(new byte[] {1}, values.get(1), "each key must arrive with its value");
    }

    /** An empty scan visits nothing rather than failing. */
    @Test
    void prefixScanOverNothingVisitsNothing() {
        store.put("other/a", new byte[] {9});
        store.forEachPrefix("held/", (k, v) ->
                assertEquals(null, k, "no key outside the prefix may be visited"));
    }

    /**
     * A one-character prefix is scanned like any other. Its exclusive upper bound is its single
     * byte incremented, which exists; only the empty prefix has no bound at all, and rejecting
     * anything shorter than two bytes would refuse a legal scan.
     */
    @Test
    void singleCharacterPrefixIsScanned() {
        store.put("h", new byte[] {1});
        store.put("ha", new byte[] {2});
        store.put("i", new byte[] {3});

        List<String> keys = new ArrayList<>();
        store.forEachPrefix("h", (k, v) -> keys.add(k));

        assertEquals(List.of("h", "ha"), keys,
                "exactly the one-byte-prefixed keys must be visited");
    }

    /** A prefix with no byte-wise upper bound is rejected rather than scanned unbounded. */
    @Test
    void prefixWithoutUpperBoundIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> store.forEachPrefix("", (k, v) -> {}),
                "the empty prefix has no exclusive upper bound and must be rejected");
    }
}
