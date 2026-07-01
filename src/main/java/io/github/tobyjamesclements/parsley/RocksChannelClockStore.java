package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ParsleyChannelClockStore} backed by a changelog-replicated Kafka
 * {@link KeyValueStore}.
 *
 * <h2>Key layout (20 bytes, all fields big-endian)</h2>
 * <pre>
 * [topicId MSB : 8B]
 * [topicId LSB : 8B]
 * [partition   : 4B]
 * </pre>
 *
 * <p>All numeric fields are encoded big-endian so that RocksDB's lexicographic comparator
 * reflects natural numeric order.
 *
 * <p>The value for each key is the serialised {@link ParsleyClock} (via
 * {@link ParsleyClock#toBytes()}) representing the maximum-merged inbound clock seen on that
 * channel across all deliveries to date.
 */
final class RocksChannelClockStore implements ParsleyChannelClockStore {

    private static final int KEY_SIZE = 20; // 8B (MSB) + 8B (LSB) + 4B (partition)

    private final KeyValueStore<byte[], byte[]> store;

    RocksChannelClockStore(KeyValueStore<byte[], byte[]> store) {
        this.store = store;
    }

    @Override
    public void update(Uuid topicId, int partition, ParsleyClock clock) {
        byte[] key = key(topicId, partition);
        byte[] existing = store.get(key);
        ParsleyClock updated = existing == null
                ? clock
                : ParsleyClock.fromBytes(existing).merge(clock);
        store.put(key, updated.toBytes());
    }

    @Override
    public ParsleyClock get(Uuid topicId, int partition) {
        byte[] value = store.get(key(topicId, partition));
        return value == null ? ParsleyClock.empty() : ParsleyClock.fromBytes(value);
    }

    @Override
    public void remove(Uuid topicId, int partition) {
        store.delete(key(topicId, partition));
    }

    @Override
    public List<ChannelEntry> allEntries() {
        List<ChannelEntry> entries = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> iter = store.all()) {
            while (iter.hasNext()) {
                var kv = iter.next();
                Uuid topicId = topicIdFrom(kv.key);
                int partition = partitionFrom(kv.key);
                ParsleyClock clock = ParsleyClock.fromBytes(kv.value);
                entries.add(new ChannelEntry(topicId, partition, clock));
            }
        }
        return entries;
    }

    static byte[] key(Uuid topicId, int partition) {
        return ByteBuffer.allocate(KEY_SIZE)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .array();
    }

    private static Uuid topicIdFrom(byte[] key) {
        ByteBuffer buf = ByteBuffer.wrap(key);
        return new Uuid(buf.getLong(), buf.getLong());
    }

    private static int partitionFrom(byte[] key) {
        return ByteBuffer.wrap(key, 16, 4).getInt();
    }
}
