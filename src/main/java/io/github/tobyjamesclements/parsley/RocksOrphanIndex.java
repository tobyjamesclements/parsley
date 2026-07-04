package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;

/**
 * A {@link ParsleyOrphanIndex} backed by a changelog-replicated Kafka {@link KeyValueStore}.
 *
 * <h2>Key layout (20 bytes, all fields big-endian)</h2>
 * <pre>
 * [topicId MSB : 8B]
 * [topicId LSB : 8B]
 * [partition   : 4B]
 * </pre>
 *
 * <p>The value is the 8-byte big-endian orphan floor.
 */
final class RocksOrphanIndex implements ParsleyOrphanIndex {

    private static final int KEY_SIZE = 20;

    private final KeyValueStore<byte[], byte[]> store;

    RocksOrphanIndex(KeyValueStore<byte[], byte[]> store) {
        this.store = store;
    }

    @Override
    public boolean markOrphaned(Uuid topicId, int partition, long floor) {
        byte[] key = key(topicId, partition);
        byte[] existing = store.get(key);
        long existingFloor = existing == null ? -1L : ByteBuffer.wrap(existing).getLong();
        if (existingFloor >= floor) {
            return false;
        }
        store.put(key, ByteBuffer.allocate(8).putLong(floor).array());
        return true;
    }

    @Override
    public long orphanFloor(Uuid topicId, int partition) {
        byte[] existing = store.get(key(topicId, partition));
        return existing == null ? -1L : ByteBuffer.wrap(existing).getLong();
    }

    @Override
    public void prune(Uuid topicId, int partition) {
        store.delete(key(topicId, partition));
    }

    static byte[] key(Uuid topicId, int partition) {
        return ByteBuffer.allocate(KEY_SIZE)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .array();
    }
}
