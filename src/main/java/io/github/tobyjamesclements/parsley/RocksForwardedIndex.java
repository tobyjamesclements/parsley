package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ParsleyForwardedIndex} backed by a changelog-replicated Kafka {@link KeyValueStore}.
 *
 * <h2>Key layout (28 bytes, all fields big-endian)</h2>
 * <pre>
 * [topicId MSB : 8B]
 * [topicId LSB : 8B]
 * [partition   : 4B]
 * [offset      : 8B]
 * </pre>
 *
 * <p>All numeric fields are encoded big-endian so that RocksDB's lexicographic comparator
 * reflects natural numeric order. A range scan of the form "coordinate C, offset &gt; N" becomes
 * {@code store.range(key(C, N + 1), key(C, Long.MAX_VALUE))}.
 */
final class RocksForwardedIndex implements ParsleyForwardedIndex {

    private static final byte[] PRESENT = new byte[0];
    private static final int KEY_SIZE = 28;

    private final KeyValueStore<byte[], byte[]> store;

    RocksForwardedIndex(KeyValueStore<byte[], byte[]> store) {
        this.store = store;
    }

    @Override
    public void mark(Uuid topicId, int partition, long offset) {
        store.put(key(topicId, partition, offset), PRESENT);
    }

    @Override
    public List<Long> forwardedAfter(Uuid topicId, int partition, long frontierOffset) {
        byte[] from = key(topicId, partition, frontierOffset + 1);
        byte[] to = key(topicId, partition, Long.MAX_VALUE);
        List<Long> offsets = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> iter = store.range(from, to)) {
            while (iter.hasNext()) {
                byte[] k = iter.next().key;
                offsets.add(ByteBuffer.wrap(k, 20, 8).getLong());
            }
        }
        return offsets;
    }

    @Override
    public void unmark(Uuid topicId, int partition, long offset) {
        store.delete(key(topicId, partition, offset));
    }

    @Override
    public void pruneAtOrBelow(Uuid topicId, int partition, long watermark) {
        if (watermark < 0) {
            return;
        }
        byte[] from = key(topicId, partition, 0L);
        byte[] to = key(topicId, partition, watermark + 1);
        List<byte[]> stale = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> iter = store.range(from, to)) {
            while (iter.hasNext()) {
                stale.add(iter.next().key);
            }
        }
        // Deleted after the iterator closes, not during the range scan.
        for (byte[] key : stale) {
            store.delete(key);
        }
    }

    static byte[] key(Uuid topicId, int partition, long offset) {
        return ByteBuffer.allocate(KEY_SIZE)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .putLong(offset)
                .array();
    }
}
