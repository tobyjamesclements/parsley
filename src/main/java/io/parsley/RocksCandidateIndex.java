package io.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ParsleyCandidateIndex} backed by a changelog-replicated Kafka {@link KeyValueStore}.
 *
 * <h2>Key layout (36 bytes, all fields big-endian)</h2>
 * <pre>
 * [topicId MSB      : 8B]
 * [topicId LSB      : 8B]
 * [partition        : 4B]
 * [requiredOffset   : 8B]
 * [recordId         : 8B]
 * </pre>
 *
 * <p>All numeric fields are encoded big-endian so that RocksDB's lexicographic comparator
 * reflects natural numeric order. A range scan of the form "coordinate C, requiredOffset ≤ N"
 * becomes {@code store.range(key(C, 0, 0), key(C, N, Long.MAX_VALUE))}.
 */
final class RocksCandidateIndex implements ParsleyCandidateIndex {

    private static final byte[] PRESENT = new byte[0];
    private static final int KEY_SIZE = 36;

    private final KeyValueStore<byte[], byte[]> store;

    RocksCandidateIndex(KeyValueStore<byte[], byte[]> store) {
        this.store = store;
    }

    @Override
    public void index(long recordId, ParsleyClock required, ParsleyClock frontier) {
        required.forEach((topicId, partition, offset) -> {
            if (frontier.offsetFor(topicId, partition) < offset) {
                store.put(key(topicId, partition, offset, recordId), PRESENT);
            }
        });
    }

    @Override
    public List<Candidate> findCandidates(Uuid topicId, int partition, long newOffset) {
        byte[] from = key(topicId, partition, 0L, 0L);
        byte[] to   = key(topicId, partition, newOffset, Long.MAX_VALUE);
        List<Candidate> candidates = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> iter = store.range(from, to)) {
            while (iter.hasNext()) {
                byte[] k = iter.next().key;
                long requiredOffset = ByteBuffer.wrap(k, 20, 8).getLong();
                long recordId       = ByteBuffer.wrap(k, 28, 8).getLong();
                candidates.add(new Candidate(topicId, partition, requiredOffset, recordId));
            }
        }
        return candidates;
    }

    @Override
    public void prune(Candidate candidate) {
        store.delete(key(candidate.topicId(), candidate.partition(),
                candidate.requiredOffset(), candidate.recordId()));
    }

    static byte[] key(Uuid topicId, int partition, long requiredOffset, long recordId) {
        return ByteBuffer.allocate(KEY_SIZE)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .putLong(requiredOffset)
                .putLong(recordId)
                .array();
    }
}
