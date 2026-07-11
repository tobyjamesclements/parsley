package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

/**
 * A {@link ParsleyBufferStore} backed by a changelog-replicated Kafka {@link KeyValueStore}, keyed by a
 * monotonic insertion sequence and valued by the {@link ParsleySerializer}-serialised record. This
 * is the authoritative, restart-durable home of held records: because the store <em>is</em> the
 * buffer, held records need no separate rehydration step — they are read back on the next drain.
 *
 * <p>The insertion-sequence counter and the live record count are seeded once from the store at
 * construction (a single pass over its keys), so a restarted instance continues the sequence past
 * whatever survived the previous run.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class RocksBufferStore<K, V> implements ParsleyBufferStore<K, V> {

    private final KeyValueStore<Long, byte[]> store;
    private final ParsleySerializer<K, V> serializer;
    private long nextSequence;
    private int size;

    RocksBufferStore(KeyValueStore<Long, byte[]> store, ParsleySerializer<K, V> serializer) {
        this.store = store;
        this.serializer = serializer;
        // Seed the sequence past anything that survived a previous run, and count what is held, in a
        // single pass — this replaces the old explicit "restore held records" step.
        long maxSequence = -1;
        int count = 0;
        try (KeyValueIterator<Long, byte[]> all = store.all()) {
            while (all.hasNext()) {
                maxSequence = Math.max(maxSequence, all.next().key);
                count++;
            }
        }
        this.nextSequence = maxSequence + 1;
        this.size = count;
    }

    @Override
    public long add(ParsleyMessage<K, V> record, long bufferedAt) {
        long seq = nextSequence++;
        store.put(seq, pack(bufferedAt, serializer.serialize(record)));
        size++;
        return seq;
    }

    @Override
    public @Nullable Entry<K, V> get(long sequence) {
        byte[] value = store.get(sequence);
        if (value == null) return null;
        return toEntry(sequence, value);
    }

    @Override
    public List<IndexEntry> indexEntries() {
        List<IndexEntry> entries = new ArrayList<>(size);
        try (KeyValueIterator<Long, byte[]> all = store.all()) {
            while (all.hasNext()) {
                var kv = all.next();
                // Decode only the buffered-at time, source coordinate, and dependency clock (Parsley
                // framing), never the user-serde key/value, so a record whose value can no longer be
                // decoded does not block restore or the drain scan.
                long bufferedAt = ByteBuffer.wrap(kv.value, 0, 8).getLong();
                ParsleySerializer.IndexMetadata meta =
                        serializer.deserializeIndexMetadata(Arrays.copyOfRange(kv.value, 8, kv.value.length));
                entries.add(new IndexEntry(kv.key, bufferedAt, meta.topic(), meta.topicId(), meta.partition(),
                        meta.offset(), meta.dependencies()));
            }
        }
        return entries;
    }

    private Entry<K, V> toEntry(long sequence, byte[] value) {
        long bufferedAt = ByteBuffer.wrap(value, 0, 8).getLong();
        ParsleyMessage<K, V> record = serializer.deserialize(Arrays.copyOfRange(value, 8, value.length));
        return new Entry<>(sequence, bufferedAt, record, record.dependencies());
    }

    private static byte[] pack(long bufferedAt, byte[] serializedRecord) {
        return ByteBuffer.allocate(8 + serializedRecord.length)
                .putLong(bufferedAt)
                .put(serializedRecord)
                .array();
    }

    @Override
    public void remove(long sequence) {
        if (store.delete(sequence) != null) {
            size--;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public OptionalLong oldestBufferedAt() {
        // A range seek to the lowest surviving sequence, not a full scan: RocksDB seeks directly to
        // the start key, and only the 8-byte bufferedAt prefix is read — the record itself is never
        // deserialised.
        try (KeyValueIterator<Long, byte[]> oldest = store.range(0L, Long.MAX_VALUE)) {
            if (!oldest.hasNext()) {
                return OptionalLong.empty();
            }
            byte[] value = oldest.next().value;
            return OptionalLong.of(ByteBuffer.wrap(value, 0, 8).getLong());
        }
    }
}
