package io.parsley;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
    public long add(ParsleyRecord<K, V> record) {
        long seq = nextSequence++;
        store.put(seq, serializer.serialize(record));
        size++;
        return seq;
    }

    @Override
    public Entry<K, V> get(long sequence) {
        byte[] value = store.get(sequence);
        if (value == null) return null;
        ParsleyRecord<K, V> record = serializer.deserialize(value);
        return new Entry<>(sequence, record, CausalDependencies.fromBytes(record.encodedDependencies()));
    }

    @Override
    public List<Entry<K, V>> entries() {
        List<Entry<K, V>> entries = new ArrayList<>(size);
        try (KeyValueIterator<Long, byte[]> all = store.all()) {
            while (all.hasNext()) {
                var kv = all.next();
                ParsleyRecord<K, V> record = serializer.deserialize(kv.value);
                entries.add(new Entry<>(kv.key, record, CausalDependencies.fromBytes(record.encodedDependencies())));
            }
        }
        // Iteration order across store implementations is not guaranteed to be key order, so sort by
        // sequence explicitly to hand back records in causal arrival order.
        entries.sort(Comparator.comparingLong(Entry::sequence));
        return entries;
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
}
