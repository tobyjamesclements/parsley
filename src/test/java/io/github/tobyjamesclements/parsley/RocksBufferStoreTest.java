package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link RocksBufferStore} — the {@link org.apache.kafka.streams.state.KeyValueStore}-backed
 * {@link ParsleyBufferStore} implementation — against a real {@link KeyValueStore} (via
 * {@link TestKeyValueStore}), distinct from {@link ParsleyBufferStoreTest}, which exercises the same
 * contract against the purely in-memory {@link MockBufferStore}.
 */
class RocksBufferStoreTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();

    private final ParsleySerializer<String, String> serializer =
            new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> Serdes.String()));

    /**
     * {@code entries()} and {@code indexEntries()} both decode the records held in the backing
     * store, returning the same sequences, buffer-admission times, and dependency clocks.
     *
     * Asserts that both methods report the two added records with matching sequence, bufferedAt,
     * and dependencies, and that {@code entries()} additionally returns the decoded record.
     */
    @Test
    void entriesAndIndexEntriesReadBackThroughTheRealStore() {
        RocksBufferStore<String, String> store = new RocksBufferStore<>(newRocksStore(), serializer);
        ParsleyClock depsA = ParsleyClock.empty().observe(T1_ID, 0, 9);
        ParsleyClock depsB = ParsleyClock.empty().observe(T1_ID, 0, 3);

        long seqA = store.add(record(T1, 0, depsA), 100L);
        long seqB = store.add(record(T1, 1, depsB), 200L);

        List<ParsleyBufferStore.Entry<String, String>> entries = store.entries();
        assertEquals(2, entries.size(), "entries() must return both added records");
        assertEquals(seqA, entries.get(0).sequence(), "entries() must be ordered by insertion sequence");
        assertEquals(0L, entries.get(0).record().offset(), "first entry must decode the first added record");
        assertEquals(depsA, entries.get(0).dependencies(), "first entry must carry its original dependencies");
        assertEquals(100L, entries.get(0).bufferedAt(), "first entry must carry its bufferedAt");

        List<ParsleyBufferStore.IndexEntry> indexEntries = store.indexEntries();
        assertEquals(2, indexEntries.size(), "indexEntries() must return both added records' metadata");
        var indexA = indexEntries.stream().filter(e -> e.sequence() == seqA).findFirst().orElseThrow();
        var indexB = indexEntries.stream().filter(e -> e.sequence() == seqB).findFirst().orElseThrow();
        assertEquals(100L, indexA.bufferedAt(), "indexEntries() must carry the first record's bufferedAt");
        assertEquals(depsA, indexA.dependencies(), "indexEntries() must carry the first record's dependencies");
        assertEquals(200L, indexB.bufferedAt(), "indexEntries() must carry the second record's bufferedAt");
        assertEquals(depsB, indexB.dependencies(), "indexEntries() must carry the second record's dependencies");
    }

    /**
     * Constructing a {@code RocksBufferStore} over a backing store that already holds records (as
     * after a restart, restored from the changelog) seeds the insertion-sequence counter and size
     * from what is already there, rather than starting from zero and risking a sequence collision.
     *
     * Asserts the restored size matches the pre-populated entries, and that the next record added
     * is assigned a sequence past the highest pre-existing one.
     */
    @Test
    void constructorSeedsSequenceAndSizeFromAPreExistingStore() {
        KeyValueStore<Long, byte[]> backing = newRocksStore();
        // Simulate a prior run that buffered records at sequences 5 and 9 (a gap is fine — sequences
        // need not be contiguous after removals).
        backing.put(5L, packed(record(T1, 0, ParsleyClock.empty()), 10L));
        backing.put(9L, packed(record(T1, 1, ParsleyClock.empty()), 20L));

        RocksBufferStore<String, String> restored = new RocksBufferStore<>(backing, serializer);

        assertEquals(2, restored.size(), "size must be seeded from the pre-existing entries");
        long newSeq = restored.add(record(T1, 2, ParsleyClock.empty()), 30L);
        assertEquals(10L, newSeq, "the next sequence must continue past the highest pre-existing sequence (9)");
        assertEquals(3, restored.entries().size(), "all three records must now be present");
    }

    /**
     * {@code indexEntries()} decodes only the buffer-admission time and dependency clock — never the
     * user-serde key/value — so it succeeds even when the held value can no longer be deserialised.
     * This is what lets the candidate index rebuild from a restored buffer that contains a poison
     * record without blocking startup.
     *
     * Asserts that {@code indexEntries()} does not throw and returns the correct dependencies, while
     * {@code entries()} (which does decode the value) does throw for the same record.
     */
    @Test
    void indexEntriesSurvivesAnUndecodableValue() {
        ParsleySerializer<String, String> poisonSerializer =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> new ThrowingDeserializerSerde()));
        RocksBufferStore<String, String> store = new RocksBufferStore<>(newRocksStore(), poisonSerializer);
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3);
        store.add(record(T1, 0, deps), 100L);

        assertThrows(ParsleyBufferDeserializationException.class, store::entries,
                "entries() decodes the value and must fail on a poison record");

        List<ParsleyBufferStore.IndexEntry> indexEntries =
                assertDoesNotThrow(store::indexEntries, "indexEntries() must not touch the value serde");
        assertEquals(1, indexEntries.size(), "the poison record's metadata must still be returned");
        assertEquals(deps, indexEntries.get(0).dependencies(), "the dependency clock must decode correctly");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static KeyValueStore<Long, byte[]> newRocksStore() {
        return new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder());
    }

    private byte[] packed(ParsleyMessage<String, String> record, long bufferedAt) {
        byte[] serialized = serializer.serialize(record);
        return java.nio.ByteBuffer.allocate(8 + serialized.length).putLong(bufferedAt).put(serialized).array();
    }

    private static ParsleyMessage<String, String> record(TopicPartition tp, long offset, ParsleyClock deps) {
        return new ParsleyMessage<>(tp.topic(), T1_ID, tp.partition(), offset, 0L, "k", "v", List.of(), deps);
    }

    /** A {@link Serde} whose deserializer always throws, simulating a value that can no longer be decoded. */
    private static final class ThrowingDeserializerSerde implements Serde<String> {
        private final Serde<String> delegate = Serdes.String();
        @Override public Serializer<String> serializer() { return delegate.serializer(); }
        @Override public Deserializer<String> deserializer() {
            return (topic, data) -> { throw new SerializationException("registry can no longer decode " + topic); };
        }
    }
}
