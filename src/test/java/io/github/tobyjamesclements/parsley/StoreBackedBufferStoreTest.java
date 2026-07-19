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
 * Tests {@link StoreBackedBufferStore} — the {@link org.apache.kafka.streams.state.KeyValueStore}-backed
 * {@link ParsleyBufferStore} implementation — against a real {@link KeyValueStore} (via
 * {@link TestKeyValueStore}), distinct from {@link ParsleyBufferStoreTest}, which exercises the same
 * contract against the purely in-memory {@link MockBufferStore}.
 */
class StoreBackedBufferStoreTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();

    private final ParsleySerializer<String, String> serializer =
            new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> Serdes.String()));

    /**
     * {@code get()} and {@code indexEntries()} both decode the records held in the backing store,
     * returning the same sequences, buffer-admission times, and dependency clocks.
     *
     * Asserts that both methods report the two added records with matching sequence, bufferedAt,
     * and dependencies, and that {@code get()} additionally returns the decoded record.
     */
    @Test
    void getAndIndexEntriesReadBackThroughTheRealStore() {
        StoreBackedBufferStore<String, String> store = new StoreBackedBufferStore<>(newRocksStore(), serializer);
        ParsleyVectorClock depsA = ParsleyVectorClock.empty().observe(T1_ID, 0, 9);
        ParsleyVectorClock depsB = ParsleyVectorClock.empty().observe(T1_ID, 0, 3);

        long seqA = store.add(record(T1, 0, depsA), 100L);
        long seqB = store.add(record(T1, 1, depsB), 200L);

        ParsleyBufferStore.Entry<String, String> first = store.get(seqA);
        assertEquals(0L, first.record().offset(), "the first sequence must decode the first added record");
        assertEquals(depsA, first.dependencies(), "the first record must carry its original dependencies");
        assertEquals(100L, first.bufferedAt(), "the first record must carry its bufferedAt");

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
     * Constructing a {@code StoreBackedBufferStore} over a backing store that already holds records (as
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
        backing.put(5L, packed(record(T1, 0, ParsleyVectorClock.empty()), 10L));
        backing.put(9L, packed(record(T1, 1, ParsleyVectorClock.empty()), 20L));

        StoreBackedBufferStore<String, String> restored = new StoreBackedBufferStore<>(backing, serializer);

        assertEquals(2, restored.size(), "size must be seeded from the pre-existing entries");
        long newSeq = restored.add(record(T1, 2, ParsleyVectorClock.empty()), 30L);
        assertEquals(10L, newSeq, "the next sequence must continue past the highest pre-existing sequence (9)");
        assertEquals(3, restored.indexEntries().size(), "all three records must now be present");
    }

    /**
     * {@code indexEntries()} decodes only the buffer-admission time and dependency clock — never the
     * user-serde key/value — so it succeeds even when the held value can no longer be deserialised.
     * This is what lets the candidate index rebuild from a restored buffer that contains a poison
     * record without blocking startup.
     *
     * Asserts that {@code indexEntries()} does not throw and returns the correct dependencies, while
     * {@code get()} (which does decode the value) does throw for the same record.
     */
    @Test
    void indexEntriesSurvivesAnUndecodableValue() {
        ParsleySerializer<String, String> poisonSerializer =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> new ThrowingDeserializerSerde()));
        StoreBackedBufferStore<String, String> store = new StoreBackedBufferStore<>(newRocksStore(), poisonSerializer);
        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(T1_ID, 0, 3);
        long seq = store.add(record(T1, 0, deps), 100L);

        assertThrows(ParsleyBufferDeserializationException.class, () -> store.get(seq),
                "get() decodes the value and must fail on a poison record");

        List<ParsleyBufferStore.IndexEntry> indexEntries =
                assertDoesNotThrow(store::indexEntries, "indexEntries() must not touch the value serde");
        assertEquals(1, indexEntries.size(), "the poison record's metadata must still be returned");
        assertEquals(deps, indexEntries.get(0).dependencies(), "the dependency clock must decode correctly");
    }

    /**
     * {@code add()} resolves and invokes the value serde using the buffered record's own source
     * topic — not any other name (e.g. a changelog topic) — which matters for topic-specific serdes
     * such as schema-registry Avro serdes, whose subject is derived from the topic argument.
     *
     * Asserts the value serde is invoked with exactly the record's source topic.
     */
    @Test
    void addResolvesTheValueSerdeUsingTheRecordsSourceTopic() {
        ParsleyProcessorsTopologyTest.SpyStringSerde valueSpy = new ParsleyProcessorsTopologyTest.SpyStringSerde();
        ParsleySerializer<String, String> spySerializer =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> valueSpy));
        StoreBackedBufferStore<String, String> store = new StoreBackedBufferStore<>(newRocksStore(), spySerializer);

        store.add(record(T1, 0, ParsleyVectorClock.empty()), 100L);

        assertEquals(List.of("t1"), valueSpy.serializeTopics,
                "the value serde must be invoked with the record's own source topic ('t1')");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static KeyValueStore<Long, byte[]> newRocksStore() {
        return new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder());
    }

    private byte[] packed(ParsleyMessage<String, String> record, long bufferedAt) {
        byte[] serialized = serializer.serialize(record);
        return java.nio.ByteBuffer.allocate(8 + serialized.length).putLong(bufferedAt).put(serialized).array();
    }

    private static ParsleyMessage<String, String> record(TopicPartition tp, long offset, ParsleyVectorClock deps) {
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
