package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Robustness of the buffer-read path when a held record can no longer be deserialised — the scenario
 * where the Schema Registry state for a buffered record's subject changes between buffer-write and
 * buffer-read (the writer schema id is deleted, or a redeployed reader schema is incompatible).
 *
 * <p>The contract under test: the failure surfaces as a typed {@link ParsleyBufferDeserializationException}
 * (a {@link RuntimeException}, never an {@link Error} — the JVM is never crashed), the offending record
 * stays in the buffer for recovery (the drain deserialises before it removes), the error is counted,
 * and — crucially — a poison record never blocks startup, because index restore decodes only the
 * dependency clock, never the user serde.
 */
class CausalBufferDeserializationFailureTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * A held record that can no longer be deserialised on drain raises a typed
     * {@link ParsleyBufferDeserializationException} (not an {@link Error}, so the JVM survives), leaves
     * the record in the buffer, and increments the deserialization-error metric.
     */
    @Test
    void drainFailureRaisesTypedErrorRetainsRecordAndCountsMetric() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        CountingMetrics metrics = new CountingMetrics();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(100), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), metrics);

        // Buffer a T2 record that depends on T1@3 — held, not yet decoded.
        engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(1, buffer.size(), "the record must be buffered while its dependency is unmet");

        // T1@3 arrives and triggers the drain, which must deserialise the held record — and fails.
        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class,
                () -> engine.onRecord(message(T1, 3, T1_ID, ParsleyClock.empty())),
                "an undecodable held record must surface a typed exception on drain");

        assertEquals("t2", thrown.topic(), "the exception must carry the held record's source topic");
        assertEquals(1, buffer.size(), "the undecodable record must remain buffered for recovery, not be dropped");
        assertEquals(1, metrics.deserializationErrors.get(), "the deserialization-error metric must be incremented");
    }

    /**
     * Constructing the engine over a buffer whose held values can no longer be deserialised does not
     * throw: index restore reads only the dependency clock (Parsley framing), never the user serde, so
     * a poison record cannot block startup. The failure is deferred to the forward path.
     */
    @Test
    void restoreSkipsValueDeserializationSoAPoisonRecordDoesNotBlockStartup() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        // Seed a held record as if it survived a restart with a now-undecodable value.
        buffer.add(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)), 0L);

        // Sanity: reading the full record would fail (the value can't be decoded)...
        assertThrows(ParsleyBufferDeserializationException.class, buffer::entries,
                "entries() decodes the value and would fail on a poison record");

        // ...but constructing the engine (which rebuilds the index from indexEntries()) must not.
        MockCandidateIndex index = new MockCandidateIndex();
        assertDoesNotThrow(
                () -> new ParsleyEngine<>(CausalBufferLimit.ofSize(100), ParsleyClock.empty(), c -> {},
                        buffer, index, ParsleyMetrics.NOOP),
                "index restore must not deserialise the value, so a poison record cannot block startup");
    }

    /**
     * {@link ParsleySerializer#deserialize} wraps a user-serde failure in a typed
     * {@link ParsleyBufferDeserializationException} carrying the held record's source coordinate, rather
     * than letting the raw {@link SerializationException} escape.
     */
    @Test
    void deserializeWrapsSerdeFailureWithTheSourceCoordinate() {
        ParsleySerializer<String, String> serializer = serializerWith(new ThrowingDeserializerSerde());
        byte[] bytes = serializer.serialize(message(T2, 7, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes),
                "a serde decode failure must be wrapped in a typed exception");
        assertEquals("t2", thrown.topic(), "the exception must carry the source topic");
        assertEquals(7L, thrown.offset(), "the exception must carry the source offset");
    }

    /**
     * {@link ParsleySerializer#deserializeDependencies} reads the dependency clock without invoking the
     * value serde, so it succeeds even when the value can no longer be decoded — the property that
     * makes index restore immune to a Schema Registry change.
     */
    @Test
    void deserializeDependenciesSurvivesAnUndecodableValue() {
        ParsleySerializer<String, String> serializer = serializerWith(new ThrowingDeserializerSerde());
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3);
        byte[] bytes = serializer.serialize(message(T2, 0, T2_ID, deps));

        ParsleyClock restored = assertDoesNotThrow(() -> serializer.deserializeDependencies(bytes),
                "metadata decode must not touch the value serde");
        assertEquals(deps, restored, "the dependency clock must round-trip without the value serde");
    }

    /**
     * The exception's {@link ParsleyBufferDeserializationException#details() details} carry the held
     * record's coordinate, dependencies, header keys, and value length for an operator log — but
     * <strong>never the payload bytes</strong> (those stay in the buffer changelog).
     */
    @Test
    void detailsCarryMetadataButNotThePayloadBytes() {
        ParsleySerializer<String, String> serializer = serializerWith(new ThrowingDeserializerSerde());
        byte[] bytes = serializer.serialize(message(T2, 7, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes));
        String details = thrown.details();

        assertTrue(details.contains("t2-0@7"), "details must carry the source coordinate: " + details);
        assertTrue(details.contains(T1_ID.toString()), "details must carry the dependency clock: " + details);
        assertTrue(details.contains("value bytes: 1"), "details must carry the value length (\"v\" = 1 byte): " + details);
        // "v" base64 is "dg==" / hex "76" — neither must appear; the payload is never rendered.
        assertTrue(!details.contains("dg==") && !details.contains("\"v\""),
                "details must NOT contain the payload bytes: " + details);
    }

    /**
     * In continue-mode (deserialization handler = {@code LogAndContinue}) an undecodable held record is
     * dropped on the drain path and processing continues: the satisfying record is still forwarded, the
     * poison record is removed, a violation is counted, and no exception propagates.
     */
    @Test
    void continueModeSkipsThePoisonRecordOnDrainInsteadOfFailing() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        CountingMetrics metrics = new CountingMetrics();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(100), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), metrics, System::currentTimeMillis, /* skip */ true);

        engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));  // held (poison)
        assertEquals(1, buffer.size(), "the poison record is buffered while its dependency is unmet");

        List<ParsleyMessage<String, String>> forwarded = assertDoesNotThrow(
                () -> engine.onRecord(message(T1, 3, T1_ID, ParsleyClock.empty())),
                "continue-mode must not propagate the decode failure");

        assertEquals(List.of("t1"), forwarded.stream().map(ParsleyMessage::topic).toList(),
                "the satisfying record is still forwarded; the poison record is dropped, not delivered");
        assertEquals(0, buffer.size(), "the dropped poison record must be removed from the buffer");
        assertEquals(1, metrics.deserializationErrors.get(), "the decode error is counted");
        assertEquals(1, metrics.violations.get(), "dropping the record counts a causal violation");
    }

    /**
     * In continue-mode, a poison record that overflows the size limit is skip-dropped by eviction
     * instead of crashing — the eviction path no longer all-or-nothing decodes the buffer.
     */
    @Test
    void continueModeSkipsThePoisonRecordOnEvictionInsteadOfFailing() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        CountingMetrics metrics = new CountingMetrics();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), metrics, System::currentTimeMillis, /* skip */ true);

        // Both depend on an unmet T1@3, so both are held; the second overflows the size-1 limit.
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 3);
        engine.onRecord(message(T2, 0, T2_ID, unmet));
        List<ParsleyMessage<String, String>> out = assertDoesNotThrow(
                () -> engine.onRecord(message(T2, 1, T2_ID, unmet)),
                "overflow eviction of poison records must not propagate the decode failure");

        assertTrue(out.isEmpty(), "skip-dropped records are not forwarded");
        assertEquals(0, buffer.size(), "overflow poison records are dropped from the buffer");
        assertTrue(metrics.deserializationErrors.get() >= 1, "the decode error(s) are counted");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleySerializer<String, String> serializerWith(Serde<String> valueSerde) {
        return new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> valueSerde));
    }

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset, Uuid topicId,
                                                           ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }

    /** Counts the failure/violation callbacks; everything else is a no-op. */
    private static final class CountingMetrics implements ParsleyMetrics {
        final AtomicInteger deserializationErrors = new AtomicInteger();
        final AtomicInteger violations = new AtomicInteger();
        @Override public void recordBuffered(int d) {}
        @Override public void recordReleased(int c, int d) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() { violations.incrementAndGet(); }
        @Override public void recordDeserializationError() { deserializationErrors.incrementAndGet(); }
    }

    /**
     * A {@link Serde} whose serializer is a normal {@link Serdes#String()} but whose deserializer always
     * throws {@link SerializationException} — simulating a held value the registry can no longer decode.
     */
    private static final class ThrowingDeserializerSerde implements Serde<String> {
        private final Serde<String> delegate = Serdes.String();
        @Override public Serializer<String> serializer() { return delegate.serializer(); }
        @Override public Deserializer<String> deserializer() {
            return (topic, data) -> { throw new SerializationException("registry can no longer decode " + topic); };
        }
    }

    /**
     * A buffer store that holds records but cannot decode them on the forward path: {@link #get} and
     * {@link #entries} throw {@link ParsleyBufferDeserializationException}, while {@link #indexEntries}
     * (dependency-only) succeeds — modelling a held record whose value became undecodable.
     */
    private static final class PoisonBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final List<ParsleyMessage<K, V>> held = new ArrayList<>();

        @Override public long add(ParsleyMessage<K, V> record, long bufferedAt) {
            held.add(record);
            return held.size() - 1L;
        }

        @Override public Entry<K, V> get(long sequence) {
            throw poison(held.get((int) sequence));
        }

        @Override public List<Entry<K, V>> entries() {
            if (!held.isEmpty()) throw poison(held.get(0));
            return List.of();
        }

        @Override public List<IndexEntry> indexEntries() {
            List<IndexEntry> out = new ArrayList<>(held.size());
            for (int i = 0; i < held.size(); i++) {
                if (held.get(i) != null) out.add(new IndexEntry(i, 0L, held.get(i).dependencies()));
            }
            return out;
        }

        @Override public void remove(long sequence) {
            if (sequence >= 0 && sequence < held.size()) held.set((int) sequence, null);
        }

        @Override public int size() {
            return (int) held.stream().filter(m -> m != null).count();
        }

        private static ParsleyBufferDeserializationException poison(ParsleyMessage<?, ?> m) {
            return new ParsleyBufferDeserializationException(
                    m.topic(), m.partition(), m.offset(), -1, "test poison " + m.topic(),
                    new SerializationException("poison"));
        }
    }
}
