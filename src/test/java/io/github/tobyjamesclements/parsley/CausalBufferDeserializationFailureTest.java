package io.github.tobyjamesclements.parsley;

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
import java.util.OptionalLong;
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
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), metrics);

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
                        buffer, index, new MockForwardedIndex(), ParsleyMetrics.NOOP),
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
     * {@link ParsleySerializer#deserializeIndexMetadata} reads the source coordinate and dependency
     * clock without invoking the value serde, so it succeeds even when the value can no longer be
     * decoded — the property that makes index restore immune to a Schema Registry change.
     */
    @Test
    void deserializeIndexMetadataSurvivesAnUndecodableValue() {
        ParsleySerializer<String, String> serializer = serializerWith(new ThrowingDeserializerSerde());
        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3);
        byte[] bytes = serializer.serialize(message(T2, 0, T2_ID, deps));

        ParsleySerializer.IndexMetadata restored = assertDoesNotThrow(
                () -> serializer.deserializeIndexMetadata(bytes), "metadata decode must not touch the value serde");
        assertEquals(deps, restored.dependencies(), "the dependency clock must round-trip without the value serde");
        assertEquals("t2", restored.topic(), "the source topic must round-trip without the value serde");
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
     * When the held value's bytes carry the Confluent wire-format magic byte ({@code 0x00}) followed
     * by a 4-byte writer schema id, {@link ParsleyBufferDeserializationException#details() details}
     * decodes and reports that exact id — pinning the bit-shift/mask arithmetic in
     * {@code ParsleySerializer.schemaId()}, which only the excluded Avro integration test otherwise
     * exercises (PIT's mutation profile excludes {@code *IT} classes).
     */
    @Test
    void detailsDecodeTheConfluentSchemaIdFromTheMagicByteValueBytes() {
        ParsleySerializer<String, String> serializer =
                serializerWith(new FixedBytesThrowingSerde(new byte[] {0x00, 0, 0, 0, 42}));
        byte[] bytes = serializer.serialize(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes));

        assertTrue(thrown.details().contains("schema id: 42"),
                "details must report the decoded schema id: " + thrown.details());
    }

    /**
     * The decoded schema id is correct when every byte of the 4-byte big-endian id is non-zero —
     * unlike a small id (e.g. {@code 42}), where the high three bytes are zero and a shift-direction
     * or OR/AND mutation on them is unobservable (0 shifted either way, or ORed/ANDed with 0, is
     * still 0). Pins the actual shift-and-mask arithmetic across all four bytes.
     */
    @Test
    void detailsDecodeAMultiByteSchemaIdAcrossAllFourBytes() {
        // 0x12345678 = 305419896; every byte is non-zero and distinct, so a shift-direction or
        // OR/AND mutation on any one of them changes the decoded result.
        ParsleySerializer<String, String> serializer =
                serializerWith(new FixedBytesThrowingSerde(new byte[] {0x00, 0x12, 0x34, 0x56, 0x78}));
        byte[] bytes = serializer.serialize(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes));

        assertTrue(thrown.details().contains("schema id: 305419896"),
                "details must report the correctly-decoded multi-byte schema id: " + thrown.details());
    }

    /**
     * A decoded schema id of exactly {@code 0} is reported as {@code "0"}, not {@code "n/a"} — the
     * precise boundary between "no magic byte found" ({@code -1}) and "magic byte found, id is 0".
     */
    @Test
    void detailsReportSchemaIdZeroRatherThanNotApplicable() {
        ParsleySerializer<String, String> serializer =
                serializerWith(new FixedBytesThrowingSerde(new byte[] {0x00, 0, 0, 0, 0}));
        byte[] bytes = serializer.serialize(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes));

        assertTrue(thrown.details().contains("schema id: 0"),
                "a schema id of exactly 0 must be reported as \"0\", not \"n/a\": " + thrown.details());
    }

    /**
     * When the held record's key is null, {@link ParsleyBufferDeserializationException#details() details}
     * reports {@code "key bytes: null"} rather than a length — the existing coverage above only
     * exercises a non-null key.
     */
    @Test
    void detailsReportNullKeyBytesWhenTheKeyIsNull() {
        ParsleySerializer<String, String> serializer = serializerWith(new ThrowingDeserializerSerde());
        ParsleyMessage<String, String> record = new ParsleyMessage<>(
                T2.topic(), T2_ID, T2.partition(), 7L, 0L, null, "v", List.of(),
                ParsleyClock.empty().observe(T1_ID, 0, 3));
        byte[] bytes = serializer.serialize(record);

        ParsleyBufferDeserializationException thrown = assertThrows(
                ParsleyBufferDeserializationException.class, () -> serializer.deserialize(bytes));

        assertTrue(thrown.details().contains("key bytes: null"),
                "details must report \"key bytes: null\" for a null key: " + thrown.details());
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
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(100), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), metrics, audit,
                System::currentTimeMillis, /* skip */ true, /* failOnEvictionLimit */ false);

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

        assertEquals(1, audit.deserializationFailures.size(), "the dropped poison record must be audited");
        RecordingCausalAudit.DeserializationFailure failure = audit.deserializationFailures.get(0);
        assertEquals("t2", failure.topic(), "audit must name the poison record's own topic");
        assertEquals(0L, failure.offset(), "audit must name the poison record's own offset");
        assertTrue(failure.dropped(), "continue-mode drops the record, which the audit must reflect");
    }

    /**
     * In continue-mode, a poison record that overflows the size limit is skip-dropped by eviction
     * instead of crashing — the eviction path no longer all-or-nothing decodes the buffer.
     */
    @Test
    void continueModeSkipsThePoisonRecordOnEvictionInsteadOfFailing() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        CountingMetrics metrics = new CountingMetrics();
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), metrics, audit,
                System::currentTimeMillis, /* skip */ true, /* failOnEvictionLimit */ false);

        // Both depend on an unmet T1@3, so both are held; the second overflows the size-1 limit.
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 3);
        engine.onRecord(message(T2, 0, T2_ID, unmet));
        List<ParsleyMessage<String, String>> out = assertDoesNotThrow(
                () -> engine.onRecord(message(T2, 1, T2_ID, unmet)),
                "overflow eviction of poison records must not propagate the decode failure");

        assertTrue(out.isEmpty(), "skip-dropped records are not forwarded");
        assertEquals(0, buffer.size(), "overflow poison records are dropped from the buffer");
        assertTrue(metrics.deserializationErrors.get() >= 1, "the decode error(s) are counted");

        assertTrue(audit.deserializationFailures.size() >= 1, "the overflow-dropped poison record(s) must be audited");
        assertTrue(audit.deserializationFailures.stream().allMatch(RecordingCausalAudit.DeserializationFailure::dropped),
                "continue-mode drops every overflow poison record, which the audit must reflect");
    }

    /**
     * Dropping a poison record produces the same contiguous-frontier catch-up jump as a natural
     * release or an eviction: the dropped record's own coordinate still gets marked forwarded and
     * walked, absorbing every already-forwarded record above it in one step. Release, eviction, and
     * poison-drop all feed the exact same bookkeeping — none are special-cased.
     */
    @Test
    void continueModePoisonDropCatchesUpThroughEverythingAlreadyForwardedAboveIt() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(100), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, /* skip */ true, /* failOnEvictionLimit */ false);

        // T1@5 is held (poison: its value can never be decoded) on a dependency T2@0 will satisfy.
        engine.onRecord(message(T1, 5, T1_ID, ParsleyClock.empty().observe(T2_ID, 0, 0)));

        // T1@6, T1@7, T1@8 each forward immediately, piling up above the gap.
        engine.onRecord(message(T1, 6, T1_ID, ParsleyClock.empty()));
        engine.onRecord(message(T1, 7, T1_ID, ParsleyClock.empty()));
        engine.onRecord(message(T1, 8, T1_ID, ParsleyClock.empty()));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0), "frontier stalls below the held poison record");

        // T2@0 satisfies T1@5's own dependency, triggering a drain attempt on T1@5 — which fails to
        // decode and is dropped (continue-mode) rather than released, but its coordinate is still
        // accounted for: the frontier catches up through 6, 7, and 8 in the same step.
        List<ParsleyMessage<String, String>> forwarded = assertDoesNotThrow(
                () -> engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty())),
                "the poison drop on drain must not propagate the decode failure");

        assertEquals(List.of("t2"), forwarded.stream().map(ParsleyMessage::topic).toList(),
                "only T2@0 is forwarded; the poison record is dropped, not delivered");
        assertEquals(8L, engine.frontier().offsetFor(T1_ID, 0),
                "dropping the poison record must catch the frontier up through everything forwarded above it");
    }

    /**
     * The same catch-up jump and drain cascade happen when the poison record is reached through
     * {@code evictSequences} (a size limit firing) rather than {@code drainInto}'s own scan —
     * eviction-driven and drain-driven poison-drops feed the exact same contiguous-clock
     * bookkeeping, with no special-casing between them.
     */
    @Test
    void continueModePoisonDropViaEvictionCascadesThroughEverythingAlreadyForwardedAboveIt() {
        PoisonBufferStore<String, String> buffer = new PoisonBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(3), ParsleyClock.empty(), c -> {},
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, /* skip */ true, /* failOnEvictionLimit */ false);

        // T1@5 is held (poison) on a dependency that will never be satisfied in this test.
        engine.onRecord(message(T1, 5, T1_ID, ParsleyClock.empty().observe(T2_ID, 0, 99)));

        // T1@6, T1@7, T1@8 each forward immediately, piling up above the gap.
        engine.onRecord(message(T1, 6, T1_ID, ParsleyClock.empty()));
        engine.onRecord(message(T1, 7, T1_ID, ParsleyClock.empty()));
        engine.onRecord(message(T1, 8, T1_ID, ParsleyClock.empty()));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0), "frontier stalls below the held poison record");

        // T1@9 depends on exactly T1@8 (also poison, held) — indexed under (T1_ID, 0, 8). When the
        // eviction below closes the gap at 5, the cascade must reach all the way to T1@9.
        engine.onRecord(message(T1, 9, T1_ID, ParsleyClock.empty().observe(T1_ID, 0, 8)));

        // T2@0 (unrelated, never-satisfied) overflows the size-3 buffer (T1@5, T1@9, T2@0),
        // force-evicting T1@5 — the oldest — through evictSequences, not drainInto.
        List<ParsleyMessage<String, String>> forwarded = assertDoesNotThrow(
                () -> engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T2_ID, 0, 99))),
                "eviction of the poison record must not propagate the decode failure");

        assertTrue(forwarded.isEmpty(), "both poison records (T1@5, T1@9) are dropped, not delivered");
        assertEquals(1, buffer.size(), "only T2@0 remains held");
        assertEquals(9L, engine.frontier().offsetFor(T1_ID, 0),
                "evicting T1@5 must cascade all the way through T1@9 via drainInto, not just close its own gap");
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
        @Override public void recordBuffered() {}
        @Override public void recordReleased(int c) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() { violations.incrementAndGet(); }
        @Override public void recordDeserializationError() { deserializationErrors.incrementAndGet(); }
        @Override public void recordClockResolutionError() {}
        @Override public void recordEvictionLimitExceeded() {}
        @Override public void reportState(int depth, OptionalLong oldest) {}
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
     * A {@link Serde} whose serializer always returns a fixed byte array (ignoring the value passed
     * in) and whose deserializer always throws — lets a test control the exact bytes
     * {@code ParsleySerializer.schemaId()} decodes from, including the Confluent magic byte
     * ({@code 0x00}) followed by a 4-byte writer schema id.
     */
    private static final class FixedBytesThrowingSerde implements Serde<String> {
        private final byte[] bytes;
        FixedBytesThrowingSerde(byte[] bytes) { this.bytes = bytes; }
        @Override public Serializer<String> serializer() { return (topic, data) -> bytes; }
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
            ParsleyMessage<K, V> m = held.get((int) sequence);
            if (m == null) return null; // already removed — matches the real @Nullable contract
            throw poison(m);
        }

        @Override public List<Entry<K, V>> entries() {
            if (!held.isEmpty()) throw poison(held.get(0));
            return List.of();
        }

        @Override public List<IndexEntry> indexEntries() {
            List<IndexEntry> out = new ArrayList<>(held.size());
            for (int i = 0; i < held.size(); i++) {
                ParsleyMessage<K, V> m = held.get(i);
                if (m != null) out.add(new IndexEntry(i, 0L, m.topic(), m.topicId(), m.partition(), m.offset(),
                        m.dependencies()));
            }
            return out;
        }

        @Override public void remove(long sequence) {
            if (sequence >= 0 && sequence < held.size()) held.set((int) sequence, null);
        }

        @Override public int size() {
            return (int) held.stream().filter(m -> m != null).count();
        }

        @Override public OptionalLong oldestBufferedAt() {
            for (ParsleyMessage<K, V> m : held) {
                if (m != null) return OptionalLong.of(0L);
            }
            return OptionalLong.empty();
        }

        private static ParsleyBufferDeserializationException poison(ParsleyMessage<?, ?> m) {
            return new ParsleyBufferDeserializationException(
                    m.topic(), m.topicId(), m.partition(), m.offset(), -1, "test poison " + m.topic(),
                    new SerializationException("poison"));
        }
    }
}
