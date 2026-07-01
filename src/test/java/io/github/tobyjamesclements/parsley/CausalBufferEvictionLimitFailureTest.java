package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The {@code parsley.buffer.eviction.failure.policy} fail/continue branch: whether a
 * {@link CausalBufferLimit} firing fails the owning task fast (leaving every candidate record
 * buffered) or evicts and forwards them anyway, out of causal order — the original behaviour.
 *
 * <p>The contract under test: under {@code fail} (the default), the engine throws a typed
 * {@link ParsleyBufferEvictionLimitException} naming the oldest candidate and the unmet
 * dependencies, touches neither the buffer nor the user-serde value, and reports the event via
 * both the audit and metrics hooks; under {@code continue}, eviction proceeds exactly as it did
 * before this policy existed.
 */
class CausalBufferEvictionLimitFailureTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * The size limit's inline check (in {@code onRecord}) fails the task fast under the default
     * policy instead of evicting the record that just pushed the buffer over the limit: the
     * exception names the offending coordinate and gap, and the record remains buffered.
     */
    @Test
    void sizeLimitFailsFastByDefaultAndLeavesTheRecordBuffered() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, false, /* failOnEvictionLimit */ true);

        ParsleyBufferEvictionLimitException thrown = assertThrows(
                ParsleyBufferEvictionLimitException.class,
                () -> engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 99))),
                "reaching the size limit must fail fast under the default policy, not evict");

        assertEquals("t2", thrown.topic(), "the exception must name the oldest candidate's topic");
        assertEquals(0L, thrown.offset(), "the exception must name the oldest candidate's offset");
        assertEquals(1, buffer.size(), "the candidate record must remain buffered, not be evicted");
    }

    /**
     * The duration limit's punctuator-driven {@code evictExpired()} fails the task fast under the
     * default policy too, leaving the aged-out record buffered rather than evicting it.
     */
    @Test
    void durationLimitFailsFastByDefaultAndLeavesTheRecordBuffered() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofDuration(java.time.Duration.ofMillis(200)), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, clock::get, false, /* failOnEvictionLimit */ true);

        engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 99)));
        clock.set(300L);

        assertThrows(ParsleyBufferEvictionLimitException.class, engine::evictExpired,
                "an aged-out record must fail the task fast under the default policy, not be evicted");
        assertEquals(1, buffer.size(), "the aged-out record must remain buffered, not be evicted");
    }

    /**
     * {@code continue} restores Parsley's original always-forward behaviour: the candidate is
     * evicted and forwarded out of causal order instead of failing the task.
     */
    @Test
    void continuePolicyStillEvictsAndForwardsAsBefore() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, false, /* failOnEvictionLimit */ false);

        List<ParsleyMessage<String, String>> out = assertDoesNotThrow(
                () -> engine.onRecord(message(T2, 0, T2_ID, ParsleyClock.empty().observe(T1_ID, 0, 99))),
                "continue must not fail the task");

        assertEquals(List.of("t2"), out.stream().map(ParsleyMessage::topic).toList(),
                "continue must evict and forward the record anyway, out of causal order");
        assertEquals(0, buffer.size(), "the evicted record must leave the buffer");
    }

    /**
     * Both the audit and metrics hooks fire exactly once, naming the unmet dependency, before the
     * exception propagates — so an operator sees the event even though the task is about to fail.
     */
    @Test
    void auditAndMetricsFireBeforeTheExceptionPropagates() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        CountingMetrics metrics = new CountingMetrics();
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), metrics, audit,
                System::currentTimeMillis, false, /* failOnEvictionLimit */ true);

        assertThrows(ParsleyBufferEvictionLimitException.class,
                () -> engine.onRecord(message(T2, 0, T2_ID, unmet)));

        assertEquals(1, metrics.evictionLimitExceeded.get(), "the eviction-limit-exceeded metric must fire once");
        assertEquals(1, audit.evictionLimitExceeded.size(), "the eviction-limit-exceeded audit event must fire once");
        RecordingCausalAudit.EvictionLimitExceeded event = audit.evictionLimitExceeded.get(0);
        assertEquals("t2", event.topic(), "the audit event must name the candidate's topic");
        assertEquals(0L, event.offset(), "the audit event must name the candidate's offset");
        assertEquals(CausalDependencies.of(unmet.missing(ParsleyClock.empty())), event.gap(),
                "the audit event must name the unmet dependency as the shortfall against the frontier");
    }

    /**
     * A buffer restored from a changelog that already exceeds a since-lowered size limit fails
     * fast on the startup enforcement pass ({@code evictOverflow()} called directly, as
     * {@code ParsleyProcessor.init()} does) — the same as the inline per-record check, since both
     * funnel through the same branch point.
     */
    @Test
    void restoredOverflowFailsFastOnTheStartupEnforcementPass() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        buffer.add(message(T2, 0, T2_ID, unmet), 0L);
        buffer.add(message(T2, 1, T2_ID, unmet), 0L);

        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, false, /* failOnEvictionLimit */ true);

        assertThrows(ParsleyBufferEvictionLimitException.class, engine::evictOverflow,
                "a restored buffer already over a lowered limit must fail fast, not evict the excess");
        assertEquals(2, buffer.size(), "neither restored record must be evicted");
    }

    /**
     * Identifying the oldest candidate for the fail-fast exception never decodes the user-serde
     * value — it reads only the index metadata, exactly like the poison-record-immune restore path.
     * A buffer whose {@code get()} would throw (simulating an undecodable value) must still fail
     * fast on the eviction limit, never on a decode attempt it never makes.
     */
    @Test
    void identifyingTheCandidateForTheExceptionNeverDecodesTheValue() {
        NeverGetBufferStore<String, String> buffer = new NeverGetBufferStore<>();
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        buffer.add(message(T2, 0, T2_ID, unmet), 0L);

        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1), ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis, false, /* failOnEvictionLimit */ true);

        ParsleyBufferEvictionLimitException thrown = assertThrows(
                ParsleyBufferEvictionLimitException.class, engine::evictOverflow,
                "fail-fast eviction must never call get(), so a buffer that forbids it must still work");
        assertEquals("t2", thrown.topic(), "the exception must still name the candidate, from index metadata alone");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset, Uuid topicId,
                                                           ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }

    /** Counts the eviction-limit-exceeded callback; everything else is a no-op. */
    private static final class CountingMetrics implements ParsleyMetrics {
        final AtomicInteger evictionLimitExceeded = new AtomicInteger();
        @Override public void recordBuffered() {}
        @Override public void recordReleased(int c) {}
        @Override public void recordEvicted(int c) {}
        @Override public void recordViolation() {}
        @Override public void recordDeserializationError() {}
        @Override public void recordClockResolutionError() {}
        @Override public void recordEvictionLimitExceeded() { evictionLimitExceeded.incrementAndGet(); }
        @Override public void reportState(int depth, OptionalLong oldest) {}
    }

    /**
     * A {@link ParsleyBufferStore} whose {@link #get} fails the test immediately if ever called —
     * proving the fail-fast eviction path identifies its candidate from {@link #indexEntries}
     * alone, exactly like a real undecodable (poison) record's metadata is read without its value.
     */
    private static final class NeverGetBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final List<ParsleyMessage<K, V>> held = new ArrayList<>();
        private final List<Long> bufferedAt = new ArrayList<>();

        @Override public long add(ParsleyMessage<K, V> record, long at) {
            held.add(record);
            bufferedAt.add(at);
            return held.size() - 1L;
        }

        @Override public Entry<K, V> get(long sequence) {
            throw new AssertionError("fail-fast eviction must never decode the buffered value");
        }

        @Override public List<Entry<K, V>> entries() {
            throw new AssertionError("fail-fast eviction must never decode the buffered value");
        }

        @Override public List<IndexEntry> indexEntries() {
            List<IndexEntry> out = new ArrayList<>(held.size());
            for (int i = 0; i < held.size(); i++) {
                ParsleyMessage<K, V> m = held.get(i);
                if (m != null) out.add(new IndexEntry(i, bufferedAt.get(i), m.topic(), m.topicId(),
                        m.partition(), m.offset(), m.dependencies()));
            }
            return out;
        }

        @Override public void remove(long sequence) {
            held.set((int) sequence, null);
        }

        @Override public int size() {
            return (int) held.stream().filter(m -> m != null).count();
        }

        @Override public OptionalLong oldestBufferedAt() {
            for (int i = 0; i < held.size(); i++) {
                if (held.get(i) != null) return OptionalLong.of(bufferedAt.get(i));
            }
            return OptionalLong.empty();
        }
    }
}
