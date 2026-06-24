package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyEngineTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    private final List<ParsleyMessage<String, String>> forwarded = new ArrayList<>();
    private final List<ParsleyClock> frontiers = new ArrayList<>();
    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();
    private final MockForwardedIndex forwardedIndex = new MockForwardedIndex();

    // --- test methods ---------------------------------------------------------------------------

    /**
     * A record whose causal dependencies are already satisfied by the current frontier
     * is forwarded to the downstream processor immediately, without entering the buffer.
     *
     * <p>The frontier is advanced to reflect the record's source coordinate on forward, and
     * the record is stamped in causal order.
     *
     * Asserts that exactly one record is forwarded, it carries the SATISFIED result, and the
     * frontier reflects the admitted record's source offset.
     */
    @Test
    void satisfiedRecordForwardsImmediatelyAndAdvancesFrontier() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));

        assertEquals(1, forwarded.size(), "satisfied record must be forwarded immediately");
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 3), engine.frontier(),
                "frontier must advance to the forwarded record's source offset");
    }

    /**
     * A record whose dependencies are not yet satisfied is held in the buffer until the
     * frontier advances to cover the required coordinate.
     *
     * <p>When the dependency arrives, the engine releases the buffered record after the
     * satisfying record and advances the frontier through both. Both released records are
     * stamped in causal order — the result reflects whether the engine ever
     * had to forcibly evict, not how long a record waited.
     *
     * Asserts that the buffered record is released in causal order after the satisfying record,
     * both carry the SATISFIED result, and the frontier reflects both records' source offsets.
     */
    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        ParsleyClock deps = ParsleyClock.empty().observe(T1_ID, 0, 3);
        processRecord(engine, incomingRecord(T2, 0, deps));
        assertTrue(forwarded.isEmpty(), "unsatisfied record must be held in the buffer");

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded after the dependency arrives");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "buffered record must be released second");
        assertEquals(
                ParsleyClock.empty()
                        .observe(T1_ID, 0, 3)
                        .observe(T2_ID, 0, 0),
                engine.frontier(),
                "frontier must reflect both records after drain");
    }

    /**
     * An unsatisfied record is held in the buffer store until its dependency is satisfied,
     * then removed from the buffer on drain.
     *
     * Asserts that the buffer size is 1 while the record is held and 0 after the drain.
     */
    @Test
    void bufferHoldsAnUnsatisfiedRecordAndReleasesItOnDrain() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        engine.onRecord(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(1, buffer.size(), "unsatisfied record must be held in the buffer");

        engine.onRecord(incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(0, buffer.size(), "drained record must be removed from the buffer");
    }

    /**
     * When the buffer is at its size limit and a new unsatisfied record arrives, the engine
     * always evicts the oldest record needed to bring the buffer back under the limit and
     * forwards it — there is no drop/discard outcome anymore. The evicted record is stamped
     * out of causal order (evicted).
     *
     * Asserts that the record is removed from the buffer and forwarded with the EVICTED result.
     */
    @Test
    void evictionRemovesTheRecordFromTheBufferAndForwardsItAnyway() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(1));

        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 99)));

        assertEquals(0, buffer.size(), "evicted record must never remain in the buffer");
        assertEquals(1, forwarded.size(), "evicted record must still be forwarded — Parsley never drops");
    }

    /**
     * Records pre-loaded into the buffer store before the engine is constructed are drained
     * when the frontier subsequently advances to satisfy their dependencies.
     *
     * <p>This simulates recovery: the engine restores buffered records from RocksDB on startup,
     * then drains them as satisfying records arrive.
     *
     * Asserts that pre-buffered records are released in causal order and that the frontier
     * does not advance until the dependency arrives.
     */
    @Test
    void recordsAlreadyInTheBufferDrainWhenTheFrontierCatchesUp() {
        buffer.add(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)), 0L);
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));
        assertEquals(ParsleyClock.empty(), engine.frontier(), "pre-buffered records must not advance the frontier on construction");
        assertTrue(frontiers.isEmpty(), "pre-buffered records must not fire the frontier listener on construction");

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));

        assertEquals(2, forwarded.size(), "satisfying record and pre-buffered record must both be forwarded");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "pre-buffered record must be released second");
    }

    /**
     * The engine never folds an inbound record's dependency clock into the frontier.
     * Only the record's own source coordinate is used to advance the frontier.
     *
     * <p>This invariant prevents a record carrying a large dependency set from artificially
     * widening the frontier and inadvertently releasing unrelated buffered records.
     *
     * Asserts that after processing a record with 200 dependency entries, the frontier
     * contains only the record's own source coordinate.
     */
    @Test
    void inboundClockIsNeverFoldedIntoTheFrontier() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(1));

        ParsleyClock big = ParsleyClock.empty();
        Uuid ghostId = Uuid.randomUuid();
        for (int p = 0; p < 200; p++) {
            big = big.observe(ghostId, p, 1_000 + p);
        }
        processRecord(engine, incomingRecord(T2, 0, big));

        assertEquals(ParsleyClock.empty().observe(T2_ID, 0, 0), engine.frontier(),
                "frontier must contain only the record's own source coordinate");
        assertEquals(1, engine.frontier().size(),
                "inbound dependency entries must not widen the frontier");
    }

    /**
     * A message with empty dependencies is trivially satisfied by any frontier — it is forwarded
     * immediately and advances the frontier through its own source coordinate. (Absent and
     * undecodable dependency headers are normalised to empty by {@link ParsleyMessage#from}, covered
     * in {@link ParsleyMessageTest}.)
     *
     * Asserts that the record is forwarded and the frontier advances through its source coordinate.
     */
    @Test
    void emptyDependenciesRecordIsForwardedAsTriviallySatisfied() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty()));

        assertEquals(1, forwarded.size(), "a trivially-satisfied record must be forwarded");
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 0), engine.frontier(),
                "frontier must advance through the forwarded record");
    }

    /**
     * When the buffer reaches its size limit and a new unsatisfied record arrives, the engine
     * evicts only the oldest buffered record needed to bring the buffer back under the limit,
     * leaving the rest held. The evicted record is forwarded, stamped EVICTED.
     *
     * Asserts that only the oldest record is forwarded, stamped EVICTED, the frontier advances
     * only through the evicted record, and the younger record remains buffered.
     */
    @Test
    void sizeLimitEvictsOnlyTheOldestRecord() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);

        processRecord(engine, incomingRecord(T2, 0, unmet));
        assertTrue(forwarded.isEmpty(), "first record must be buffered while limit not yet reached");
        processRecord(engine, incomingRecord(T2, 1, unmet)); // hits size 2 → evict only the oldest

        assertEquals(1, forwarded.size(), "only the oldest evicted record must be forwarded");
        assertEquals(ParsleyClock.empty().observe(T2_ID, 0, 0), engine.frontier(),
                "frontier must advance only through the evicted record");
        assertEquals(1, buffer.size(), "the younger record must remain held in the buffer");
    }

    /**
     * A size limit evicts the buffer's oldest record one at a time as new records keep arriving,
     * sliding the window forward rather than discarding everything on each overflow.
     *
     * Asserts that each overflow evicts exactly one record, oldest-first, and the buffer never
     * exceeds the configured size.
     */
    @Test
    void sizeLimitSlidesTheWindowEvictingOneOldestRecordPerOverflow() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);

        processRecord(engine, incomingRecord(T2, 0, unmet)); // A: depth 1, no eviction
        assertTrue(forwarded.isEmpty(), "no eviction while under the limit");

        processRecord(engine, incomingRecord(T2, 1, unmet)); // B: depth 2 → evict A
        assertEquals(List.of(0L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "the first overflow must evict only the oldest record (A)");
        assertEquals(1, buffer.size(), "buffer must hold exactly the size limit after eviction");

        processRecord(engine, incomingRecord(T2, 2, unmet)); // C: depth 2 → evict B
        assertEquals(List.of(0L, 1L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "the second overflow must evict only the next-oldest record (B)");
        assertEquals(1, buffer.size(), "buffer must never exceed the configured size limit");
    }

    /**
     * Simulates a restart after the configured size limit was lowered: the buffer (e.g. restored
     * from a changelog) already holds more records than the new limit allows, and nothing has
     * trimmed it back yet because the inline check in {@code onRecord()} only fires on the next
     * admission. {@code evictOverflow()} is invoked directly here, the same way
     * {@code ParsleyProcessor.init()} invokes it once right after construction.
     *
     * Asserts that exactly the oldest excess records are evicted, in oldest-first order, leaving
     * the buffer at exactly the new limit.
     */
    @Test
    void evictOverflowTrimsARestoredBufferThatAlreadyExceedsTheLimit() {
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        buffer.add(incomingRecord(T2, 0, unmet), 0L);
        buffer.add(incomingRecord(T2, 1, unmet), 1L);
        buffer.add(incomingRecord(T2, 2, unmet), 2L);

        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));
        forwarded.addAll(engine.evictOverflow());

        assertEquals(List.of(0L, 1L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "the two oldest excess records must be evicted, oldest-first");
        assertEquals(1, buffer.size(), "the buffer must be trimmed down to exactly the new limit");
    }

    /**
     * {@code evictOverflow()} must be a safe no-op when called on a buffer that is already at or
     * under the configured limit — the call site added to {@code ParsleyProcessor.init()} invokes
     * it unconditionally on every restart, including the common case where nothing needs trimming.
     */
    @Test
    void evictOverflowIsANoOpWhenBufferIsNotOverTheLimit() {
        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        buffer.add(incomingRecord(T2, 0, unmet), 0L);

        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));
        List<ParsleyMessage<String, String>> result = engine.evictOverflow();

        assertTrue(result.isEmpty(), "no eviction must occur while the buffer is within the limit");
        assertEquals(1, buffer.size(), "the buffer must be left untouched");
    }

    /**
     * When the buffer limit is duration-based, the engine exposes the configured eviction
     * interval so callers can schedule punctuators.
     *
     * Asserts that {@code evictionInterval()} returns the configured duration.
     */
    @Test
    void durationLimitExposesEvictionInterval() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofDuration(Duration.ofSeconds(5)));
        assertEquals(Duration.ofSeconds(5), engine.evictionInterval().orElseThrow(),
                "eviction interval must match the configured duration");
    }

    /**
     * {@code evictExpired()} must leave a buffered record alone if it hasn't aged past the
     * configured duration yet, even when the punctuator fires.
     *
     * Asserts that the record remains in the buffer.
     */
    @Test
    void evictExpiredLeavesRecordsYoungerThanTheDurationInTheBuffer() {
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = engineWithClock(CausalBufferLimit.ofDuration(Duration.ofMillis(200)), clock::get);

        processRecord(engine, incomingRecord(T2, 0,
                ParsleyClock.empty().observe(T1_ID, 0, 99)));
        clock.set(150L);

        List<ParsleyMessage<String, String>> forwardedOnEvict = engine.evictExpired();

        assertTrue(forwardedOnEvict.isEmpty(), "a record younger than the duration must not be evicted");
        assertEquals(1, buffer.size(), "the record must remain held in the buffer");
    }

    /**
     * {@code evictExpired()} evicts a buffered record once it has aged past the configured
     * duration, forwarding it stamped EVICTED.
     *
     * Asserts that the record is removed from the buffer and forwarded, stamped EVICTED.
     */
    @Test
    void evictExpiredEvictsRecordsOlderThanTheDuration() {
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = engineWithClock(CausalBufferLimit.ofDuration(Duration.ofMillis(200)), clock::get);

        processRecord(engine, incomingRecord(T2, 0,
                ParsleyClock.empty().observe(T1_ID, 0, 99)));
        clock.set(250L);

        List<ParsleyMessage<String, String>> evicted = engine.evictExpired();

        assertEquals(0, buffer.size(), "a record older than the duration must be evicted");
        assertEquals(1, evicted.size(), "the aged-out record must be forwarded, not dropped");
    }

    /**
     * When the buffer holds records admitted at different times, {@code evictExpired()} evicts
     * only the ones old enough, in oldest-first order, leaving younger records held — it must
     * not surrender the entire buffer the way a size-limit eviction would.
     *
     * Asserts that only the oldest record is evicted and the two younger records remain.
     */
    @Test
    void evictExpiredOnlyEvictsAgedOutRecordsLeavingYoungerOnesHeld() {
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = engineWithClock(CausalBufferLimit.ofDuration(Duration.ofMillis(200)), clock::get);

        clock.set(0L);
        processRecord(engine, incomingRecord(T2, 0,
                ParsleyClock.empty().observe(T1_ID, 0, 99)));
        clock.set(100L);
        processRecord(engine, incomingRecord(T2, 1,
                ParsleyClock.empty().observe(T1_ID, 0, 99)));
        clock.set(300L);
        processRecord(engine, incomingRecord(T2, 2,
                ParsleyClock.empty().observe(T1_ID, 0, 99)));

        clock.set(250L); // only the record buffered at t=0 has aged past the 200ms duration
        engine.evictExpired();

        assertEquals(2, buffer.size(), "only the oldest record must be evicted; the two younger ones remain held");
        List<Long> remainingOffsets = buffer.entries().stream()
                .map(e -> e.record().offset()).sorted().toList();
        assertEquals(List.of(1L, 2L), remainingOffsets, "the two younger records must still be held");
    }

    /**
     * The frontier listener is invoked before each record is forwarded, receiving the
     * frontier as it stands at the moment of forwarding.
     *
     * Asserts that after one record is forwarded, the last frontier snapshot delivered to
     * the listener matches the engine's current frontier.
     */
    @Test
    void frontierListenerFiresBeforeEachForward() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));

        assertEquals(engine.frontier(), frontiers.get(frontiers.size() - 1),
                "frontier listener must receive the current frontier before each forward");
    }

    /**
     * The metrics callbacks fire at the correct lifecycle points: {@code recordBuffered}
     * fires when a record enters the buffer, {@code recordReleased} fires when it is drained
     * (receiving the count of released records), and {@code reportState} fires alongside both,
     * receiving the buffer's depth after the change.
     *
     * Asserts that {@code recordBuffered} fires on buffer with {@code reportState} reporting
     * depth 1, and {@code recordReleased} fires with count 1 on drain with {@code reportState}
     * reporting depth 0.
     */
    @Test
    void metricsCallbacksFireOnBufferAndRelease() {
        List<Integer> bufferedCounts = new ArrayList<>();
        List<Integer> releasedCounts = new ArrayList<>();
        List<Integer> reportedDepths = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered()             { bufferedCounts.add(1); }
            @Override public void recordReleased(int c)        { releasedCounts.add(c); }
            @Override public void recordEvicted(int c)         {}
            @Override public void recordViolation()             {}
            @Override public void recordDeserializationError()  {}
            @Override public void recordEvictionLimitExceeded() {}
            @Override public void reportState(int depth, OptionalLong oldest) { reportedDepths.add(depth); }
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(100),
                ParsleyClock.empty(), frontiers::add, buffer,
                new MockCandidateIndex(), forwardedIndex, capturing);

        engine.onRecord(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(List.of(1), bufferedCounts, "recordBuffered must fire when a record enters the buffer");
        assertEquals(List.of(1), reportedDepths, "reportState must fire with the new buffer depth");
        assertTrue(releasedCounts.isEmpty(), "recordReleased must not fire while record is buffered");

        engine.onRecord(incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased must fire with the count of drained records");
        assertEquals(List.of(1, 0), reportedDepths, "reportState must report the post-drain buffer depth");
    }

    /**
     * The {@code recordEvicted} metrics callback fires when a record is evicted from the
     * buffer, receiving the count of evicted records.
     *
     * Asserts that {@code recordEvicted} fires with count 1 when the buffer limit is reached.
     */
    @Test
    void metricsCallbackFiresOnEviction() {
        List<Integer> evictedCounts = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered()             {}
            @Override public void recordReleased(int c)        {}
            @Override public void recordEvicted(int c)         { evictedCounts.add(c); }
            @Override public void recordViolation()            {}
            @Override public void recordDeserializationError() {}
            @Override public void recordEvictionLimitExceeded() {}
            @Override public void reportState(int depth, OptionalLong oldest) {}
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferLimit.ofSize(1),
                ParsleyClock.empty(), frontiers::add, buffer,
                new MockCandidateIndex(), forwardedIndex, capturing, CausalAudit.NOOP,
                System::currentTimeMillis, false, false);

        engine.onRecord(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 99)));

        assertEquals(List.of(1), evictedCounts, "recordEvicted must fire with the count of evicted records");
    }

    /**
     * The {@code CausalAudit} callback fires alongside the existing forward/hold behaviour:
     * {@code recordForwarded} for a record whose dependencies are already satisfied, and
     * {@code recordHeld} (with the buffer depth and the unmet gap) for a record that is buffered.
     *
     * Asserts that the satisfied record is reported forwarded and the unsatisfied record is
     * reported held with depth 1 and a gap naming the unmet dependency.
     */
    @Test
    void auditReceivesForwardedAndHeldEvents() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyEngine<String, String> engine = engineWithAudit(CausalBufferLimit.ofSize(100), audit);

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty()));
        assertEquals(List.of(new RecordingCausalAudit.Forwarded("t1", 0, 0L)), audit.forwarded,
                "a trivially-satisfied record must be reported forwarded");

        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 99)));
        assertEquals(1, audit.held.size(), "an unsatisfied record must be reported held");
        RecordingCausalAudit.Held held = audit.held.get(0);
        assertEquals("t2", held.topic(), "held event must carry the record's source topic");
        assertEquals(1, held.bufferDepth(), "held event must carry the buffer depth after admission");
        assertEquals(CausalDependencies.of(ParsleyClock.empty().observe(T1_ID, 0, 99)), held.gap(),
                "held event must carry the unmet dependency as the gap");
    }

    /**
     * The {@code recordReleased} audit callback fires once per record drained from the buffer,
     * carrying the buffer depth immediately after that record's removal.
     *
     * Asserts that releasing the one buffered record reports it with depth 0 after removal.
     */
    @Test
    void auditReceivesReleasedEventOnDrain() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyEngine<String, String> engine = engineWithAudit(CausalBufferLimit.ofSize(100), audit);

        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertTrue(audit.released.isEmpty(), "recordReleased must not fire while the record is held");

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(List.of(new RecordingCausalAudit.Released("t2", 0, 0L, 0)), audit.released,
                "the released record must be reported with the post-removal buffer depth");
    }

    /**
     * The {@code recordViolation} audit callback fires once per record evicted by a
     * {@link CausalBufferLimit}, carrying the dependencies still unmet at the time of eviction.
     *
     * Asserts that the evicted record is reported with a gap naming its unmet dependency.
     */
    @Test
    void auditReceivesViolationEventOnEviction() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        ParsleyEngine<String, String> engine = engineWithAudit(CausalBufferLimit.ofSize(1), audit);

        ParsleyClock unmet = ParsleyClock.empty().observe(T1_ID, 0, 99);
        processRecord(engine, incomingRecord(T2, 0, unmet));

        assertEquals(
                List.of(new RecordingCausalAudit.Violation(
                        "t2", 0, 0L, CausalDependencies.of(unmet.missing(ParsleyClock.empty())))),
                audit.violations, "the evicted record must be reported with its unmet dependency as the gap");
    }

    /**
     * A record whose only dependency is on itself (same topic, partition, and offset) has
     * its self-reference stripped before the admissibility check. After stripping, its
     * effective dependencies are empty, so it is forwarded immediately.
     *
     * Asserts that the record is forwarded without entering the buffer, and the frontier
     * advances through the record.
     */
    @Test
    void forwardImmediatelyWhenOnlySelfDependency() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // Record at T1/0@3 whose dependencies require T1/0@3 — exactly itself.
        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty().observe(T1_ID, 0, 3)));

        assertEquals(1, forwarded.size(), "self-dep is stripped → dependencies empty → forwarded immediately");
        assertEquals(0, buffer.size(), "self-dep record must never enter the buffer");
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 3), engine.frontier(),
                "frontier must advance through the forwarded record");
    }

    /**
     * A record with both a self-referential dependency and a real external dependency has
     * its self-reference stripped. The effective dependencies contain only the real dep,
     * so the record is buffered until that dep is satisfied.
     *
     * Asserts that the record is held while the real dependency is unmet, and released
     * in causal order after the dependency arrives.
     */
    @Test
    void bufferOnRealDependencyWhenMixedWithSelfDependency() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // T2/0@0 has a self-dep on T2_ID/0@0 AND a real dep on T1_ID/0@5.
        // After stripping the self-ref, the effective dependencies are {T1_ID/0@5}: still unsatisfied.
        ParsleyClock mixed = ParsleyClock.empty()
                .observe(T2_ID, 0, 0)
                .observe(T1_ID, 0, 5);
        processRecord(engine, incomingRecord(T2, 0, mixed));
        assertTrue(forwarded.isEmpty(), "real dep on T1@5 must still hold the record in the buffer");
        assertEquals(1, buffer.size(), "record must be in the buffer while real dep is unmet");

        // T1@5 arrives — satisfies the real dep; T2 drains.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty()));
        assertEquals(2, forwarded.size(), "satisfying record and buffered record must both be forwarded");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "buffered record must be released second");
    }

    /**
     * A dependency on a <em>higher</em> offset of the record's own partition is not a
     * self-reference — it names a later record on that partition — so it is honoured, not stripped.
     * But it can never be satisfied by waiting: the contiguous frontier cannot pass this coordinate
     * past the required offset without first passing through the held record's own offset, which is
     * exactly the thing waiting on it. A later record on the same partition can still forward on its
     * own, but does not release the held one. The only way out is eventual eviction.
     *
     * Asserts the record is held while the forward dependency is unmet, that a later same-partition
     * record forwards independently without releasing it, and that it is eventually force-evicted —
     * never naturally released — once the buffer limit requires it.
     */
    @Test
    void forwardSamePartitionDependencyCanOnlyResolveByEviction() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));

        // T1@3 depends on T1@5 — a later record on its own partition (forward dep, not a self-reference).
        processRecord(engine, incomingRecord(T1, 3,
                ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(forwarded.isEmpty(), "forward same-partition dep must hold the record, not be stripped");
        assertEquals(1, buffer.size(), "record must be buffered while the forward dependency is unmet");

        // T1@5 itself arrives — it forwards on its own, but cannot release T1@3: the contiguous
        // frontier cannot reach offset 5 without first passing through offset 3, the very record
        // waiting on it.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty()));
        assertEquals(List.of(5L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "T1@5 forwards on its own, but does not release T1@3");
        assertEquals(1, buffer.size(),
                "T1@3 remains held — a forward same-partition dependency can never be satisfied by waiting");

        // A second held record overflows the size limit, forcing T1@3 out by eviction instead.
        forwarded.clear();
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 99)));

        assertEquals(List.of(3L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "the held record must be force-evicted, never naturally released");
        assertEquals(1, buffer.size(), "buffer holds exactly the size limit after eviction (T2@0 now held)");
    }

    /**
     * A dependency on a <em>lower</em> offset of the record's own partition is satisfiable and
     * honoured. Realistically, by the time a record arrives, an earlier offset on its own partition
     * has already been delivered to {@link ParsleyEngine#onRecord} (Kafka guarantees strictly
     * increasing per-partition delivery) — so the interesting case isn't "waiting for it to arrive",
     * it's "waiting for it to actually be forwarded", when that earlier record is itself held on an
     * unrelated dependency.
     *
     * Asserts the dependent record is held until the earlier record's own (unrelated) dependency is
     * satisfied, releasing the earlier record first and the dependent one second.
     */
    @Test
    void holdRecordUntilBackwardSamePartitionDependencyArrives() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // T1@3 arrives first (natural Kafka order) but is itself held on an unrelated dependency.
        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty().observe(T2_ID, 0, 0)));
        assertTrue(forwarded.isEmpty(), "T1@3 is held on its own, unrelated, unmet dependency");

        // T1@5 depends on T1@3 — an earlier record on its own partition (backward dep, honoured) —
        // but T1@3 hasn't actually been forwarded yet, only buffered, so T1@5 must wait too.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertTrue(forwarded.isEmpty(), "backward same-partition dep must hold T1@5 until T1@3 is actually forwarded");
        assertEquals(2, buffer.size(), "both T1@3 and T1@5 remain held");

        // T2@0 arrives, satisfying T1@3's own dependency — T1@3 releases, which in turn satisfies T1@5.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty()));

        assertEquals(3, forwarded.size(), "T2@0, then T1@3, then T1@5 must all be forwarded");
        assertEquals(0L, forwarded.get(0).offset(), "the satisfying record (T2@0) is forwarded first");
        assertEquals(3L, forwarded.get(1).offset(), "T1@3 releases once its own dependency is met");
        assertEquals(5L, forwarded.get(2).offset(), "T1@5 releases once T1@3 (its backward dependency) is forwarded");
    }

    /**
     * When a topic is recreated (same name and partition, different UUID), a record that
     * depends on an offset under the old incarnation is not satisfied by a record from the
     * new incarnation at the same offset. Only a record from the old incarnation unblocks it.
     *
     * Asserts that the new-incarnation record does not release the buffered record, and that
     * the old-incarnation record does.
     */
    @Test
    void recreatedTopicDoesNotSatisfyDependencyOnOldIncarnation() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        Uuid oldT1 = new Uuid(0L, 1L);
        Uuid newT1 = new Uuid(0L, 2L);   // recreated — different UUID, same name + partition

        // T2 depends on t1-0@5 under the OLD incarnation.
        processRecord(engine, incomingRecordWithId(T2, 0, T2_ID,
                ParsleyClock.empty().observe(oldT1, 0, 5L)));
        assertTrue(forwarded.isEmpty(), "T2 must be buffered: old-t1 dependency unsatisfied");

        // A record from the NEW t1 incarnation at the same offset must NOT unblock T2.
        processRecord(engine, incomingRecordWithId(T1, 5, newT1, ParsleyClock.empty()));
        assertEquals(1, forwarded.size(), "only the new-t1 record must be forwarded; T2 stays buffered");
        assertEquals(T1, tp(forwarded.get(0)), "new-incarnation record must be forwarded");

        // A record from the OLD t1 incarnation arrives — dependency now satisfied.
        processRecord(engine, incomingRecordWithId(T1, 5, oldT1, ParsleyClock.empty()));
        assertEquals(3, forwarded.size(), "old-t1 record forwarded, then buffered T2 released");
        assertEquals(T2, tp(forwarded.get(2)), "T2 must be released after old-t1 arrives");
    }

    /**
     * {@code ParsleyEngine.sizeLimitOf} and {@code durationLimitOf} resolve through a composite
     * {@code ParsleyFirstLimit}, finding the first limit of the requested kind regardless of its
     * position, and returning empty when no limit of that kind is present in the composite.
     *
     * Asserts that the size limit and duration limit are each found within a composite of both
     * kinds, and that a composite missing a kind resolves to {@code Optional.empty()} for it.
     */
    @Test
    void sizeAndDurationLimitOfResolveThroughAFirstLimitComposite() {
        CausalBufferLimit composite = CausalBufferLimit.first(
                CausalBufferLimit.ofSize(5), CausalBufferLimit.ofDuration(Duration.ofSeconds(1)));

        assertEquals(Optional.of(5), ParsleyEngine.sizeLimitOf(composite),
                "sizeLimitOf must find the size limit within the composite");
        assertEquals(Optional.of(Duration.ofSeconds(1)), ParsleyEngine.durationLimitOf(composite),
                "durationLimitOf must find the duration limit within the composite");

        CausalBufferLimit sizeOnly = CausalBufferLimit.first(CausalBufferLimit.ofSize(5));
        assertEquals(Optional.empty(), ParsleyEngine.durationLimitOf(sizeOnly),
                "durationLimitOf must resolve to empty when the composite carries no duration limit");

        CausalBufferLimit durationOnly = CausalBufferLimit.first(CausalBufferLimit.ofDuration(Duration.ofSeconds(1)));
        assertEquals(Optional.empty(), ParsleyEngine.sizeLimitOf(durationOnly),
                "sizeLimitOf must resolve to empty when the composite carries no size limit");
    }

    /**
     * The frontier is a contiguous watermark, not a running max: a later-offset record on a
     * partition may forward immediately even while an earlier-offset record on the same partition
     * remains held, but doing so must never advance the frontier past the held record's offset —
     * otherwise a third record depending on exactly the held offset would be released on
     * bookkeeping alone, without that record's effects ever having gone out.
     *
     * Asserts that forwarding the later offset leaves the frontier exactly one below the held
     * record, and that neither the held record nor a record depending on it is released.
     */
    @Test
    void laterSamePartitionRecordForwardsWithoutLeapfroggingAnEarlierHeldOne() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // T1@5 is held on an unrelated dependency requiring real progress on T2 (not just T2's
        // baseline — a dependency on T2's very first observed offset would be trivially satisfied
        // the moment any record on T2 is seen at all, which isn't the scenario under test here).
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 10)));
        assertTrue(forwarded.isEmpty(), "T1@5 must be held while its dependency is unmet");

        // A third record depends on exactly T1@5 having been forwarded.
        processRecord(engine, incomingRecord(T2, 1, ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(forwarded.isEmpty(), "T2@1 must be held: T1@5 has not actually been forwarded yet");

        // T1@6 has independently-satisfied (empty) deps and forwards immediately.
        processRecord(engine, incomingRecord(T1, 6, ParsleyClock.empty()));

        assertEquals(List.of(6L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "T1@6 forwards on its own");
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "the frontier must stall one below T1@5, never leapfrogging past the still-held record");
        assertEquals(2, buffer.size(),
                "T1@5 and T2@1 (depending on it) must both remain held — T1@6 forwarding must not release either");
    }

    /**
     * Once the held record at the bottom of a gap is released, the frontier jumps forward in a
     * single step to absorb every already-forwarded record above it — it does not need those later
     * records to arrive again, since they are already sitting in the forwarded index.
     *
     * Asserts that releasing T1@5 advances the frontier all the way to T1@8 (which already
     * forwarded while T1@5 was held) in one step, and that a record depending on T1@8 releases too.
     */
    @Test
    void releasingTheHeldRecordCatchesUpThroughEverythingAlreadyForwardedAboveIt() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // T1@5 is held on an unrelated dependency.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 0)));

        // T1@6, T1@7, T1@8 each forward immediately (independently-satisfied), piling up above the gap.
        processRecord(engine, incomingRecord(T1, 6, ParsleyClock.empty()));
        processRecord(engine, incomingRecord(T1, 7, ParsleyClock.empty()));
        processRecord(engine, incomingRecord(T1, 8, ParsleyClock.empty()));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0), "frontier stalls below T1@5 the whole time");
        forwarded.clear();

        // T1@9 depends on T1@8 (its own immediate predecessor) — held, since the frontier hasn't
        // actually reached 8 yet (it is stuck behind the still-held T1@5).
        processRecord(engine, incomingRecord(T1, 9, ParsleyClock.empty().observe(T1_ID, 0, 8)));
        assertTrue(forwarded.isEmpty(), "T1@9 must be held: the frontier hasn't reached 8 yet");
        assertEquals(2, buffer.size(), "T1@5 and T1@9 both remain held");
        forwarded.clear();

        // T2@0 satisfies T1@5's own dependency — releasing it closes the gap, and the frontier
        // should catch up through 6, 7, and 8 in one step, releasing T1@9 too.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty()));

        assertEquals(9L, engine.frontier().offsetFor(T1_ID, 0),
                "the frontier must jump straight to 9, absorbing every already-forwarded record above the gap");
        assertEquals(List.of(0L, 5L, 9L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "T2@0 forwards, releasing T1@5, which in turn releases T1@9");
        assertTrue(forwardedIndex.forwardedAfter(T1_ID, 0, 4).isEmpty(),
                "every absorbed offset (5-9) must be pruned from the forwarded index, not left to "
                        + "accumulate — it is no longer needed once folded into the frontier");
    }

    /**
     * The same catch-up jump happens when the gap-closing record resolves by eviction rather than a
     * natural release — eviction and release feed the exact same contiguous-clock bookkeeping, with
     * no special-casing between them.
     *
     * Asserts that evicting T1@5 (forced, out of causal order) advances the frontier through the
     * already-forwarded T1@6/7/8 in one step, exactly as a natural release would.
     */
    @Test
    void evictingTheHeldRecordCatchesUpThroughEverythingAlreadyForwardedAboveIt() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));

        // T1@5 is held on a dependency that will never be satisfied in this test.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 99)));

        // T1@6, T1@7, T1@8 each forward immediately, piling up above the gap.
        processRecord(engine, incomingRecord(T1, 6, ParsleyClock.empty()));
        processRecord(engine, incomingRecord(T1, 7, ParsleyClock.empty()));
        processRecord(engine, incomingRecord(T1, 8, ParsleyClock.empty()));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0), "frontier stalls below T1@5 the whole time");
        forwarded.clear();

        // A second held record overflows the size-2 buffer, force-evicting T1@5 (the oldest).
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T2_ID, 0, 99)));

        assertEquals(List.of(5L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "T1@5 must be force-evicted");
        assertEquals(8L, engine.frontier().offsetFor(T1_ID, 0),
                "evicting T1@5 must catch the frontier up through everything already forwarded above it");
    }

    /**
     * Evicting a held record must trigger the same drain cascade a natural release does: another
     * record depending on exactly the evicted record's coordinate is released as a direct result of
     * the eviction itself, not only on some later, unrelated admission.
     *
     * Asserts that evicting T2@0 immediately releases T1@1, which depends on exactly T2@0's
     * coordinate — in the same call that performed the eviction.
     */
    @Test
    void evictionTriggersTheSameDrainCascadeAsANaturalRelease() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(2));

        // T2@0 is held (its dependency on T1@99 will never arrive in this test).
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 99)));
        assertTrue(forwarded.isEmpty(), "T2@0 must be held while its dependency is unmet");

        // T1@1 depends on exactly T2@0's own coordinate, and its own admission overflows the
        // size-2 buffer, force-evicting T2@0. T1@1 must be released as a direct result of that very
        // eviction — proving evictSequences now drives the same drain cascade a natural release does.
        processRecord(engine, incomingRecord(T1, 1, ParsleyClock.empty().observe(T2_ID, 0, 0)));

        assertEquals(List.of(0L, 1L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "evicting T2@0 must immediately release T1@1, which depended on exactly its coordinate");
        assertEquals(0, buffer.size(), "both records must have left the buffer");
    }

    /**
     * The forwarded index is itself durable (restored from its own changelog, like the buffer and
     * candidate index already are), so a restart never loses track of offsets forwarded ahead of a
     * still-open gap — simulated here by handing a fresh engine instance the same forwarded-index
     * and buffer-store contents an earlier instance left behind, without persisting any separate
     * "ceiling" value.
     *
     * Asserts that resolving the gap on the new instance reaches the same final frontier as
     * resolving it without a restart in between.
     */
    @Test
    void forwardedIndexSurvivesARestartSoTheCatchUpJumpStillWorks() {
        MockForwardedIndex sharedForwardedIndex = new MockForwardedIndex();
        MockBufferStore<String, String> sharedBuffer = new MockBufferStore<>();

        ParsleyEngine<String, String> first = new ParsleyEngine<>(CausalBufferLimit.ofSize(100),
                ParsleyClock.empty(), f -> {}, sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        // T1@5 is held; T1@6, T1@7, T1@8 each forward immediately, piling up above the gap.
        first.onRecord(incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 0)));
        first.onRecord(incomingRecord(T1, 6, ParsleyClock.empty()));
        first.onRecord(incomingRecord(T1, 7, ParsleyClock.empty()));
        first.onRecord(incomingRecord(T1, 8, ParsleyClock.empty()));
        ParsleyClock persistedFrontier = first.frontier();
        assertEquals(4L, persistedFrontier.offsetFor(T1_ID, 0),
                "frontier persisted at the gap, as it would be before a crash");

        // Simulate a restart: a brand-new engine instance, seeded with the persisted frontier and a
        // buffer restored from the changelog (still holding T1@5), and the SAME forwarded-index
        // contents (standing in for "restored from its own changelog") — no separate "ceiling"
        // value is needed; the forwarded index alone remembers that 6, 7, and 8 already went out.
        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(CausalBufferLimit.ofSize(100),
                persistedFrontier, f -> {}, sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> released =
                restarted.onRecord(incomingRecord(T2, 0, ParsleyClock.empty()));

        assertEquals(8L, restarted.frontier().offsetFor(T1_ID, 0),
                "the restarted instance must still catch up through 6, 7, and 8 via the surviving forwarded index");
        assertEquals(List.of(0L, 5L), released.stream().map(ParsleyMessage::offset).toList(),
                "T2@0 forwards and releases T1@5 on the restarted instance");
    }

    /**
     * Establishing the baseline for a coordinate this engine has never observed before can itself
     * release an already-buffered record — before the very record that triggered the baseline seed
     * is even dispositioned by its own dominates check.
     *
     * Asserts that T2@0, depending on a coordinate the engine has never seen, is released as a
     * direct effect of T1@5 establishing that coordinate's baseline — and is forwarded ahead of
     * T1@5 itself in the returned order.
     */
    @Test
    void establishingTheBaselineForAFirstSeenCoordinateCanItselfReleaseAWaitingRecord() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferLimit.ofSize(100));

        // T2@0 depends on T1_ID/0@4 — a coordinate this engine has never observed at all yet.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 4)));
        assertTrue(forwarded.isEmpty(), "T2@0 must be held: T1/0 has never been observed");

        // T1@5 is the very first record this engine ever sees on T1/0. Establishing its baseline
        // (frontier = 4) is itself enough to satisfy T2@0's dependency — before T1@5 is even
        // dispositioned by its own dominates check.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty()));

        assertEquals(List.of(0L, 5L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "establishing T1's baseline must release T2@0 before T1@5 itself forwards");
        assertEquals(0, buffer.size(), "both records must have left the buffer");
    }

    /**
     * The baseline seed must never re-fire once the persisted frontier already reflects real
     * progress on a coordinate — even progress as low as offset 0 exactly. A restarted engine
     * instance, with a fresh {@code seenCoordinates} set, sees every coordinate "for the first
     * time" in this process, but must defer entirely to a restored frontier value rather than guess
     * a new baseline from whatever offset it happens to see next.
     *
     * Asserts that with a restored frontier already at exactly 0 for a coordinate, the first record
     * this instance sees on it at a much later offset does not corrupt the frontier: it correctly
     * stalls at 0 (a real, unaccounted-for gap exists from 1 through 9), rather than treating that
     * gap as moot just because {@code seenCoordinates} happened to be fresh.
     */
    @Test
    void baselineSeedNeverRefiresWhenTheRestoredFrontierAlreadyHasRealProgress() {
        ParsleyClock restoredFrontier = ParsleyClock.empty().observe(T1_ID, 0, 0);
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(CausalBufferLimit.ofSize(100),
                restoredFrontier, frontiers::add, buffer, new MockCandidateIndex(),
                forwardedIndex, ParsleyMetrics.NOOP);

        // The first record this (restarted) instance ever sees on T1/0 is offset 10 — far above
        // the restored frontier of 0. There is a real, unaccounted-for gap from 1 through 9.
        processRecord(engine, incomingRecord(T1, 10, ParsleyClock.empty()));

        assertEquals(0L, engine.frontier().offsetFor(T1_ID, 0),
                "the frontier must stay at the restored value (0); it must not be corrupted into "
                        + "treating the unaccounted-for gap from 1-9 as moot just because "
                        + "seenCoordinates was fresh");
    }

    // --- helpers --------------------------------------------------------------------------------

    // failOnEvictionLimit=false (continue) below: these helpers back tests that assert the
    // evict-and-forward-out-of-order outcome, so they opt out of the new fail-fast default
    // explicitly rather than via the convenience constructors (which now default to fail-fast,
    // matching ParsleyConfig's production default).

    private ParsleyEngine<String, String> engineWith(CausalBufferLimit limit) {
        return new ParsleyEngine<>(limit, ParsleyClock.empty(), frontiers::add, buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP, CausalAudit.NOOP,
                System::currentTimeMillis, false, false);
    }

    private ParsleyEngine<String, String> engineWithClock(CausalBufferLimit limit,
                                                           java.util.function.LongSupplier clock) {
        return new ParsleyEngine<>(limit, ParsleyClock.empty(), frontiers::add, buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP, CausalAudit.NOOP, clock, false, false);
    }

    private ParsleyEngine<String, String> engineWithAudit(CausalBufferLimit limit, CausalAudit audit) {
        return new ParsleyEngine<>(limit, ParsleyClock.empty(), frontiers::add, buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP, audit,
                System::currentTimeMillis, false, false);
    }

    private void processRecord(ParsleyEngine<String, String> engine, ParsleyMessage<String, String> message) {
        forwarded.addAll(engine.onRecord(message));
    }

    private static TopicPartition tp(ParsleyMessage<String, String> m) {
        return new TopicPartition(m.topic(), m.partition());
    }

    private static Uuid idFor(TopicPartition tp) {
        if (T1.topic().equals(tp.topic())) return T1_ID;
        if (T2.topic().equals(tp.topic())) return T2_ID;
        throw new IllegalArgumentException("no known id for topic " + tp.topic());
    }

    private static ParsleyMessage<String, String> incomingRecord(TopicPartition tp, long offset,
                                                                  ParsleyClock deps) {
        return incomingRecordWithId(tp, offset, idFor(tp), deps);
    }

    private static ParsleyMessage<String, String> incomingRecordWithId(TopicPartition tp, long offset,
                                                                        Uuid topicId,
                                                                        ParsleyClock deps) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), deps == null ? ParsleyClock.empty() : deps);
    }
}
