package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyEngineTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = CausalPosition.deriveUuid(T1.topic());
    private static final Uuid T2_ID = CausalPosition.deriveUuid(T2.topic());

    private final List<ParsleyRecord<String, String>> forwarded = new ArrayList<>();
    private final List<CausalViolation> violations = new ArrayList<>();
    private final List<CausalFrontier> frontiers = new ArrayList<>();
    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    // --- test methods ---------------------------------------------------------------------------

    /**
     * A record whose causal dependencies are already satisfied by the current frontier
     * is forwarded to the downstream processor immediately, without entering the buffer.
     *
     * <p>The frontier is advanced to reflect the record's source coordinate on forward.
     *
     * Asserts that exactly one record is forwarded and the frontier reflects the admitted
     * record's source offset.
     */
    @Test
    void satisfiedRecordForwardsImmediatelyAndAdvancesFrontier() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.empty()));

        assertEquals(1, forwarded.size(), "satisfied record must be forwarded immediately");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 3)), engine.frontier(),
                "frontier must advance to the forwarded record's source offset");
    }

    /**
     * A record whose dependencies are not yet satisfied is held in the buffer until the
     * frontier advances to cover the required coordinate.
     *
     * <p>When the dependency arrives, the engine releases the buffered record after the
     * satisfying record and advances the frontier through both.
     *
     * Asserts that the buffered record is released in causal order after the satisfying record,
     * and that the frontier reflects both records' source offsets.
     */
    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        CausalDependencies deps = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build();
        processRecord(engine, incomingRecord(T2, 0, deps));
        assertTrue(forwarded.isEmpty(), "unsatisfied record must be held in the buffer");

        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded after the dependency arrives");
        assertEquals(T1, forwarded.get(0).sourcePartition(), "satisfying record must be forwarded first");
        assertEquals(T2, forwarded.get(1).sourcePartition(), "buffered record must be released second");
        assertEquals(
                CausalFrontier.empty()
                        .observe(new CausalPosition(T1_ID, 0, 3))
                        .observe(new CausalPosition(T2_ID, 0, 0)),
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
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        engine.onRecord(incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()));
        assertEquals(1, buffer.size(), "unsatisfied record must be held in the buffer");

        engine.onRecord(incomingRecord(T1, 3, CausalDependencies.empty()));
        assertEquals(0, buffer.size(), "drained record must be removed from the buffer");
    }

    /**
     * When the buffer is at its size limit and a new unsatisfied record arrives, the engine
     * applies the configured eviction policy. Under the drop policy, the record is discarded
     * and removed from the buffer immediately.
     *
     * Asserts that the buffer is empty after the eviction (the record never entered the buffer).
     */
    @Test
    void strictEvictionRemovesTheRecordFromTheBuffer() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        engine.onRecord(incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertEquals(0, buffer.size(), "dropped record must never reside in the buffer");
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
        buffer.add(incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()), 0L);
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));
        assertEquals(CausalFrontier.empty(), engine.frontier(), "pre-buffered records must not advance the frontier on construction");
        assertTrue(frontiers.isEmpty(), "pre-buffered records must not fire the frontier listener on construction");

        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size(), "satisfying record and pre-buffered record must both be forwarded");
        assertEquals(T1, forwarded.get(0).sourcePartition(), "satisfying record must be forwarded first");
        assertEquals(T2, forwarded.get(1).sourcePartition(), "pre-buffered record must be released second");
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
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)));

        CausalDependencies.Builder bigBuilder = CausalDependencies.builder();
        for (int p = 0; p < 200; p++) {
            bigBuilder.require(new CausalPosition(CausalPosition.deriveUuid("ghost"), p, 1_000 + p));
        }
        processRecord(engine, incomingRecord(T2, 0, bigBuilder.build()));

        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T2_ID, 0, 0)), engine.frontier(),
                "frontier must contain only the record's own source coordinate");
        assertEquals(1, engine.frontier().positions().size(),
                "inbound dependency entries must not widen the frontier");
    }

    /**
     * A record with no causal-dependencies header is a protocol violation. Under the
     * forward-unsafe policy, the engine forwards it immediately and reports the violation.
     *
     * Asserts that the record is forwarded, the violation list contains one MISSING_HEADER
     * entry, and the frontier advances through the record's source coordinate.
     */
    @Test
    void forwardRecordWithViolationWhenHeaderMissingUnderForwardUnsafePolicy() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        processRecord(engine, incomingRecord(T1, 0, null));

        assertEquals(1, forwarded.size(), "forward-unsafe policy must forward the missing-header record");
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), reasons(),
                "a MISSING_HEADER violation must be reported");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 0)), engine.frontier(),
                "frontier must advance even for violation records");
    }

    /**
     * A record with no causal-dependencies header is a protocol violation. Under the drop
     * policy, the engine silently discards it and reports the violation, but still advances
     * the frontier so downstream records are not held indefinitely.
     *
     * Asserts that nothing is forwarded, a MISSING_HEADER violation is reported, and the
     * frontier reflects the dropped record's source offset.
     */
    @Test
    void discardRecordWhenHeaderMissingUnderDropPolicy() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        processRecord(engine, incomingRecord(T1, 0, null));

        assertTrue(forwarded.isEmpty(), "drop policy must not forward a missing-header record");
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), reasons(),
                "a MISSING_HEADER violation must be reported");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 0)), engine.frontier(),
                "frontier must still advance so downstream records are not blocked");
    }

    /**
     * A record with no causal-dependencies header is a protocol violation. Under the
     * dead-letter policy, the engine routes it to the configured sink and reports the
     * violation.
     *
     * Asserts that nothing is forwarded downstream, one record reaches the dead-letter sink,
     * and the sink record carries a MISSING_HEADER reason header.
     */
    @Test
    void routeToDeadLetterWhenHeaderMissingUnderDeadLetterPolicy() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        ParsleyEngine<String, String> engine = engineWith(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(100)), deadLettered::add);

        processRecord(engine, incomingRecord(T1, 0, null));

        assertTrue(forwarded.isEmpty(), "dead-letter policy must not forward a missing-header record downstream");
        assertEquals(1, deadLettered.size(), "missing-header record must reach the dead-letter sink");
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), reasons(),
                "a MISSING_HEADER violation must be reported");
        assertEquals("MISSING_HEADER",
                new String(deadLettered.get(0).toConsumerRecord()
                        .headers().lastHeader(CausalViolation.DLQ_REASON_HEADER).value(), UTF_8),
                "dead-letter record must carry the MISSING_HEADER reason header");
    }

    /**
     * A record whose causal-dependencies header cannot be parsed is a protocol violation.
     * Under the forward-unsafe policy, the engine forwards it immediately and reports the
     * violation.
     *
     * Asserts that the record is forwarded and an UNRESOLVABLE_DEPENDENCIES violation is
     * reported.
     */
    @Test
    void forwardRecordWithViolationWhenDependenciesUnresolvableUnderForwardUnsafePolicy() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> garbage = garbledRecord(T1, 0);
        processRecord(engine, garbage);

        assertEquals(1, forwarded.size(), "forward-unsafe policy must forward the unresolvable-dependencies record");
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES), reasons(),
                "an UNRESOLVABLE_DEPENDENCIES violation must be reported");
    }

    /**
     * A record whose causal-dependencies header cannot be parsed is a protocol violation.
     * Under the drop policy, the engine discards it, reports the violation, and advances
     * the frontier.
     *
     * Asserts that nothing is forwarded, an UNRESOLVABLE_DEPENDENCIES violation is reported,
     * and the frontier reflects the dropped record's source offset.
     */
    @Test
    void discardRecordWhenDependenciesUnresolvableUnderDropPolicy() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> garbage = garbledRecord(T1, 0);
        processRecord(engine, garbage);

        assertTrue(forwarded.isEmpty(), "drop policy must not forward an unresolvable-dependencies record");
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES), reasons(),
                "an UNRESOLVABLE_DEPENDENCIES violation must be reported");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 0)), engine.frontier(),
                "frontier must still advance for the dropped record");
    }

    /**
     * When the buffer reaches its size limit and a new unsatisfied record arrives, the engine
     * evicts only the oldest buffered record needed to bring the buffer back under the limit,
     * leaving the rest held. Under the forward-unsafe policy, the evicted record is forwarded
     * with a violation reported.
     *
     * Asserts that only the oldest record is forwarded, exactly one violation is reported, the
     * frontier advances only through the evicted record, and the younger record remains buffered.
     */
    @Test
    void sizeLimitEvictsOnlyTheOldestRecordUnderForwardUnsafePolicy() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        CausalDependencies unmet = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build();

        processRecord(engine, incomingRecord(T2, 0, unmet));
        assertTrue(forwarded.isEmpty(), "first record must be buffered while limit not yet reached");
        processRecord(engine, incomingRecord(T2, 1, unmet)); // hits size 2 → evict only the oldest

        assertEquals(1, forwarded.size(), "only the oldest evicted record must be forwarded under forward-unsafe policy");
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons(),
                "exactly one LIMIT_REACHED violation must be reported");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T2_ID, 0, 0)), engine.frontier(),
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
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        CausalDependencies unmet = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build();

        processRecord(engine, incomingRecord(T2, 0, unmet)); // A: depth 1, no eviction
        assertTrue(forwarded.isEmpty(), "no eviction while under the limit");

        processRecord(engine, incomingRecord(T2, 1, unmet)); // B: depth 2 → evict A
        assertEquals(List.of(0L), forwarded.stream().map(ParsleyRecord::sourceOffset).toList(),
                "the first overflow must evict only the oldest record (A)");
        assertEquals(1, buffer.size(), "buffer must hold exactly the size limit after eviction");

        processRecord(engine, incomingRecord(T2, 2, unmet)); // C: depth 2 → evict B
        assertEquals(List.of(0L, 1L), forwarded.stream().map(ParsleyRecord::sourceOffset).toList(),
                "the second overflow must evict only the next-oldest record (B)");
        assertEquals(1, buffer.size(), "buffer must never exceed the configured size limit");
        assertEquals(2, reasons().size(), "exactly one LIMIT_REACHED violation must be reported per overflow");
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
        CausalDependencies unmet = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build();
        buffer.add(incomingRecord(T2, 0, unmet), 0L);
        buffer.add(incomingRecord(T2, 1, unmet), 1L);
        buffer.add(incomingRecord(T2, 2, unmet), 2L);

        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        forwarded.addAll(engine.evictOverflow());

        assertEquals(List.of(0L, 1L), forwarded.stream().map(ParsleyRecord::sourceOffset).toList(),
                "the two oldest excess records must be evicted, oldest-first");
        assertEquals(2, reasons().size(), "one LIMIT_REACHED violation must be reported per evicted record");
        assertEquals(1, buffer.size(), "the buffer must be trimmed down to exactly the new limit");
    }

    /**
     * {@code evictOverflow()} must be a safe no-op when called on a buffer that is already at or
     * under the configured limit — the call site added to {@code ParsleyProcessor.init()} invokes
     * it unconditionally on every restart, including the common case where nothing needs trimming.
     */
    @Test
    void evictOverflowIsANoOpWhenBufferIsNotOverTheLimit() {
        CausalDependencies unmet = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build();
        buffer.add(incomingRecord(T2, 0, unmet), 0L);

        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        List<ParsleyRecord<String, String>> result = engine.evictOverflow();

        assertTrue(result.isEmpty(), "no eviction must occur while the buffer is within the limit");
        assertTrue(violations.isEmpty(), "no violation must be reported when nothing is evicted");
        assertEquals(1, buffer.size(), "the buffer must be left untouched");
    }

    /**
     * When the buffer is at its size limit and a new unsatisfied record arrives, the engine
     * evicts the oldest buffered record. Under the drop policy, the evicted record is
     * discarded and a violation is reported, but the frontier does not advance.
     *
     * Asserts that nothing is forwarded, one LIMIT_REACHED violation is reported, the
     * violation carries the expected frontier and gap, and the frontier remains unchanged.
     */
    @Test
    void dropPolicyDiscardsOnEvictionAndReportsTheGap() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        processRecord(engine, incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertTrue(forwarded.isEmpty(), "drop policy must not forward evicted records");
        assertEquals(1, violations.size(), "one LIMIT_REACHED violation must be reported");
        CausalViolation violation = violations.get(0);
        assertEquals(CausalViolationReason.LIMIT_REACHED, violation.reason(),
                "violation reason must be LIMIT_REACHED");
        assertEquals(CausalFrontier.empty(), violation.frontier(),
                "violation must carry the frontier at eviction time");
        assertEquals(CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build(), violation.required(),
                "violation must carry the evicted record's required dependencies");
        assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("t1"), 0, 100L)),
                violation.gap(), "gap must reflect the shortfall: required 99, frontier at -1 → need 100");
    }

    /**
     * Under the dead-letter policy, records evicted from the buffer are routed to the
     * configured dead-letter sink rather than forwarded downstream.
     *
     * Asserts that nothing is forwarded, the evicted record reaches the dead-letter sink,
     * and a LIMIT_REACHED violation is reported.
     */
    @Test
    void deadLetterPolicyRoutesEvictedRecordsToSink() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        ParsleyEngine<String, String> engine = engineWith(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1)), deadLettered::add);

        processRecord(engine, incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertTrue(forwarded.isEmpty(), "dead-letter policy must not forward evicted records downstream");
        assertEquals(1, deadLettered.size(), "evicted record must reach the dead-letter sink");
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons(),
                "a LIMIT_REACHED violation must be reported");
    }

    /**
     * A record routed to the dead-letter sink carries three causal-context headers:
     * the reason code, the required dependencies at the time of eviction, and the gap
     * between the required and satisfied state.
     *
     * Asserts that all three headers are present and decode to the expected values.
     */
    @Test
    void deadLetteredRecordCarriesCausalContextHeaders() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        CausalDependencies required = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 5L)).build();

        ParsleyEngine<String, String> engine = engineWith(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1)), deadLettered::add);
        processRecord(engine, incomingRecord(T2, 0, required));

        assertEquals(1, deadLettered.size(), "evicted record must reach the dead-letter sink");
        Headers headers = deadLettered.get(0).toConsumerRecord().headers();

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_REASON_HEADER),
                "dead-letter record must carry the reason header");
        assertEquals("LIMIT_REACHED",
                new String(headers.lastHeader(CausalViolation.DLQ_REASON_HEADER).value(), UTF_8),
                "reason header must encode LIMIT_REACHED");

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_REQUIRED_DEPENDENCIES_HEADER),
                "dead-letter record must carry the required-dependencies header");
        assertEquals(required,
                CausalDependencies.fromBytes(headers.lastHeader(CausalViolation.DLQ_REQUIRED_DEPENDENCIES_HEADER).value()),
                "required-dependencies header must round-trip the original dependencies");

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_GAP_HEADER),
                "dead-letter record must carry the gap header");
        CausalDependencies decodedGap =
                CausalDependencies.fromBytes(headers.lastHeader(CausalViolation.DLQ_GAP_HEADER).value());
        assertTrue(decodedGap.dependencies().stream()
                        .anyMatch(p -> p.topicId().equals(CausalPosition.deriveUuid("t1")) && p.partition() == 0),
                "gap header must cover the unsatisfied partition");
    }

    /**
     * When the buffer limit is duration-based, the engine exposes the configured eviction
     * interval so callers can schedule punctuators.
     *
     * Asserts that {@code evictionInterval()} returns the configured duration.
     */
    @Test
    void durationPolicyExposesEvictionInterval() {
        ParsleyEngine<String, String> engine =
                engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))));
        assertEquals(Duration.ofSeconds(5), engine.evictionInterval().orElseThrow(),
                "eviction interval must match the configured duration");
    }

    /**
     * {@code evictExpired()} must leave a buffered record alone if it hasn't aged past the
     * configured duration yet, even when the punctuator fires.
     *
     * Asserts that the record remains in the buffer and no violation is reported.
     */
    @Test
    void evictExpiredLeavesRecordsYoungerThanTheDurationInTheBuffer() {
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = engineWithClock(
                CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofMillis(200))), clock::get);

        processRecord(engine, incomingRecord(T2, 0,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));
        clock.set(150L);

        List<ParsleyRecord<String, String>> forwardedOnEvict = engine.evictExpired();

        assertTrue(forwardedOnEvict.isEmpty(), "a record younger than the duration must not be evicted");
        assertEquals(1, buffer.size(), "the record must remain held in the buffer");
        assertTrue(violations.isEmpty(), "no violation must be reported for a record that hasn't aged out");
    }

    /**
     * {@code evictExpired()} evicts a buffered record once it has aged past the configured
     * duration, reporting a {@link CausalViolationReason#LIMIT_REACHED} violation.
     *
     * Asserts that the record is removed from the buffer and a violation is reported.
     */
    @Test
    void evictExpiredEvictsRecordsOlderThanTheDuration() {
        AtomicLong clock = new AtomicLong(0L);
        ParsleyEngine<String, String> engine = engineWithClock(
                CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofMillis(200))), clock::get);

        processRecord(engine, incomingRecord(T2, 0,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));
        clock.set(250L);

        engine.evictExpired();

        assertEquals(0, buffer.size(), "a record older than the duration must be evicted");
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons(),
                "evicting an aged-out record must report a LIMIT_REACHED violation");
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
        ParsleyEngine<String, String> engine = engineWithClock(
                CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofMillis(200))), clock::get);

        clock.set(0L);
        processRecord(engine, incomingRecord(T2, 0,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));
        clock.set(100L);
        processRecord(engine, incomingRecord(T2, 1,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));
        clock.set(300L);
        processRecord(engine, incomingRecord(T2, 2,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));

        clock.set(250L); // only the record buffered at t=0 has aged past the 200ms duration
        engine.evictExpired();

        assertEquals(2, buffer.size(), "only the oldest record must be evicted; the two younger ones remain held");
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons(),
                "exactly one violation must be reported, for the single aged-out record");
        List<Long> remainingOffsets = buffer.entries().stream()
                .map(e -> e.record().sourceOffset()).sorted().toList();
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
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.empty()));

        assertEquals(engine.frontier(), frontiers.get(frontiers.size() - 1),
                "frontier listener must receive the current frontier before each forward");
    }

    /**
     * The metrics callbacks fire at the correct lifecycle points: {@code recordBuffered}
     * fires when a record enters the buffer, and {@code recordReleased} fires when it is
     * drained, receiving the count of released records and the post-drain buffer depth.
     *
     * Asserts that {@code recordBuffered} fires with depth 1 on buffer, and
     * {@code recordReleased} fires with count 1 and depth 0 on drain.
     */
    @Test
    void metricsCallbacksFireOnBufferAndRelease() {
        List<Integer> bufferedDepths = new ArrayList<>();
        List<Integer> releasedCounts = new ArrayList<>();
        List<Integer> releasedDepths = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered(int depth)        { bufferedDepths.add(depth); }
            @Override public void recordReleased(int c, int depth) { releasedCounts.add(c); releasedDepths.add(depth); }
            @Override public void recordEvicted(int c)             {}
            @Override public void recordViolation()                {}
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)),
                violations::add, CausalFrontier.empty(), null, frontiers::add, buffer,
                new MockWaitIndex(), capturing);

        engine.onRecord(incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()));
        assertEquals(List.of(1), bufferedDepths, "recordBuffered must fire with the new buffer depth");
        assertTrue(releasedCounts.isEmpty(), "recordReleased must not fire while record is buffered");

        engine.onRecord(incomingRecord(T1, 3, CausalDependencies.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased must fire with the count of drained records");
        assertEquals(List.of(0), releasedDepths, "recordReleased must report the post-drain buffer depth");
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
            @Override public void recordBuffered(int d)        {}
            @Override public void recordReleased(int c, int d) {}
            @Override public void recordEvicted(int c)         { evictedCounts.add(c); }
            @Override public void recordViolation()            {}
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)),
                violations::add, CausalFrontier.empty(), null, frontiers::add, buffer,
                new MockWaitIndex(), capturing);

        engine.onRecord(incomingRecord(T2, 0, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertEquals(List.of(1), evictedCounts, "recordEvicted must fire with the count of evicted records");
    }

    /**
     * A record whose only dependency is on itself (same topic, partition, and offset) has
     * its self-reference stripped before the admissibility check. After stripping, its
     * effective dependencies are empty, so it is forwarded immediately.
     *
     * Asserts that the record is forwarded without entering the buffer, no violation is
     * reported, and the frontier advances through the record.
     */
    @Test
    void forwardImmediatelyWhenOnlySelfDependency() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // Record at T1/0@3 whose dependencies require T1/0@3 — exactly itself.
        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()));

        assertEquals(1, forwarded.size(), "self-dep is stripped → dependencies empty → forwarded immediately");
        assertTrue(violations.isEmpty(), "self-reference stripping must not produce a violation");
        assertEquals(0, buffer.size(), "self-dep record must never enter the buffer");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 3)), engine.frontier(),
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
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // T2/0@0 has a self-dep on T2_ID/0@0 AND a real dep on T1_ID/0@5.
        // After stripping the self-ref, the effective dependencies are {T1_ID/0@5}: still unsatisfied.
        CausalDependencies mixed = CausalDependencies.builder()
                .require(new CausalPosition(T2_ID, 0, 0))
                .require(new CausalPosition(T1_ID, 0, 5))
                .build();
        processRecord(engine, incomingRecord(T2, 0, mixed));
        assertTrue(forwarded.isEmpty(), "real dep on T1@5 must still hold the record in the buffer");
        assertEquals(1, buffer.size(), "record must be in the buffer while real dep is unmet");

        // T1@5 arrives — satisfies the real dep; T2 drains.
        processRecord(engine, incomingRecord(T1, 5, CausalDependencies.empty()));
        assertEquals(2, forwarded.size(), "satisfying record and buffered record must both be forwarded");
        assertEquals(T1, forwarded.get(0).sourcePartition(), "satisfying record must be forwarded first");
        assertEquals(T2, forwarded.get(1).sourcePartition(), "buffered record must be released second");
    }

    /**
     * A dependency on a <em>higher</em> offset of the record's own partition is not a
     * self-reference — it names a later record on that partition — so it is honoured, not stripped:
     * the record waits until that later offset arrives and the frontier reaches it.
     *
     * Asserts the record is held while the forward dependency is unmet, then released after the
     * later same-partition record advances the frontier.
     */
    @Test
    void holdRecordUntilForwardSamePartitionDependencyArrives() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // T1@3 depends on T1@5 — a later record on its own partition (forward dep, not a self-reference).
        processRecord(engine, incomingRecord(T1, 3,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 5)).build()));
        assertTrue(forwarded.isEmpty(), "forward same-partition dep must hold the record, not be stripped");
        assertEquals(1, buffer.size(), "record must be buffered while the forward dependency is unmet");

        // T1@5 arrives, advancing the frontier to 5 and satisfying the forward dependency.
        processRecord(engine, incomingRecord(T1, 5, CausalDependencies.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded once the forward dependency arrives");
        assertEquals(5L, forwarded.get(0).sourceOffset(), "the satisfying later record (offset 5) is forwarded first");
        assertEquals(3L, forwarded.get(1).sourceOffset(), "the held record (offset 3) is released after its dependency");
    }

    /**
     * A dependency on a <em>lower</em> offset of the record's own partition is satisfiable and
     * honoured: the record waits until that earlier offset has been delivered.
     *
     * Asserts the record is held while the backward dependency is unmet, then released once the
     * earlier same-partition record advances the frontier.
     */
    @Test
    void holdRecordUntilBackwardSamePartitionDependencyArrives() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // T1@5 depends on T1@3 — an earlier record on its own partition (backward dep, honoured).
        processRecord(engine, incomingRecord(T1, 5,
                CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build()));
        assertTrue(forwarded.isEmpty(), "backward same-partition dep must hold the record until it is satisfied");
        assertEquals(1, buffer.size(), "record must be buffered while the backward dependency is unmet");

        processRecord(engine, incomingRecord(T1, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded once the backward dependency arrives");
        assertEquals(3L, forwarded.get(0).sourceOffset(), "the satisfying earlier record (offset 3) is forwarded first");
        assertEquals(5L, forwarded.get(1).sourceOffset(), "the held record (offset 5) is released after its dependency");
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
        ParsleyEngine<String, String> engine =
                engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        Uuid oldT1 = new Uuid(0L, 1L);
        Uuid newT1 = new Uuid(0L, 2L);   // recreated — different UUID, same name + partition

        // T2 depends on t1-0@5 under the OLD incarnation.
        processRecord(engine, incomingRecordWithId(T2, 0, CausalPosition.deriveUuid(T2.topic()),
                CausalDependencies.builder().require(new CausalPosition(oldT1, 0, 5L)).build()));
        assertTrue(forwarded.isEmpty(), "T2 must be buffered: old-t1 dependency unsatisfied");

        // A record from the NEW t1 incarnation at the same offset must NOT unblock T2.
        processRecord(engine, incomingRecordWithId(T1, 5, newT1, CausalDependencies.empty()));
        assertEquals(1, forwarded.size(), "only the new-t1 record must be forwarded; T2 stays buffered");
        assertEquals(T1, forwarded.get(0).sourcePartition(), "new-incarnation record must be forwarded");

        // A record from the OLD t1 incarnation arrives — dependency now satisfied.
        processRecord(engine, incomingRecordWithId(T1, 5, oldT1, CausalDependencies.empty()));
        assertEquals(3, forwarded.size(), "old-t1 record forwarded, then buffered T2 released");
        assertEquals(T2, forwarded.get(2).sourcePartition(), "T2 must be released after old-t1 arrives");
    }

    // --- per-violation-type policy tests --------------------------------------------------------

    /**
     * Under a policy with per-type DROP action for MISSING_HEADER, a record without a
     * causal-dependencies header is discarded. The frontier still advances through the
     * record's source coordinate so downstream records are not held indefinitely.
     *
     * Asserts that nothing is forwarded and the frontier reflects the dropped record's offset.
     */
    @Test
    void discardRecordButAdvanceFrontierWhenHeaderMissingWithDropAction() {
        CausalBufferPolicy policy = CausalBufferPolicy.builder()
                .onMissing(ViolationAction.DROP)
                .onUnresolvable(ViolationAction.DROP)
                .onLimit(ViolationAction.DROP)
                .setLimit(CausalBufferLimit.ofSize(100))
                .build();
        ParsleyEngine<String, String> engine = engineWith(policy);

        processRecord(engine, incomingRecord(T1, 0, null));

        assertTrue(forwarded.isEmpty(), "DROP action must not forward the missing-header record");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 0)), engine.frontier(),
                "frontier must advance for MISSING_HEADER regardless of DROP action");
    }

    /**
     * A mixed per-type policy can forward missing-header records under FORWARD_UNSAFE while
     * dropping records that exceed the buffer limit under DROP, in a single engine instance.
     *
     * Asserts that the missing-header record is forwarded, the limit-reached record is not,
     * and both violations are reported in order.
     */
    @Test
    void forwardMissingHeaderAndDropLimitReachedUnderMixedPolicy() {
        CausalBufferPolicy policy = CausalBufferPolicy.builder()
                .onMissing(ViolationAction.FORWARD_UNSAFE)
                .onUnresolvable(ViolationAction.DROP)
                .onLimit(ViolationAction.DROP)
                .setLimit(CausalBufferLimit.ofSize(1))
                .build();
        ParsleyEngine<String, String> engine = engineWith(policy);

        // Legacy record (no header) → forwarded under FORWARD_UNSAFE
        processRecord(engine, incomingRecord(T1, 0, null));
        assertEquals(1, forwarded.size(), "MISSING_HEADER with FORWARD_UNSAFE must be forwarded");

        // Parsley record that exceeds the buffer limit → dropped under DROP
        processRecord(engine, incomingRecord(T2, 0, CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 99)).build()));
        assertEquals(1, forwarded.size(), "LIMIT_REACHED with DROP must not forward the evicted record");
        assertEquals(2, violations.size(), "both violations must be reported");
        assertEquals(CausalViolationReason.MISSING_HEADER, violations.get(0).reason(),
                "first violation must be MISSING_HEADER");
        assertEquals(CausalViolationReason.LIMIT_REACHED, violations.get(1).reason(),
                "second violation must be LIMIT_REACHED");
    }

    /**
     * Under a policy with FORWARD_UNSAFE action for LIMIT_REACHED, records evicted from the
     * buffer are forwarded and the frontier advances through their source coordinates.
     *
     * Asserts that the evicted record is forwarded and the frontier reflects its offset.
     */
    @Test
    void advanceFrontierWhenLimitReachedUnderForwardUnsafeAction() {
        CausalBufferPolicy policy = CausalBufferPolicy.builder()
                .onMissing(ViolationAction.DROP)
                .onUnresolvable(ViolationAction.DROP)
                .onLimit(ViolationAction.FORWARD_UNSAFE)
                .setLimit(CausalBufferLimit.ofSize(1))
                .build();
        ParsleyEngine<String, String> engine = engineWith(policy);

        processRecord(engine, incomingRecord(T2, 0, CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertEquals(1, forwarded.size(), "FORWARD_UNSAFE eviction must forward the record");
        assertEquals(CausalFrontier.empty().observe(new CausalPosition(T2_ID, 0, 0)), engine.frontier(),
                "frontier must advance when eviction action is FORWARD_UNSAFE");
    }

    /**
     * Under a policy with DROP action for LIMIT_REACHED, records evicted from the buffer are
     * discarded and the frontier does not advance through them.
     *
     * Asserts that nothing is forwarded and the frontier remains empty.
     */
    @Test
    void doNotAdvanceFrontierWhenLimitReachedUnderDropAction() {
        ParsleyEngine<String, String> engine = engineWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        processRecord(engine, incomingRecord(T2, 0, CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 99)).build()));

        assertTrue(forwarded.isEmpty(), "DROP eviction must not forward the record");
        assertEquals(CausalFrontier.empty(), engine.frontier(),
                "frontier must not advance when eviction action is DROP");
    }

    // --- helpers --------------------------------------------------------------------------------

    private ParsleyEngine<String, String> engineWith(CausalBufferPolicy policy) {
        return engineWith(policy, null);
    }

    private ParsleyEngine<String, String> engineWith(CausalBufferPolicy policy,
                                                      java.util.function.Consumer<ParsleyRecord<String, String>> sink) {
        return new ParsleyEngine<>(policy, violations::add, CausalFrontier.empty(), sink, frontiers::add,
                buffer, new MockWaitIndex(), ParsleyMetrics.NOOP);
    }

    private ParsleyEngine<String, String> engineWithClock(CausalBufferPolicy policy,
                                                           java.util.function.LongSupplier clock) {
        return new ParsleyEngine<>(policy, violations::add, CausalFrontier.empty(), null, frontiers::add,
                buffer, new MockWaitIndex(), ParsleyMetrics.NOOP, clock);
    }

    private List<CausalViolationReason> reasons() {
        return violations.stream().map(CausalViolation::reason).toList();
    }

    private void processRecord(ParsleyEngine<String, String> engine, ParsleyRecord<String, String> record) {
        forwarded.addAll(engine.onRecord(record));
    }

    private static ParsleyRecord<String, String> incomingRecord(TopicPartition tp, long offset,
                                                                  CausalDependencies deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        if (deps != null) {
            headers.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }

    private static ParsleyRecord<String, String> incomingRecordWithId(TopicPartition tp, long offset,
                                                                        Uuid topicId,
                                                                        CausalDependencies deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        if (deps != null) {
            headers.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC_ID, ParsleyRecord.uuidToBytes(topicId)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }

    private static ParsleyRecord<String, String> garbledRecord(TopicPartition tp, long offset) {
        return new ParsleyRecord<>("k", "v", 0L, List.of(
                new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, new byte[]{9, 9, 9}),
                new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)),
                new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())),
                new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset))));
    }
}
