package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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

    // A consumed scope owning partition 0 of t1 and t2 — what a Streams task sees for these sources.
    private static final ParsleyClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID));

    private final List<ParsleyMessage<String, String>> forwarded = new ArrayList<>();
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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

        engine.onRecord(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(1, buffer.size(), "unsatisfied record must be held in the buffer");

        engine.onRecord(incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(0, buffer.size(), "drained record must be removed from the buffer");
    }

    /**
     * Regression guard for the opposite case the fix must preserve: a dependency on a coordinate this
     * engine <em>does</em> consume but has not yet observed still blocks. Scoping must not collapse
     * "behind on a coordinate I own" into "vacuously satisfied".
     *
     * Asserts the record is held while the in-scope coordinate is unobserved.
     */
    @Test
    void dependencyOnConsumedButUnobservedCoordinateStillBlocks() {
        ParsleyEngine<String, String> engine = engineConsuming(SCOPE);

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty().observe(T2_ID, 0, 3)));

        assertTrue(forwarded.isEmpty(), "an unobserved in-scope dependency must still hold the record");
        assertEquals(1, buffer.size(), "the record must be buffered, not forwarded");
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
        ParsleyEngine<String, String> engine = engineWith();
        assertEquals(ParsleyClock.empty(), engine.frontier(), "pre-buffered records must not advance the frontier on construction");

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));

        assertEquals(2, forwarded.size(), "satisfying record and pre-buffered record must both be forwarded");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "pre-buffered record must be released second");
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
        ParsleyEngine<String, String> engine = engineWith();

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty()));

        assertEquals(1, forwarded.size(), "a trivially-satisfied record must be forwarded");
        assertEquals(ParsleyClock.empty().observe(T1_ID, 0, 0), engine.frontier(),
                "frontier must advance through the forwarded record");
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
            @Override public void recordDeserializationError()  {}
            @Override public void recordClockResolutionError()  {}
            @Override public void recordDeadLetter()            {}
            @Override public void reportState(int depth, OptionalLong oldest) { reportedDepths.add(depth); }
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, new MockOrphanIndex(), capturing);

        engine.onRecord(incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(List.of(1), bufferedCounts, "recordBuffered must fire when a record enters the buffer");
        assertEquals(List.of(1), reportedDepths, "reportState must fire with the new buffer depth");
        assertTrue(releasedCounts.isEmpty(), "recordReleased must not fire while record is buffered");

        engine.onRecord(incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased must fire with the count of drained records");
        assertEquals(List.of(1, 0), reportedDepths, "reportState must report the post-drain buffer depth");
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
        ParsleyEngine<String, String> engine = engineWithAudit(audit);

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
        ParsleyEngine<String, String> engine = engineWithAudit(audit);

        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 3)));
        assertTrue(audit.released.isEmpty(), "recordReleased must not fire while the record is held");

        processRecord(engine, incomingRecord(T1, 3, ParsleyClock.empty()));
        assertEquals(List.of(new RecordingCausalAudit.Released("t2", 0, 0L, 0)), audit.released,
                "the released record must be reported with the post-removal buffer depth");
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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = engineWith();

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
     * When the contiguous frontier jumps from one value to a much higher one in a single step,
     * every buffered record whose dependency falls <em>anywhere</em> in that advanced range must
     * be released in the same cascade — not only records waiting on exactly the new boundary.
     *
     * <p>T1@5 holds the gap while T1@6–T1@12 pile up in the forwarded index. Five records each
     * wait on a different T1/0 offset inside that range (8, 9, 10, 11, 12). When T2@0 closes the
     * gap and releases T1@5, the frontier walks from 4 to 12 in one step, and the candidate-index
     * scan of {@code [0, 12]} must find all five — not just the record waiting on 12.
     *
     * <p>Asserts all five are forwarded in the same cascade, without any eviction trigger, and
     * the buffer is empty afterward.
     */
    @Test
    void frontierJumpReleasesAllWaitingRecordsAcrossTheJumpedRange() {
        ParsleyEngine<String, String> engine = engineWith();

        // T1@5 holds the gap; T1's baseline is seeded to 4 (offset - 1).
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 0)));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "baseline must seed T1/0 frontier at offset - 1 = 4");

        // T1@6–T1@12 forward immediately, piling up in the forwarded index above the gap.
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(engine, incomingRecord(T1, offset, ParsleyClock.empty()));
        }
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "frontier must stall at 4 while the gap at T1@5 remains open");

        // Five records, each waiting on a distinct T1/0 offset spanning the entire jump range.
        processRecord(engine, incomingRecord(T1, 13, ParsleyClock.empty().observe(T1_ID, 0, 8)));
        processRecord(engine, incomingRecord(T1, 14, ParsleyClock.empty().observe(T1_ID, 0, 9)));
        processRecord(engine, incomingRecord(T1, 15, ParsleyClock.empty().observe(T1_ID, 0, 10)));
        processRecord(engine, incomingRecord(T1, 16, ParsleyClock.empty().observe(T1_ID, 0, 11)));
        processRecord(engine, incomingRecord(T1, 17, ParsleyClock.empty().observe(T1_ID, 0, 12)));
        assertEquals(6, buffer.size(), "T1@5 and T1@13–T1@17 must all be held before the trigger");
        forwarded.clear();

        // T2@0 closes the gap: T1@5 releases, the frontier walks to 12 in one step, and all five
        // waiting records must be released in the same cascade — none left for eviction to handle.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty()));

        assertEquals(
                List.of(0L, 5L, 13L, 14L, 15L, 16L, 17L),
                forwarded.stream().map(ParsleyMessage::offset).toList(),
                "after the 4→12 frontier jump every record waiting in the range 8–12 must be "
                        + "released eagerly — range scan must cover all newly-satisfied offsets, not just 12");
        assertEquals(17L, engine.frontier().offsetFor(T1_ID, 0),
                "frontier must advance through the full release cascade to 17");
        assertEquals(0, buffer.size(),
                "buffer must be empty — all releases must be driven by the frontier advance, not eviction");
    }

    /**
     * A buffered record whose required dependency offset is strictly inside (neither at the bottom
     * nor the top of) a frontier jump range must be released in the same cascade as the jump —
     * the candidate-index range scan must cover the whole range, not just the exact new boundary.
     *
     * <p>The frontier jumps 4 → 12 when T2@0 releases T1@5, but the single waiting record depends
     * on T1/0 at offset 10, which is interior to the jumped range. It must be released immediately
     * in that cascade without any eviction trigger.
     *
     * <p>Asserts the intermediate-offset record is forwarded and the buffer is empty afterward.
     */
    @Test
    void frontierJumpReleasesRecordWaitingOnIntermediateOffsetNotJustFinalOffset() {
        ParsleyEngine<String, String> engine = engineWith();

        // T1@5 holds the gap; T1@6–T1@12 pile up in the forwarded index above it.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty().observe(T2_ID, 0, 0)));
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(engine, incomingRecord(T1, offset, ParsleyClock.empty()));
        }
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "frontier must stall at 4 until the gap is closed");

        // One record waiting on offset 10 — strictly inside the 4→12 jump range.
        processRecord(engine, incomingRecord(T1, 13, ParsleyClock.empty().observe(T1_ID, 0, 10)));
        assertEquals(2, buffer.size(), "T1@5 and T1@13 must both be held");
        forwarded.clear();

        // T2@0 closes the gap; frontier jumps 4→12. T1@13 (waiting on 10, not 12) must be found.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty()));

        assertEquals(
                List.of(0L, 5L, 13L),
                forwarded.stream().map(ParsleyMessage::offset).toList(),
                "T1@13 depends on T1/0 offset 10 (interior to the 4→12 jump) and must be released "
                        + "eagerly — offset 10 must be found by the range scan, not missed because "
                        + "the frontier landed on 12");
        assertEquals(0, buffer.size(),
                "buffer must be empty — the intermediate-offset record must not remain until eviction");
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

        ParsleyEngine<String, String> first = new ParsleyEngine<>(ParsleyClock.empty(), sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP);

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
        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(persistedFrontier, sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> released =
                restarted.onRecord(incomingRecord(T2, 0, ParsleyClock.empty())).delivered();

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
        ParsleyEngine<String, String> engine = engineWith();

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
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(restoredFrontier, buffer, new MockCandidateIndex(),
                forwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP);

        // The first record this (restarted) instance ever sees on T1/0 is offset 10 — far above
        // the restored frontier of 0. There is a real, unaccounted-for gap from 1 through 9.
        processRecord(engine, incomingRecord(T1, 10, ParsleyClock.empty()));

        assertEquals(0L, engine.frontier().offsetFor(T1_ID, 0),
                "the frontier must stay at the restored value (0); it must not be corrupted into "
                        + "treating the unaccounted-for gap from 1-9 as moot just because "
                        + "seenCoordinates was fresh");
    }

    /**
     * A record depending on a coordinate below its topology-epoch {@code startsAt} bound is delivered:
     * the below-bound dependency is stripped before the completeness check, so a coordinate no channel
     * will ever confirm no longer holds the record forever.
     *
     * <p>The record on T1 depends on T2@2, but the epoch bounds T2 at {@code startsAt = 5}, so T2@2 is
     * an out-of-domain reference (a prior, closed epoch) and is stripped. Without the bound the record
     * would be held indefinitely (nothing ever delivers T2@2).
     *
     * Asserts the record forwards immediately despite the unsatisfied-but-stripped dependency.
     */
    @Test
    void dependencyBelowEpochBoundIsStrippedAndRecordDelivers() {
        ParsleyEpoch bound = (topicId, partition) ->
                topicId.equals(T2_ID) ? 5L : ParsleyEpoch.NO_BOUND;
        ParsleyEngine<String, String> engine = engineWithEpoch(bound);

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty().observe(T2_ID, 0, 2)));

        assertEquals(1, forwarded.size(),
                "a record whose only unsatisfied dependency is below the epoch bound must deliver, not hold");
        assertEquals(0, engine.bufferSize(), "the record must not be buffered");
    }

    /**
     * A dependency at or above the epoch {@code startsAt} bound is gated normally: it is in-domain, so
     * it is not stripped, and the record is held until the frontier confirms it — no behaviour change
     * from a bound-free engine.
     *
     * <p>The record on T1 depends on T2@5 with the epoch bounding T2 at {@code startsAt = 5}. T2@5 is
     * exactly at the bound (in-domain), so it is not stripped and the record is held until T2@5 is
     * delivered on its own channel.
     *
     * Asserts the record is held while the in-domain dependency is unmet, then released once it arrives.
     */
    @Test
    void dependencyAtOrAboveEpochBoundIsGatedNormally() {
        ParsleyEpoch bound = (topicId, partition) ->
                topicId.equals(T2_ID) ? 5L : ParsleyEpoch.NO_BOUND;
        ParsleyEngine<String, String> engine = engineWithEpoch(bound);

        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty().observe(T2_ID, 0, 5)));
        assertEquals(0, forwarded.size(), "an in-domain dependency at the bound must still gate the record");
        assertEquals(1, engine.bufferSize(), "the record must be held until its in-domain dependency is met");

        // Deliver T2 up to offset 5 on its own channel; the held record must then release.
        processRecord(engine, incomingRecord(T2, 5, ParsleyClock.empty()));
        assertTrue(forwarded.size() >= 2,
                "once T2@5 is confirmed, the held record releases (both records forwarded)");
        assertEquals(0, engine.bufferSize(), "the buffer must drain once the in-domain dependency is met");
    }

    /**
     * A below-floor record is delivered (it feeds state) but does not advance the causal frontier into
     * the domain: with T1 floored at 100, a T1@5 record with no dependencies forwards immediately, and
     * the frontier stays at the epoch origin (below the floor) rather than jumping to 5 — so nothing
     * in-domain is confirmed.
     *
     * Asserts the record forwards while the completeness frontier stays below the floor (at the origin).
     */
    @Test
    void belowFloorRecordDeliversButDoesNotAdvanceIntoTheDomain() {
        ParsleyEpoch bound = (topicId, partition) -> topicId.equals(T1_ID) ? 100L : ParsleyEpoch.NO_BOUND;
        ParsleyEngine<String, String> engine = engineWithEpochTrackingChannels(bound);

        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty()));

        assertEquals(1, forwarded.size(), "a below-floor record with no unmet dependency must deliver");
        assertEquals(0, engine.bufferSize(), "the below-floor record must not be buffered");
        assertTrue(engine.completeness().offsetFor(T1_ID, 0) < 100L,
                "the below-floor delivery must not advance completeness into the domain (it stays at the origin)");
    }

    /**
     * The load-bearing case for frontier flooring: below-floor history arriving with a gap must not
     * stall the in-domain frontier. With T1 floored at 100, records T1@5 and T1@7 (offset 6 missing)
     * are delivered below the floor, then in-domain T1@100 arrives. A later record depending on T1@100
     * must deliver — the below-floor gap at 6 is out of domain and cannot stall the epoch-origin walk to
     * 100. Without frontier flooring the contiguous walk would stall at 5 and the dependent record would
     * be held forever.
     *
     * Asserts the T1@100-dependent record forwards once T1@100 is delivered.
     */
    @Test
    void belowFloorGapDoesNotStallTheInDomainFrontier() {
        ParsleyEpoch bound = (topicId, partition) -> topicId.equals(T1_ID) ? 100L : ParsleyEpoch.NO_BOUND;
        ParsleyEngine<String, String> engine = engineWithEpoch(bound);

        // Below-floor replay with a gap at offset 6 — all out of domain, none advances the frontier.
        processRecord(engine, incomingRecord(T1, 5, ParsleyClock.empty()));
        processRecord(engine, incomingRecord(T1, 7, ParsleyClock.empty()));
        // First in-domain delivery: the frontier walks from the epoch origin 99 to 100, gap-unaffected.
        processRecord(engine, incomingRecord(T1, 100, ParsleyClock.empty()));

        int before = forwarded.size();
        // A record whose only dependency is the delivered in-domain T1@100.
        processRecord(engine, incomingRecord(T2, 0, ParsleyClock.empty().observe(T1_ID, 0, 100)));

        assertEquals(before + 1, forwarded.size(),
                "a record depending on the delivered in-domain T1@100 must forward — the below-floor "
                        + "gap at offset 6 must not stall the frontier below the floor");
        assertEquals(0, engine.bufferSize(), "nothing may remain held once the in-domain dependency is met");
    }

    /**
     * The overlapping-epoch transition honours the old floor for in-flight records. Settled at epoch 1
     * (floor T2@5), a boundary raises T2's floor to 20 (epoch 2) but stays in progress. During the
     * window the effective floor is the old F_1=5, so a record depending on T2@10 — in {@code [5, 20)} —
     * is held and waited for, not stripped as if below the new floor and released out of causal order.
     *
     * Asserts the in-flight record is held during the window and releases only once T2@10 is delivered.
     */
    @Test
    void inFlightPriorEpochRecordIsGatedAgainstTheOldFloorDuringTheWindow() {
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyClock.empty().observe(T2_ID, 0, 5), 1);
        ParsleyEngine<String, String> engine = engineWithEpochState(epoch);

        // Receive the epoch-2 boundary (floor T2@20). The window opens but cannot close (nothing delivered).
        engine.onEpochBoundary(new ParsleyEpochBoundary(2, ParsleyClock.empty().observe(T2_ID, 0, 20)), T1_ID, 0);
        assertTrue(epoch.isTransitioning(), "the epoch-2 transition is in progress");

        // In-flight epoch-1 record depending on T2@10, which is in-domain under the old floor (5), not the new (20).
        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty().observe(T2_ID, 0, 10)));
        assertEquals(0, forwarded.size(),
                "the in-flight record must be held against the old floor, not stripped and released early");
        assertEquals(1, engine.bufferSize(), "it waits for its in-domain epoch-1 dependency T2@10");

        // Delivering T2@10 releases it in causal order; the window is still open (completeness < F_2=20).
        processRecord(engine, incomingRecord(T2, 10, ParsleyClock.empty()));
        assertTrue(forwarded.size() >= 2, "once T2@10 is delivered, the in-flight record releases in order");
        assertEquals(0, engine.bufferSize(), "the buffer drains");
        assertEquals(1L, epoch.settledEpochId(), "the window has not closed (completeness has not reached F_2=20)");
    }

    /**
     * The transition window closes when the delivered frontier dominates the new floor, and only then
     * does the new floor take effect. Delivering T2 up to 20 closes the epoch-2 window; afterwards a
     * dependency on T2@15 — now below the effective floor 20 — is stripped and its record delivers.
     *
     * Asserts the settled epoch advances to 2 at the boundary and the new floor then governs stripping.
     */
    @Test
    void windowClosesWhenCompletenessDominatesTheBoundaryThenTheNewFloorApplies() {
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyClock.empty().observe(T2_ID, 0, 5), 1);
        ParsleyEngine<String, String> engine = engineWithEpochState(epoch);
        engine.onEpochBoundary(new ParsleyEpochBoundary(2, ParsleyClock.empty().observe(T2_ID, 0, 20)), T2_ID, 0);

        // Deliver T2 contiguously up to the new floor 20 (all in-domain under the old floor 5).
        for (long offset = 6; offset <= 20; offset++) {
            processRecord(engine, incomingRecord(T2, offset, ParsleyClock.empty()));
        }
        assertEquals(2L, epoch.settledEpochId(), "the window closes once completeness dominates F_2=20");
        assertEquals(20L, epoch.startsAt(T2_ID, 0), "epoch 2's floor is now the effective floor");

        // A fresh record depending on T2@15 — now below the effective floor — is stripped and delivers.
        int before = forwarded.size();
        processRecord(engine, incomingRecord(T1, 0, ParsleyClock.empty().observe(T2_ID, 0, 15)));
        assertEquals(before + 1, forwarded.size(),
                "a dependency below the now-effective floor is stripped, so the record delivers immediately");
        assertEquals(0, engine.bufferSize(), "nothing is held");
    }

    // --- helpers --------------------------------------------------------------------------------

    // These helpers build an engine over an untracked in-memory frontier — completeness() is the node's
    // own frontier — exercising the frontier/buffer mechanics in isolation. The cross-channel
    // completeness layer is covered by ParsleyEngineCompletenessTest.

    private ParsleyEngine<String, String> engineWith() {
        return new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineConsuming(ParsleyClock.CoordinatePredicate inScope) {
        return new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineWithEpoch(ParsleyEpoch epoch) {
        return new ParsleyEngine<>(
                new ParsleyFrontier(ParsleyClock.empty(), forwardedIndex, new MockOrphanIndex(), false, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    /**
     * As {@link #engineWithEpoch}, but with channel tracking on so {@link ParsleyEngine#completeness()}
     * is the cross-channel min (not the node's own frontier) — needed to exercise below-floor deliveries
     * whose in-domain progress must still surface through completeness.
     */
    private ParsleyEngine<String, String> engineWithEpochTrackingChannels(ParsleyEpoch epoch) {
        return new ParsleyEngine<>(
                new ParsleyFrontier(ParsleyClock.empty(), forwardedIndex, new MockOrphanIndex(), true, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    // A live ParsleyEpochState over a frontier-only (untracked-channels) engine: completeness() is the
    // node's own frontier, and the transition window's per-channel marker check is vacuous (no channels),
    // so the window closes purely on completeness dominating F_e — exactly what these transition tests
    // exercise, without needing to stage watermarks.
    private ParsleyEngine<String, String> engineWithEpochState(ParsleyEpochState epoch) {
        return new ParsleyEngine<>(
                new ParsleyFrontier(ParsleyClock.empty(), forwardedIndex, new MockOrphanIndex(), false, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineWithClock(java.util.function.LongSupplier clock) {
        return new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, clock);
    }

    private ParsleyEngine<String, String> engineWithAudit(CausalAudit audit) {
        return new ParsleyEngine<>(ParsleyClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, new MockOrphanIndex(), ParsleyMetrics.NOOP, audit,
                System::currentTimeMillis);
    }

    private void processRecord(ParsleyEngine<String, String> engine, ParsleyMessage<String, String> message) {
        forwarded.addAll(engine.onRecord(message).delivered());
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

    /**
     * Wraps a {@link MockBufferStore}, counting {@code indexEntries()} calls — used to prove an
     * eviction guard short-circuits before consulting the buffer's index at all (beyond the engine
     * constructor's own one-time wait-index rebuild scan), rather than relying on a downstream
     * computation that happens to converge on the same (empty) result.
     */
    private static final class CountingIndexEntriesBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
        int indexEntriesCalls = 0;
        @Override public long add(ParsleyMessage<K, V> record, long bufferedAt) { return delegate.add(record, bufferedAt); }
        @Override public Entry<K, V> get(long sequence) { return delegate.get(sequence); }
        @Override public List<Entry<K, V>> entries() { return delegate.entries(); }
        @Override public List<IndexEntry> indexEntries() { indexEntriesCalls++; return delegate.indexEntries(); }
        @Override public void remove(long sequence) { delegate.remove(sequence); }
        @Override public int size() { return delegate.size(); }
        @Override public OptionalLong oldestBufferedAt() { return delegate.oldestBufferedAt(); }
    }

    /**
     * Wraps a {@link MockBufferStore} but returns {@code indexEntries()} in reverse-insertion order —
     * proving a caller's own sort over those entries is load-bearing, not merely redundant with
     * {@link MockBufferStore}'s {@code TreeMap}-backed iteration, which is already sorted and would
     * otherwise mask a regression that removes the sort.
     */
    private static final class ReverseOrderBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
        @Override public long add(ParsleyMessage<K, V> record, long bufferedAt) { return delegate.add(record, bufferedAt); }
        @Override public Entry<K, V> get(long sequence) { return delegate.get(sequence); }
        @Override public List<Entry<K, V>> entries() { return delegate.entries(); }
        @Override public List<IndexEntry> indexEntries() {
            List<IndexEntry> reversed = new ArrayList<>(delegate.indexEntries());
            Collections.reverse(reversed);
            return reversed;
        }
        @Override public void remove(long sequence) { delegate.remove(sequence); }
        @Override public int size() { return delegate.size(); }
        @Override public OptionalLong oldestBufferedAt() { return delegate.oldestBufferedAt(); }
    }
}
