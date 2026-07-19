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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyEngineTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    // T3 is never channelled by this node — a stand-in for a downstream/sibling node's own input
    // coordinate, folded into a DAG-wide committed epoch floor via mergeMin over every member.
    private static final Uuid T3_ID = Uuid.randomUuid();

    // A consumed scope owning partition 0 of t1 and t2 — what a Streams task sees for these sources.
    private static final ParsleyVectorClock.CoordinatePredicate SCOPE = (topicId, partition) ->
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

        processRecord(engine, incomingRecord(T1, 3, ParsleyVectorClock.empty()));

        assertEquals(1, forwarded.size(), "satisfied record must be forwarded immediately");
        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 3), engine.frontier(),
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

        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(T1_ID, 0, 3);
        processRecord(engine, incomingRecord(T2, 0, deps));
        assertTrue(forwarded.isEmpty(), "unsatisfied record must be held in the buffer");

        processRecord(engine, incomingRecord(T1, 3, ParsleyVectorClock.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded after the dependency arrives");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "buffered record must be released second");
        assertEquals(
                ParsleyVectorClock.empty()
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

        engine.receive(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(1, buffer.size(), "unsatisfied record must be held in the buffer");

        engine.receive(incomingRecord(T1, 3, ParsleyVectorClock.empty()));
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

        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T2_ID, 0, 3)));

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
        buffer.add(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)), 0L);
        ParsleyEngine<String, String> engine = engineWith();
        assertEquals(ParsleyVectorClock.empty(), engine.frontier(), "pre-buffered records must not advance the frontier on construction");

        processRecord(engine, incomingRecord(T1, 3, ParsleyVectorClock.empty()));

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

        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty()));

        assertEquals(1, forwarded.size(), "a trivially-satisfied record must be forwarded");
        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 0), engine.frontier(),
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
            @Override public void recordUnreachableDependencyError() {}
            @Override public void reportState(int depth, OptionalLong oldest) { reportedDepths.add(depth); }
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, capturing);

        engine.receive(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)));
        assertEquals(List.of(1), bufferedCounts, "recordBuffered must fire when a record enters the buffer");
        assertEquals(List.of(1), reportedDepths, "reportState must fire with the new buffer depth");
        assertTrue(releasedCounts.isEmpty(), "recordReleased must not fire while record is buffered");

        engine.receive(incomingRecord(T1, 3, ParsleyVectorClock.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased must fire with the count of drained records");
        assertEquals(List.of(1, 0), reportedDepths, "reportState must report the post-drain buffer depth");
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
        processRecord(engine, incomingRecord(T1, 3, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)));

        assertEquals(1, forwarded.size(), "self-dep is stripped → dependencies empty → forwarded immediately");
        assertEquals(0, buffer.size(), "self-dep record must never enter the buffer");
        assertEquals(ParsleyVectorClock.empty().observe(T1_ID, 0, 3), engine.frontier(),
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
        ParsleyVectorClock mixed = ParsleyVectorClock.empty()
                .observe(T2_ID, 0, 0)
                .observe(T1_ID, 0, 5);
        processRecord(engine, incomingRecord(T2, 0, mixed));
        assertTrue(forwarded.isEmpty(), "real dep on T1@5 must still hold the record in the buffer");
        assertEquals(1, buffer.size(), "record must be in the buffer while real dep is unmet");

        // T1@5 arrives — satisfies the real dep; T2 drains.
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty()));
        assertEquals(2, forwarded.size(), "satisfying record and buffered record must both be forwarded");
        assertEquals(T1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(T2, tp(forwarded.get(1)), "buffered record must be released second");
    }

    /**
     * A dependency on a <em>lower</em> offset of the record's own partition is satisfiable and
     * honoured. Realistically, by the time a record arrives, an earlier offset on its own partition
     * has already been delivered to {@link ParsleyEngine#receive} (Kafka guarantees strictly
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
        processRecord(engine, incomingRecord(T1, 3, ParsleyVectorClock.empty().observe(T2_ID, 0, 0)));
        assertTrue(forwarded.isEmpty(), "T1@3 is held on its own, unrelated, unmet dependency");

        // T1@5 depends on T1@3 — an earlier record on its own partition (backward dep, honoured) —
        // but T1@3 hasn't actually been forwarded yet, only buffered, so T1@5 must wait too.
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T1_ID, 0, 3)));
        assertTrue(forwarded.isEmpty(), "backward same-partition dep must hold T1@5 until T1@3 is actually forwarded");
        assertEquals(2, buffer.size(), "both T1@3 and T1@5 remain held");

        // T2@0 arrives, satisfying T1@3's own dependency — T1@3 releases, which in turn satisfies T1@5.
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty()));

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
                ParsleyVectorClock.empty().observe(oldT1, 0, 5L)));
        assertTrue(forwarded.isEmpty(), "T2 must be buffered: old-t1 dependency unsatisfied");

        // A record from the NEW t1 incarnation at the same offset must NOT unblock T2.
        processRecord(engine, incomingRecordWithId(T1, 5, newT1, ParsleyVectorClock.empty()));
        assertEquals(1, forwarded.size(), "only the new-t1 record must be forwarded; T2 stays buffered");
        assertEquals(T1, tp(forwarded.get(0)), "new-incarnation record must be forwarded");

        // A record from the OLD t1 incarnation arrives — dependency now satisfied.
        processRecord(engine, incomingRecordWithId(T1, 5, oldT1, ParsleyVectorClock.empty()));
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
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T2_ID, 0, 10)));
        assertTrue(forwarded.isEmpty(), "T1@5 must be held while its dependency is unmet");

        // A third record depends on exactly T1@5 having been forwarded.
        processRecord(engine, incomingRecord(T2, 1, ParsleyVectorClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(forwarded.isEmpty(), "T2@1 must be held: T1@5 has not actually been forwarded yet");

        // T1@6 has independently-satisfied (empty) deps and forwards immediately.
        processRecord(engine, incomingRecord(T1, 6, ParsleyVectorClock.empty()));

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
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T2_ID, 0, 0)));

        // T1@6, T1@7, T1@8 each forward immediately (independently-satisfied), piling up above the gap.
        processRecord(engine, incomingRecord(T1, 6, ParsleyVectorClock.empty()));
        processRecord(engine, incomingRecord(T1, 7, ParsleyVectorClock.empty()));
        processRecord(engine, incomingRecord(T1, 8, ParsleyVectorClock.empty()));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0), "frontier stalls below T1@5 the whole time");
        forwarded.clear();

        // T1@9 depends on T1@8 (its own immediate predecessor) — held, since the frontier hasn't
        // actually reached 8 yet (it is stuck behind the still-held T1@5).
        processRecord(engine, incomingRecord(T1, 9, ParsleyVectorClock.empty().observe(T1_ID, 0, 8)));
        assertTrue(forwarded.isEmpty(), "T1@9 must be held: the frontier hasn't reached 8 yet");
        assertEquals(2, buffer.size(), "T1@5 and T1@9 both remain held");
        forwarded.clear();

        // T2@0 satisfies T1@5's own dependency — releasing it closes the gap, and the frontier
        // should catch up through 6, 7, and 8 in one step, releasing T1@9 too.
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty()));

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
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T2_ID, 0, 0)));
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "baseline must seed T1/0 frontier at offset - 1 = 4");

        // T1@6–T1@12 forward immediately, piling up in the forwarded index above the gap.
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(engine, incomingRecord(T1, offset, ParsleyVectorClock.empty()));
        }
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "frontier must stall at 4 while the gap at T1@5 remains open");

        // Five records, each waiting on a distinct T1/0 offset spanning the entire jump range.
        processRecord(engine, incomingRecord(T1, 13, ParsleyVectorClock.empty().observe(T1_ID, 0, 8)));
        processRecord(engine, incomingRecord(T1, 14, ParsleyVectorClock.empty().observe(T1_ID, 0, 9)));
        processRecord(engine, incomingRecord(T1, 15, ParsleyVectorClock.empty().observe(T1_ID, 0, 10)));
        processRecord(engine, incomingRecord(T1, 16, ParsleyVectorClock.empty().observe(T1_ID, 0, 11)));
        processRecord(engine, incomingRecord(T1, 17, ParsleyVectorClock.empty().observe(T1_ID, 0, 12)));
        assertEquals(6, buffer.size(), "T1@5 and T1@13–T1@17 must all be held before the trigger");
        forwarded.clear();

        // T2@0 closes the gap: T1@5 releases, the frontier walks to 12 in one step, and all five
        // waiting records must be released in the same cascade — none left for eviction to handle.
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty()));

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
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T2_ID, 0, 0)));
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(engine, incomingRecord(T1, offset, ParsleyVectorClock.empty()));
        }
        assertEquals(4L, engine.frontier().offsetFor(T1_ID, 0),
                "frontier must stall at 4 until the gap is closed");

        // One record waiting on offset 10 — strictly inside the 4→12 jump range.
        processRecord(engine, incomingRecord(T1, 13, ParsleyVectorClock.empty().observe(T1_ID, 0, 10)));
        assertEquals(2, buffer.size(), "T1@5 and T1@13 must both be held");
        forwarded.clear();

        // T2@0 closes the gap; frontier jumps 4→12. T1@13 (waiting on 10, not 12) must be found.
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty()));

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

        ParsleyEngine<String, String> first = new ParsleyEngine<>(ParsleyVectorClock.empty(), sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        // T1@5 is held; T1@6, T1@7, T1@8 each forward immediately, piling up above the gap.
        first.receive(incomingRecord(T1, 5, ParsleyVectorClock.empty().observe(T2_ID, 0, 0)));
        first.receive(incomingRecord(T1, 6, ParsleyVectorClock.empty()));
        first.receive(incomingRecord(T1, 7, ParsleyVectorClock.empty()));
        first.receive(incomingRecord(T1, 8, ParsleyVectorClock.empty()));
        ParsleyVectorClock persistedFrontier = first.frontier();
        assertEquals(4L, persistedFrontier.offsetFor(T1_ID, 0),
                "frontier persisted at the gap, as it would be before a crash");

        // Simulate a restart: a brand-new engine instance, seeded with the persisted frontier and a
        // buffer restored from the changelog (still holding T1@5), and the SAME forwarded-index
        // contents (standing in for "restored from its own changelog") — no separate "ceiling"
        // value is needed; the forwarded index alone remembers that 6, 7, and 8 already went out.
        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(persistedFrontier, sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> released =
                restarted.receive(incomingRecord(T2, 0, ParsleyVectorClock.empty())).delivered();

        assertEquals(8L, restarted.frontier().offsetFor(T1_ID, 0),
                "the restarted instance must still catch up through 6, 7, and 8 via the surviving forwarded index");
        assertEquals(List.of(0L, 5L), released.stream().map(ParsleyMessage::offset).toList(),
                "T2@0 forwards and releases T1@5 on the restarted instance");
    }

    /**
     * Regression test for the BACKLOG.md cross-store tear: the buffer store and the frontier/
     * forwarded-index store are separate changelog topics with no cross-store atomicity, so {@code
     * propagate}/{@code drainSatisfied} persist the frontier's delivery before removing the record
     * from the buffer. A crash between the two writes — simulated here by a buffer store that
     * swallows one specific {@code remove()} call, standing in for "that changelog write never
     * landed" — must always leave the buffer holding a record the frontier has already recorded
     * delivered. That is a harmless at-least-once duplicate on the next drain, never the reverse (a
     * permanently stranded frontier no future dependency on that coordinate could ever cross).
     *
     * <p>Asserts the "crashed" instance's frontier already reflects T2@0 as delivered even though it
     * is still sitting in the buffer, and that a "restarted" instance redelivers it (a duplicate)
     * rather than wedging forever.
     */
    @Test
    void aCrashBetweenFrontierPersistAndBufferRemovalRedeliversAsAHarmlessDuplicateNeverAWedge() {
        SwallowingRemoveBufferStore<String, String> crashyBuffer = new SwallowingRemoveBufferStore<>(0L);

        ParsleyEngine<String, String> beforeCrash = new ParsleyEngine<>(ParsleyVectorClock.empty(), crashyBuffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP);

        // T2@0 depends on T1@5 and is held (sequence 0 in the buffer).
        beforeCrash.receive(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 5)));
        assertEquals(1, crashyBuffer.size(), "T2@0 must be held");

        // T1@5 satisfies it: propagate() releases T2@0, persisting the frontier advance before its
        // (swallowed) buffer removal — simulating a crash landing in that exact window.
        List<ParsleyMessage<String, String>> releasedBeforeCrash =
                beforeCrash.receive(incomingRecord(T1, 5, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of(5L, 0L), releasedBeforeCrash.stream().map(ParsleyMessage::offset).toList(),
                "both T1@5 and T2@0 are delivered in-process before the simulated crash");
        assertEquals(0L, beforeCrash.frontier().offsetFor(T2_ID, 0),
                "the frontier already recorded T2@0 as delivered — persisted before the swallowed removal");
        assertEquals(1, crashyBuffer.size(),
                "T2@0's buffer removal never landed (the simulated crash), so it is still sitting in "
                        + "the buffer");

        // "Restart": a fresh engine over the persisted (torn) frontier and a normal buffer store
        // standing in for the buffer changelog restoring the same still-held record.
        ParsleyVectorClock persistedFrontier = beforeCrash.frontier();
        MockBufferStore<String, String> restoredBuffer = new MockBufferStore<>();
        restoredBuffer.add(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 5)), 0L);

        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(persistedFrontier, restoredBuffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> releasedAfterRestart = restarted.drainAfterRestore().delivered();

        assertEquals(List.of(0L), releasedAfterRestart.stream().map(ParsleyMessage::offset).toList(),
                "the restarted instance redelivers T2@0 — a harmless at-least-once duplicate — rather "
                        + "than wedging forever");
        assertEquals(0, restoredBuffer.size(), "T2@0 must finally leave the buffer after the redelivery");
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
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 4)));
        assertTrue(forwarded.isEmpty(), "T2@0 must be held: T1/0 has never been observed");

        // T1@5 is the very first record this engine ever sees on T1/0. Establishing its baseline
        // (frontier = 4) is itself enough to satisfy T2@0's dependency — before T1@5 is even
        // dispositioned by its own dominates check.
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty()));

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
        ParsleyVectorClock restoredFrontier = ParsleyVectorClock.empty().observe(T1_ID, 0, 0);
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(restoredFrontier, buffer, new MockCandidateIndex(),
                forwardedIndex, ParsleyMetrics.NOOP);

        // The first record this (restarted) instance ever sees on T1/0 is offset 10 — far above
        // the restored frontier of 0. There is a real, unaccounted-for gap from 1 through 9.
        processRecord(engine, incomingRecord(T1, 10, ParsleyVectorClock.empty()));

        assertEquals(0L, engine.frontier().offsetFor(T1_ID, 0),
                "the frontier must stay at the restored value (0); it must not be corrupted into "
                        + "treating the unaccounted-for gap from 1-9 as moot just because "
                        + "seenCoordinates was fresh");
    }

    /**
     * Restart regression: the "coordinate marked seen even if the record is held" guard must survive
     * a restart. Pre-crash, T1@0 is held (deps on a third coordinate that never arrives before the
     * crash), so nothing on T1 was ever delivered and the persisted frontier has NO entry for T1 (the
     * offset-0 seed is a no-op); a record d on T2 depending on T1@0 is held too — correctly, since
     * its cause has not been delivered. After a restart (a fresh engine over the restored buffer,
     * with a fresh in-memory seen-set), the first new record on T1 arrives at offset 5. Without the
     * engine constructor re-marking T1 seen from the restored buffer, the baseline seed would fold
     * offsets 0-4 into the frontier as "outside the engine's purview" and release d before its
     * still-held cause T1@0 — an effect-before-cause delivery of exactly the kind the library exists
     * to prevent.
     *
     * Asserts the post-restart record does not re-trigger the seed (the T1 frontier stays at -1, the
     * gap at 0 intact), d stays held, and the whole chain then drains in causal order — d only after
     * T1@0 — once the third coordinate finally delivers.
     */
    @Test
    void restartDoesNotReSeedPastAStillHeldRecordAndReleaseItsDependents() {
        TopicPartition t3 = new TopicPartition("t3", 0);
        MockBufferStore<String, String> sharedBuffer = new MockBufferStore<>();
        MockForwardedIndex sharedIndex = new MockForwardedIndex();
        ParsleyEngine<String, String> first = new ParsleyEngine<>(ParsleyVectorClock.empty(), sharedBuffer,
                new MockCandidateIndex(), sharedIndex, ParsleyMetrics.NOOP);

        // T1@0 held on T3@0 (never arriving pre-crash); d = T2@0 held on its cause T1@0.
        first.receive(incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T3_ID, 0, 0)));
        first.receive(incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 0)));
        assertEquals(2, sharedBuffer.size(), "both records must be held before the crash");
        assertEquals(-1L, first.frontier().offsetFor(T1_ID, 0),
                "nothing on T1 was delivered, so the persisted frontier must have no T1 entry");

        // Restart: fresh engine (fresh seen-set) over the restored buffer and persisted frontier.
        ParsleyEngine<String, String> restarted = new ParsleyEngine<>(first.frontier(), sharedBuffer,
                new MockCandidateIndex(), sharedIndex, ParsleyMetrics.NOOP);

        // The first post-restart record on T1 arrives at offset 5, dependency-free.
        List<ParsleyMessage<String, String>> onT1At5 =
                restarted.receive(incomingRecord(T1, 5, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of(5L), onT1At5.stream().map(ParsleyMessage::offset).toList(),
                "only T1@5 itself may deliver; d must stay held because its cause T1@0 is still held");
        assertEquals(-1L, restarted.frontier().offsetFor(T1_ID, 0),
                "the baseline seed must not re-fire past the still-held T1@0: the contiguous frontier "
                        + "keeps the gap at offset 0 open");
        assertEquals(2, sharedBuffer.size(), "T1@0 and d must both still be held");

        // T3@0 finally arrives: T1@0 releases, then d — cause strictly before effect.
        List<ParsleyMessage<String, String>> drained = restarted.receive(
                incomingRecordWithId(t3, 0, T3_ID, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of("t3", "t1", "t2"), drained.stream().map(ParsleyMessage::topic).toList(),
                "the chain must drain in causal order: T3@0, then T1@0, then its dependent d");
        assertEquals(0, sharedBuffer.size(), "everything must have left the buffer");
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

        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T2_ID, 0, 2)));

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

        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T2_ID, 0, 5)));
        assertEquals(0, forwarded.size(), "an in-domain dependency at the bound must still gate the record");
        assertEquals(1, engine.bufferSize(), "the record must be held until its in-domain dependency is met");

        // Deliver T2 up to offset 5 on its own channel; the held record must then release.
        processRecord(engine, incomingRecord(T2, 5, ParsleyVectorClock.empty()));
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

        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty()));

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
        processRecord(engine, incomingRecord(T1, 5, ParsleyVectorClock.empty()));
        processRecord(engine, incomingRecord(T1, 7, ParsleyVectorClock.empty()));
        // First in-domain delivery: the frontier walks from the epoch origin 99 to 100, gap-unaffected.
        processRecord(engine, incomingRecord(T1, 100, ParsleyVectorClock.empty()));

        int before = forwarded.size();
        // A record whose only dependency is the delivered in-domain T1@100.
        processRecord(engine, incomingRecord(T2, 0, ParsleyVectorClock.empty().observe(T1_ID, 0, 100)));

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
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T2_ID, 0, 5), 1);
        ParsleyEngine<String, String> engine = engineWithEpochState(epoch);

        // Receive the epoch-2 boundary (floor T2@20). The window opens but cannot close (nothing delivered).
        engine.onEpochBoundary(new ParsleyEpochBoundary(2, ParsleyVectorClock.empty().observe(T2_ID, 0, 20)), T1_ID, 0);
        assertTrue(epoch.isTransitioning(), "the epoch-2 transition is in progress");

        // In-flight epoch-1 record depending on T2@10, which is in-domain under the old floor (5), not the new (20).
        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T2_ID, 0, 10)));
        assertEquals(0, forwarded.size(),
                "the in-flight record must be held against the old floor, not stripped and released early");
        assertEquals(1, engine.bufferSize(), "it waits for its in-domain epoch-1 dependency T2@10");

        // Delivering T2@10 releases it in causal order; the window is still open (completeness < F_2=20).
        processRecord(engine, incomingRecord(T2, 10, ParsleyVectorClock.empty()));
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
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyVectorClock.empty().observe(T2_ID, 0, 5), 1);
        ParsleyEngine<String, String> engine = engineWithEpochState(epoch);
        engine.onEpochBoundary(new ParsleyEpochBoundary(2, ParsleyVectorClock.empty().observe(T2_ID, 0, 20)), T2_ID, 0);

        // Deliver T2 contiguously up to the new floor 20 (all in-domain under the old floor 5).
        for (long offset = 6; offset <= 20; offset++) {
            processRecord(engine, incomingRecord(T2, offset, ParsleyVectorClock.empty()));
        }
        assertEquals(2L, epoch.settledEpochId(), "the window closes once completeness dominates F_2=20");
        assertEquals(20L, epoch.startsAt(T2_ID, 0), "epoch 2's floor is now the effective floor");

        // A fresh record depending on T2@15 — now below the effective floor — is stripped and delivers.
        int before = forwarded.size();
        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty().observe(T2_ID, 0, 15)));
        assertEquals(before + 1, forwarded.size(),
                "a dependency below the now-effective floor is stripped, so the record delivers immediately");
        assertEquals(0, engine.bufferSize(), "nothing is held");
    }

    /**
     * The DAG-wide committed floor {@code F_e} can name a coordinate this node never channels at all —
     * e.g. a downstream/sibling node's own input topic, folded in via {@code mergeMin} over every
     * member's published completeness. This node channels only T1; the epoch-2 floor names both T1@5
     * (in scope) and T3@3 (never channelled here). The window must still close once T1 alone reaches 5:
     * comparing completeness() against the *unfiltered* floor could never dominate T3, since this node's
     * completeness can never contain a coordinate it has no channel for.
     *
     * Asserts the transition settles at epoch 2 once the in-scope coordinate is satisfied, regardless of
     * the out-of-scope one this node can never observe.
     */
    @Test
    void windowClosesOnInScopeCoordinatesEvenWhenTheFloorNamesACoordinateThisNodeNeverChannels() {
        ParsleyEpochState epoch = new ParsleyEpochState(ParsleyVectorClock.empty(), 1);
        ParsleyEngine<String, String> engine = engineWithEpochStateTrackingChannels(epoch);

        // Establish T1 as this node's only channel.
        processRecord(engine, incomingRecord(T1, 0, ParsleyVectorClock.empty()));

        // Epoch-2 boundary floor names T1@5 (this node's own coordinate) and T3@3 (a downstream-only
        // coordinate this node never channels).
        ParsleyVectorClock floor = ParsleyVectorClock.empty().observe(T1_ID, 0, 5).observe(T3_ID, 0, 3);
        engine.onEpochBoundary(new ParsleyEpochBoundary(2, floor), T1_ID, 0);
        assertTrue(epoch.isTransitioning(), "the epoch-2 transition is in progress");

        for (long offset = 1; offset <= 5; offset++) {
            processRecord(engine, incomingRecord(T1, offset, ParsleyVectorClock.empty()));
        }

        assertEquals(2L, epoch.settledEpochId(),
                "the window must close once every in-scope coordinate is satisfied, even though T3 — "
                        + "never channelled by this node — can never appear in completeness()");
    }

    /**
     * A poison record (undecodable on the forward path) always fails the task — the regression guard for
     * the mutation-before-throw hazard: the buffer must not be touched before the throw.
     *
     * Asserts {@code receive} throws {@link ParsleyBufferDeserializationException} and the poisoned
     * record remains in the buffer (not removed).
     */
    @Test
    void poisonFailsTheTask() {
        TopicPartition t4 = new TopicPartition("t4", 0);
        Uuid t4Id = Uuid.randomUuid();
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);

        ParsleyVectorClock needsT4 = ParsleyVectorClock.empty().observe(t4Id, 0, 0);
        engine.receive(incomingRecordWithId(T1, 5, T1_ID, needsT4));
        buffer.poison(0L); // the only sequence added so far

        assertThrows(ParsleyBufferDeserializationException.class,
                () -> engine.receive(incomingRecordWithId(t4, 0, t4Id, ParsleyVectorClock.empty())),
                "a poison record on the forward path must fail the task");
        assertEquals(1, buffer.size(), "the poisoned record must remain in the buffer for recovery, not be removed");
    }

    /**
     * A record whose dependencies name a coordinate this node has no input channel for at all — here, a
     * coordinate simply outside the configured scope — can never be confirmed no matter how long it
     * waits, so it fails the task fast rather than silently treating the unreachable coordinate as
     * satisfied: this node can prove it cannot check the coordinate, never that the coordinate is
     * irrelevant.
     *
     * Asserts {@code receive} throws {@link ParsleyUnreachableDependencyException} and the record is
     * never added to the buffer.
     */
    @Test
    void unreachableDependencyFailsTheTask() {
        // Only T1 is in scope; T2 is a coordinate this node has no channel for at all.
        ParsleyVectorClock.CoordinatePredicate onlyT1 = (topicId, partition) -> partition == 0 && topicId.equals(T1_ID);
        ParsleyChannels frontier = new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(frontier, buffer, new MockCandidateIndex(),
                ParsleyMetrics.NOOP, System::currentTimeMillis, onlyT1);

        ParsleyVectorClock needsT2 = ParsleyVectorClock.empty().observe(T2_ID, 0, 0);

        assertThrows(ParsleyUnreachableDependencyException.class,
                () -> engine.receive(incomingRecord(T1, 0, needsT2)),
                "a dependency on a coordinate outside this node's scope must fail the task");
        assertEquals(0, buffer.size(), "the record must never be added to the buffer");
    }

    /**
     * The unreachable-dependency check must run <em>before</em> {@code receive} mutates any persisted
     * state. A first-ever observation of a coordinate seeds the frontier for it ({@code seedIfFirstSeen}
     * persists, folding the below-first-seen history in), so an unreachable-dependency throw that ran
     * after the seed would leave the frontier advanced by a record that never delivered — and could have
     * released and un-buffered other records into a result list the throw then discards. Under EOS the
     * whole batch rolls back so persisted state stays consistent, but an in-memory engine has no
     * rollback; failing before the first mutation keeps the two consistent.
     *
     * Asserts that after the throw the frontier is still empty — the failing record's first-observation
     * seed never took effect.
     */
    @Test
    void unreachableDependencyFailsBeforeSeedingTheFrontier() {
        // T1 and T2 are in scope; T3 is a coordinate this node has no channel for.
        ParsleyVectorClock.CoordinatePredicate scope = (topicId, partition) ->
                partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID));
        MockBufferStore<String, String> localBuffer = new MockBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex()),
                localBuffer, new MockCandidateIndex(), ParsleyMetrics.NOOP, System::currentTimeMillis, scope);

        // A first observation of T1 at offset 5 would seed the frontier to T1@4 inside seedIfFirstSeen —
        // but this record depends on T3, outside scope, so it must fail first and seed nothing.
        ParsleyVectorClock needsT3 = ParsleyVectorClock.empty().observe(T3_ID, 0, 0);

        assertThrows(ParsleyUnreachableDependencyException.class,
                () -> engine.receive(incomingRecord(T1, 5, needsT3)),
                "a dependency on a coordinate outside this node's scope must fail the task");
        assertEquals(ParsleyVectorClock.empty(), engine.frontier(),
                "the failing record must not have seeded the frontier — the unreachable check runs "
                        + "before any state mutation, so an in-memory engine (no EOS rollback) stays consistent");
        assertEquals(0, localBuffer.size(), "the failing record must never be buffered");
    }

    // --- helpers --------------------------------------------------------------------------------

    // These helpers build an engine over an untracked in-memory frontier — completeness() is the node's
    // own frontier — exercising the frontier/buffer mechanics in isolation. The cross-channel
    // completeness layer is covered by ParsleyEngineCompletenessTest.

    private ParsleyEngine<String, String> engineWith() {
        return new ParsleyEngine<>(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineConsuming(ParsleyVectorClock.CoordinatePredicate inScope) {
        return new ParsleyEngine<>(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineWithEpoch(ParsleyEpoch epoch) {
        return new ParsleyEngine<>(
                new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex, false, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    /**
     * As {@link #engineWithEpoch}, but with channel tracking on so {@link ParsleyEngine#completeness()}
     * is the cross-channel min (not the node's own frontier) — needed to exercise below-floor deliveries
     * whose in-domain progress must still surface through completeness.
     */
    private ParsleyEngine<String, String> engineWithEpochTrackingChannels(ParsleyEpoch epoch) {
        return new ParsleyEngine<>(
                new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex, true, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    // A live ParsleyEpochState over a frontier-only (untracked-channels) engine: completeness() is the
    // node's own frontier, and the transition window's per-channel marker check is vacuous (no channels),
    // so the window closes purely on completeness dominating F_e — exactly what these transition tests
    // exercise, without needing to stage watermarks.
    private ParsleyEngine<String, String> engineWithEpochState(ParsleyEpochState epoch) {
        return new ParsleyEngine<>(
                new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex, false, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    /**
     * As {@link #engineWithEpochState}, but with channel tracking on so {@link
     * ParsleyEngine#completeness()} is the cross-channel min scoped to this node's own channels — needed
     * to exercise a pending floor naming a coordinate this node never channels at all.
     */
    private ParsleyEngine<String, String> engineWithEpochStateTrackingChannels(ParsleyEpochState epoch) {
        return new ParsleyEngine<>(
                new ParsleyChannels(ParsleyVectorClock.empty(), forwardedIndex, true, epoch),
                buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> engineWithClock(java.util.function.LongSupplier clock) {
        return new ParsleyEngine<>(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP, clock);
    }

    private void processRecord(ParsleyEngine<String, String> engine, ParsleyMessage<String, String> message) {
        forwarded.addAll(engine.receive(message).delivered());
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
                                                                  ParsleyVectorClock deps) {
        return incomingRecordWithId(tp, offset, idFor(tp), deps);
    }

    private static ParsleyMessage<String, String> incomingRecordWithId(TopicPartition tp, long offset,
                                                                        Uuid topicId,
                                                                        ParsleyVectorClock deps) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), deps == null ? ParsleyVectorClock.empty() : deps);
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
        @Override public List<IndexEntry> indexEntries() { indexEntriesCalls++; return delegate.indexEntries(); }
        @Override public void remove(long sequence) { delegate.remove(sequence); }
        @Override public int size() { return delegate.size(); }
        @Override public OptionalLong oldestBufferedAt() { return delegate.oldestBufferedAt(); }
    }

    /**
     * Wraps a {@link MockBufferStore}, swallowing {@code remove()} for one specific sequence — standing
     * in for a crash that lands after the frontier's changelog write commits but before the buffer's
     * removal does, so the record is still sitting in the buffer once "restarted" against the
     * already-persisted (post-crash) frontier.
     */
    private static final class SwallowingRemoveBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
        private final long swallowedSequence;
        SwallowingRemoveBufferStore(long swallowedSequence) { this.swallowedSequence = swallowedSequence; }
        @Override public long add(ParsleyMessage<K, V> record, long bufferedAt) { return delegate.add(record, bufferedAt); }
        @Override public Entry<K, V> get(long sequence) { return delegate.get(sequence); }
        @Override public List<IndexEntry> indexEntries() { return delegate.indexEntries(); }
        @Override public void remove(long sequence) { if (sequence != swallowedSequence) delegate.remove(sequence); }
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
