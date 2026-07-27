package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.jspecify.annotations.Nullable;

class ParsleyCausalBroadcastTest {

    private static final TopicPartition C1 = new TopicPartition("c1", 0);
    private static final TopicPartition C2 = new TopicPartition("c2", 0);
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    // C3 is never channelled by this node — a stand-in for a downstream/sibling node's own input
    // coordinate, the kind an inbound clock can name without this node consuming it.
    private static final Uuid C3_ID = Uuid.randomUuid();

    // A consumed scope owning partition 0 of c1 and c2 — what a Streams task sees for these sources.
    private static final ParsleyVectorClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(C1_ID) || topicId.equals(C2_ID));

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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty()));

        assertEquals(1, forwarded.size(), "satisfied record must be forwarded immediately");
        assertEquals(ParsleyVectorClock.empty().observe(C1_ID, 0, 3), causalBroadcast.frontier(),
                "frontier must advance to the forwarded record's source offset");
    }

    /**
     * A record whose dependencies are not yet satisfied is held in the buffer until the
     * frontier advances to cover the required coordinate.
     *
     * <p>When the dependency arrives, the core releases the buffered record after the
     * satisfying record and advances the frontier through both. Both released records are
     * stamped in causal order — the result reflects whether the core ever
     * had to forcibly evict, not how long a record waited.
     *
     * Asserts that the buffered record is released in causal order after the satisfying record,
     * both carry the SATISFIED result, and the frontier reflects both records' source offsets.
     */
    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(C1_ID, 0, 3);
        processRecord(causalBroadcast, incomingRecord(C2, 0, deps));
        assertTrue(forwarded.isEmpty(), "unsatisfied record must be held in the buffer");

        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty()));

        assertEquals(2, forwarded.size(), "both records must be forwarded after the dependency arrives");
        assertEquals(C1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(C2, tp(forwarded.get(1)), "buffered record must be released second");
        assertEquals(
                ParsleyVectorClock.empty()
                        .observe(C1_ID, 0, 3)
                        .observe(C2_ID, 0, 0),
                causalBroadcast.frontier(),
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        causalBroadcast.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 3)));
        assertEquals(1, buffer.size(), "unsatisfied record must be held in the buffer");

        causalBroadcast.receive(incomingRecord(C1, 3, ParsleyVectorClock.empty()));
        assertEquals(0, buffer.size(), "drained record must be removed from the buffer");
    }

    /**
     * Guards the opposite case from the ignore branch: a dependency on a coordinate this
     * core <em>does</em> consume but has not yet observed still blocks. Scoping must not collapse
     * "behind on a coordinate I own" into "vacuously satisfied".
     *
     * Asserts the record is held while the in-scope coordinate is unobserved.
     */
    @Test
    void dependencyOnConsumedButUnobservedCoordinateStillBlocks() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastConsuming(SCOPE);

        processRecord(causalBroadcast, incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(C2_ID, 0, 3)));

        assertTrue(forwarded.isEmpty(), "an unobserved in-scope dependency must still hold the record");
        assertEquals(1, buffer.size(), "the record must be buffered, not forwarded");
    }

    /**
     * Records pre-loaded into the buffer store before the core is constructed are drained
     * when the frontier subsequently advances to satisfy their dependencies.
     *
     * <p>This simulates recovery: the core restores buffered records from RocksDB on startup,
     * then drains them as satisfying records arrive.
     *
     * Asserts that pre-buffered records are released in causal order and that the frontier
     * does not advance until the dependency arrives.
     */
    @Test
    void recordsAlreadyInTheBufferDrainWhenTheFrontierCatchesUp() {
        buffer.add(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 3)), 0L);
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();
        assertEquals(ParsleyVectorClock.empty(), causalBroadcast.frontier(), "pre-buffered records must not advance the frontier on construction");

        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty()));

        assertEquals(2, forwarded.size(), "satisfying record and pre-buffered record must both be forwarded");
        assertEquals(C1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(C2, tp(forwarded.get(1)), "pre-buffered record must be released second");
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        processRecord(causalBroadcast, incomingRecord(C1, 0, ParsleyVectorClock.empty()));

        assertEquals(1, forwarded.size(), "a trivially-satisfied record must be forwarded");
        assertEquals(ParsleyVectorClock.empty().observe(C1_ID, 0, 0), causalBroadcast.frontier(),
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
            @Override public void recordOutOfScopeIgnored(int coordinates) {}
            @Override public void recordReplaySkipped() {}
            @Override public void recordReflectedClaimAboveOwnOutputs() {}
            @Override public void reportHeldAboveHighestReceived(int count) {}
            @Override public void reportState(int depth, OptionalLong oldest) { reportedDepths.add(depth); }
        };
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, capturing);

        causalBroadcast.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 3)));
        assertEquals(List.of(1), bufferedCounts, "recordBuffered must fire when a record enters the buffer");
        assertEquals(List.of(1), reportedDepths, "reportState must fire with the new buffer depth");
        assertTrue(releasedCounts.isEmpty(), "recordReleased must not fire while record is buffered");

        causalBroadcast.receive(incomingRecord(C1, 3, ParsleyVectorClock.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased must fire with the count of drained records");
        assertEquals(List.of(1, 0), reportedDepths, "reportState must report the post-drain buffer depth");
    }

    /**
     * A replayed record whose offset is at or below the contiguous frontier — an offset this node
     * has already delivered — is skipped, never forwarded to the delegate a second time. This is the
     * receive path's replay skip guard: routine while an added input's re-fetched prefix replays
     * past the carried-ancestry seed ({@code ParsleyChannels#rescope}), and otherwise the
     * fail-safe net for a redelivery {@code exactly_once_v2} should make impossible. The skip
     * is checked first, so a replayed record whose clock names a coordinate now out of scope skips
     * without even counting the ignore metric.
     *
     * Asserts the replay is not forwarded and not buffered, the frontier is unchanged, and the
     * replay-skipped metric fires — including for a replay carrying an out-of-scope dependency.
     */
    @Test
    void alreadyDeliveredReplayIsSkippedNotForwardedAgain() {
        List<Integer> replaySkips = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered() {}
            @Override public void recordReleased(int c) {}
            @Override public void recordDeserializationError() {}
            @Override public void recordClockResolutionError() {}
            @Override public void recordOutOfScopeIgnored(int coordinates) {}
            @Override public void recordReplaySkipped() { replaySkips.add(1); }
            @Override public void recordReflectedClaimAboveOwnOutputs() {}
            @Override public void reportHeldAboveHighestReceived(int count) {}
            @Override public void reportState(int depth, OptionalLong oldest) {}
        };
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyVectorClock.empty(), buffer, new MockCandidateIndex(), forwardedIndex,
                capturing, System::currentTimeMillis);
        processRecord(causalBroadcast, incomingRecord(C1, 0, ParsleyVectorClock.empty()));
        assertEquals(1, forwarded.size(), "precondition: the original delivery forwards once");

        // The same offset again — carrying a dependency on a coordinate this node has no channel
        // for, as a replayed pre-scope-change record legitimately can.
        processRecord(causalBroadcast, incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(C3_ID, 0, 2)));

        assertEquals(1, forwarded.size(),
                "an already-delivered offset must be skipped, never forwarded to the delegate again");
        assertEquals(0, buffer.size(), "a skipped replay must not enter the buffer");
        assertEquals(ParsleyVectorClock.empty().observe(C1_ID, 0, 0), causalBroadcast.frontier(),
                "a skipped replay must not move the frontier");
        assertEquals(List.of(1), replaySkips, "the replay-skipped metric must count the skip");
    }

    /**
     * The skip guard's second clause: a record delivered out of order — above the contiguous
     * frontier, its offset still marked in the forwarded index — is equally "already delivered", so
     * its replay is skipped too. Without the forwarded-index clause a replay of such a record would
     * pass the frontier test (it sits above the watermark) and reach the delegate twice.
     *
     * Asserts the out-of-order-delivered offset's replay is not forwarded again while the earlier
     * offset is still held.
     */
    @Test
    void replayOfAForwardedButUnabsorbedOffsetIsSkipped() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        processRecord(causalBroadcast, incomingRecord(C1, 0, ParsleyVectorClock.empty()));
        // C1@1 held on an unmet dependency; C1@2 then delivers out of order past it.
        processRecord(causalBroadcast, incomingRecord(C1, 1, ParsleyVectorClock.empty().observe(C2_ID, 0, 5)));
        processRecord(causalBroadcast, incomingRecord(C1, 2, ParsleyVectorClock.empty()));
        assertEquals(2, forwarded.size(), "precondition: offsets 0 and 2 delivered, 1 held");
        assertEquals(0L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "precondition: the held offset 1 pins the frontier below the out-of-order 2");

        processRecord(causalBroadcast, incomingRecord(C1, 2, ParsleyVectorClock.empty()));

        assertEquals(2, forwarded.size(),
                "a replay of the out-of-order-delivered offset 2 must be skipped — it is still marked "
                        + "in the forwarded index, so it was already delivered despite sitting above "
                        + "the frontier");
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // Record at C1/0@3 whose dependencies require C1/0@3 — exactly itself.
        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty().observe(C1_ID, 0, 3)));

        assertEquals(1, forwarded.size(), "self-dep is stripped → dependencies empty → forwarded immediately");
        assertEquals(0, buffer.size(), "self-dep record must never enter the buffer");
        assertEquals(ParsleyVectorClock.empty().observe(C1_ID, 0, 3), causalBroadcast.frontier(),
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C2/0@0 has a self-dep on C2_ID/0@0 AND a real dep on C1_ID/0@5.
        // After stripping the self-ref, the effective dependencies are {C1_ID/0@5}: still unsatisfied.
        ParsleyVectorClock mixed = ParsleyVectorClock.empty()
                .observe(C2_ID, 0, 0)
                .observe(C1_ID, 0, 5);
        processRecord(causalBroadcast, incomingRecord(C2, 0, mixed));
        assertTrue(forwarded.isEmpty(), "real dep on C1@5 must still hold the record in the buffer");
        assertEquals(1, buffer.size(), "record must be in the buffer while real dep is unmet");

        // C1@5 arrives — satisfies the real dep; C2 drains.
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty()));
        assertEquals(2, forwarded.size(), "satisfying record and buffered record must both be forwarded");
        assertEquals(C1, tp(forwarded.get(0)), "satisfying record must be forwarded first");
        assertEquals(C2, tp(forwarded.get(1)), "buffered record must be released second");
    }

    /**
     * A dependency on a <em>lower</em> offset of the record's own partition is satisfiable and
     * honoured. Realistically, by the time a record arrives, an earlier offset on its own partition
     * has already been delivered to {@link ParsleyCausalBroadcast#receive} (Kafka guarantees strictly
     * increasing per-partition delivery) — so the interesting case isn't "waiting for it to arrive",
     * it's "waiting for it to actually be forwarded", when that earlier record is itself held on an
     * unrelated dependency.
     *
     * Asserts the dependent record is held until the earlier record's own (unrelated) dependency is
     * satisfied, releasing the earlier record first and the dependent one second.
     */
    @Test
    void holdRecordUntilBackwardSamePartitionDependencyArrives() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C1@3 arrives first (natural Kafka order) but is itself held on an unrelated dependency.
        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty().observe(C2_ID, 0, 0)));
        assertTrue(forwarded.isEmpty(), "C1@3 is held on its own, unrelated, unmet dependency");

        // C1@5 depends on C1@3 — an earlier record on its own partition (backward dep, honoured) —
        // but C1@3 hasn't actually been forwarded yet, only buffered, so C1@5 must wait too.
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C1_ID, 0, 3)));
        assertTrue(forwarded.isEmpty(), "backward same-partition dep must hold C1@5 until C1@3 is actually forwarded");
        assertEquals(2, buffer.size(), "both C1@3 and C1@5 remain held");

        // C2@0 arrives, satisfying C1@3's own dependency — C1@3 releases, which in turn satisfies C1@5.
        processRecord(causalBroadcast, incomingRecord(C2, 0, ParsleyVectorClock.empty()));

        assertEquals(3, forwarded.size(), "C2@0, then C1@3, then C1@5 must all be forwarded");
        assertEquals(0L, forwarded.get(0).offset(), "the satisfying record (C2@0) is forwarded first");
        assertEquals(3L, forwarded.get(1).offset(), "C1@3 releases once its own dependency is met");
        assertEquals(5L, forwarded.get(2).offset(), "C1@5 releases once C1@3 (its backward dependency) is forwarded");
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        Uuid oldC1 = new Uuid(0L, 1L);
        Uuid newC1 = new Uuid(0L, 2L);   // recreated — different UUID, same name + partition

        // C2 depends on c1-0@5 under the OLD incarnation.
        processRecord(causalBroadcast, incomingRecordWithId(C2, 0, C2_ID,
                ParsleyVectorClock.empty().observe(oldC1, 0, 5L)));
        assertTrue(forwarded.isEmpty(), "C2 must be buffered: old-c1 dependency unsatisfied");

        // A record from the NEW c1 incarnation at the same offset must NOT unblock C2.
        processRecord(causalBroadcast, incomingRecordWithId(C1, 5, newC1, ParsleyVectorClock.empty()));
        assertEquals(1, forwarded.size(), "only the new-c1 record must be forwarded; C2 stays buffered");
        assertEquals(C1, tp(forwarded.get(0)), "new-incarnation record must be forwarded");

        // A record from the OLD c1 incarnation arrives — dependency now satisfied.
        processRecord(causalBroadcast, incomingRecordWithId(C1, 5, oldC1, ParsleyVectorClock.empty()));
        assertEquals(3, forwarded.size(), "old-c1 record forwarded, then buffered C2 released");
        assertEquals(C2, tp(forwarded.get(2)), "C2 must be released after old-c1 arrives");
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
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C1@5 is held on an unrelated dependency requiring real progress on C2 (not just C2's
        // baseline — a dependency on C2's very first observed offset would be trivially satisfied
        // the moment any record on C2 is seen at all, which isn't the scenario under test here).
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C2_ID, 0, 10)));
        assertTrue(forwarded.isEmpty(), "C1@5 must be held while its dependency is unmet");

        // A third record depends on exactly C1@5 having been forwarded.
        processRecord(causalBroadcast, incomingRecord(C2, 1, ParsleyVectorClock.empty().observe(C1_ID, 0, 5)));
        assertTrue(forwarded.isEmpty(), "C2@1 must be held: C1@5 has not actually been forwarded yet");

        // C1@6 has independently-satisfied (empty) deps and forwards immediately.
        processRecord(causalBroadcast, incomingRecord(C1, 6, ParsleyVectorClock.empty()));

        assertEquals(List.of(6L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "C1@6 forwards on its own");
        assertEquals(4L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "the frontier must stall one below C1@5, never leapfrogging past the still-held record");
        assertEquals(2, buffer.size(),
                "C1@5 and C2@1 (depending on it) must both remain held — C1@6 forwarding must not release either");
    }

    /**
     * Once the held record at the bottom of a gap is released, the frontier jumps forward in a
     * single step to absorb every already-forwarded record above it — it does not need those later
     * records to arrive again, since they are already sitting in the forwarded index.
     *
     * Asserts that releasing C1@5 advances the frontier all the way to C1@8 (which already
     * forwarded while C1@5 was held) in one step, and that a record depending on C1@8 releases too.
     */
    @Test
    void releasingTheHeldRecordCatchesUpThroughEverythingAlreadyForwardedAboveIt() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C1@5 is held on an unrelated dependency.
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C2_ID, 0, 0)));

        // C1@6, C1@7, C1@8 each forward immediately (independently-satisfied), piling up above the gap.
        processRecord(causalBroadcast, incomingRecord(C1, 6, ParsleyVectorClock.empty()));
        processRecord(causalBroadcast, incomingRecord(C1, 7, ParsleyVectorClock.empty()));
        processRecord(causalBroadcast, incomingRecord(C1, 8, ParsleyVectorClock.empty()));
        assertEquals(4L, causalBroadcast.frontier().offsetFor(C1_ID, 0), "frontier stalls below C1@5 the whole time");
        forwarded.clear();

        // C1@9 depends on C1@8 (its own immediate predecessor) — held, since the frontier hasn't
        // actually reached 8 yet (it is stuck behind the still-held C1@5).
        processRecord(causalBroadcast, incomingRecord(C1, 9, ParsleyVectorClock.empty().observe(C1_ID, 0, 8)));
        assertTrue(forwarded.isEmpty(), "C1@9 must be held: the frontier hasn't reached 8 yet");
        assertEquals(2, buffer.size(), "C1@5 and C1@9 both remain held");
        forwarded.clear();

        // C2@0 satisfies C1@5's own dependency — releasing it closes the gap, and the frontier
        // should catch up through 6, 7, and 8 in one step, releasing C1@9 too.
        processRecord(causalBroadcast, incomingRecord(C2, 0, ParsleyVectorClock.empty()));

        assertEquals(9L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "the frontier must jump straight to 9, absorbing every already-forwarded record above the gap");
        assertEquals(List.of(0L, 5L, 9L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "C2@0 forwards, releasing C1@5, which in turn releases C1@9");
        assertTrue(forwardedIndex.forwardedAfter(C1_ID, 0, 4).isEmpty(),
                "every absorbed offset (5-9) must be pruned from the forwarded index, not left to "
                        + "accumulate — it is no longer needed once folded into the frontier");
    }

    /**
     * When the contiguous frontier jumps from one value to a much higher one in a single step,
     * every buffered record whose dependency falls <em>anywhere</em> in that advanced range must
     * be released in the same cascade — not only records waiting on exactly the new boundary.
     *
     * <p>C1@5 holds the gap while C1@6–C1@12 pile up in the forwarded index. Five records each
     * wait on a different C1/0 offset inside that range (8, 9, 10, 11, 12). When C2@0 closes the
     * gap and releases C1@5, the frontier walks from 4 to 12 in one step, and the candidate-index
     * scan of {@code [0, 12]} must find all five — not just the record waiting on 12.
     *
     * <p>Asserts all five are forwarded in the same cascade, without any eviction trigger, and
     * the buffer is empty afterward.
     */
    @Test
    void frontierJumpReleasesAllWaitingRecordsAcrossTheJumpedRange() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C1@5 holds the gap; C1's baseline is seeded to 4 (offset - 1).
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C2_ID, 0, 0)));
        assertEquals(4L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "baseline must seed C1/0 frontier at offset - 1 = 4");

        // C1@6–C1@12 forward immediately, piling up in the forwarded index above the gap.
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(causalBroadcast, incomingRecord(C1, offset, ParsleyVectorClock.empty()));
        }
        assertEquals(4L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "frontier must stall at 4 while the gap at C1@5 remains open");

        // Five records, each waiting on a distinct C1/0 offset spanning the entire jump range.
        processRecord(causalBroadcast, incomingRecord(C1, 13, ParsleyVectorClock.empty().observe(C1_ID, 0, 8)));
        processRecord(causalBroadcast, incomingRecord(C1, 14, ParsleyVectorClock.empty().observe(C1_ID, 0, 9)));
        processRecord(causalBroadcast, incomingRecord(C1, 15, ParsleyVectorClock.empty().observe(C1_ID, 0, 10)));
        processRecord(causalBroadcast, incomingRecord(C1, 16, ParsleyVectorClock.empty().observe(C1_ID, 0, 11)));
        processRecord(causalBroadcast, incomingRecord(C1, 17, ParsleyVectorClock.empty().observe(C1_ID, 0, 12)));
        assertEquals(6, buffer.size(), "C1@5 and C1@13–C1@17 must all be held before the trigger");
        forwarded.clear();

        // C2@0 closes the gap: C1@5 releases, the frontier walks to 12 in one step, and all five
        // waiting records must be released in the same cascade — none left for eviction to handle.
        processRecord(causalBroadcast, incomingRecord(C2, 0, ParsleyVectorClock.empty()));

        assertEquals(
                List.of(0L, 5L, 13L, 14L, 15L, 16L, 17L),
                forwarded.stream().map(ParsleyMessage::offset).toList(),
                "after the 4→12 frontier jump every record waiting in the range 8–12 must be "
                        + "released eagerly — range scan must cover all newly-satisfied offsets, not just 12");
        assertEquals(17L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "frontier must advance through the full release cascade to 17");
        assertEquals(0, buffer.size(),
                "buffer must be empty — all releases must be driven by the frontier advance, not eviction");
    }

    /**
     * A buffered record whose required dependency offset is strictly inside (neither at the bottom
     * nor the top of) a frontier jump range must be released in the same cascade as the jump —
     * the candidate-index range scan must cover the whole range, not just the exact new boundary.
     *
     * <p>The frontier jumps 4 → 12 when C2@0 releases C1@5, but the single waiting record depends
     * on C1/0 at offset 10, which is interior to the jumped range. It must be released immediately
     * in that cascade without any eviction trigger.
     *
     * <p>Asserts the intermediate-offset record is forwarded and the buffer is empty afterward.
     */
    @Test
    void frontierJumpReleasesRecordWaitingOnIntermediateOffsetNotJustFinalOffset() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C1@5 holds the gap; C1@6–C1@12 pile up in the forwarded index above it.
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C2_ID, 0, 0)));
        for (long offset = 6; offset <= 12; offset++) {
            processRecord(causalBroadcast, incomingRecord(C1, offset, ParsleyVectorClock.empty()));
        }
        assertEquals(4L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "frontier must stall at 4 until the gap is closed");

        // One record waiting on offset 10 — strictly inside the 4→12 jump range.
        processRecord(causalBroadcast, incomingRecord(C1, 13, ParsleyVectorClock.empty().observe(C1_ID, 0, 10)));
        assertEquals(2, buffer.size(), "C1@5 and C1@13 must both be held");
        forwarded.clear();

        // C2@0 closes the gap; frontier jumps 4→12. C1@13 (waiting on 10, not 12) must be found.
        processRecord(causalBroadcast, incomingRecord(C2, 0, ParsleyVectorClock.empty()));

        assertEquals(
                List.of(0L, 5L, 13L),
                forwarded.stream().map(ParsleyMessage::offset).toList(),
                "C1@13 depends on C1/0 offset 10 (interior to the 4→12 jump) and must be released "
                        + "eagerly — offset 10 must be found by the range scan, not missed because "
                        + "the frontier landed on 12");
        assertEquals(0, buffer.size(),
                "buffer must be empty — the intermediate-offset record must not remain until eviction");
    }

    /**
     * The forwarded index is itself durable (restored from its own changelog, like the buffer and
     * candidate index already are), so a restart never loses track of offsets forwarded ahead of a
     * still-open gap — simulated here by handing a fresh core instance the same forwarded-index
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

        ParsleyCausalBroadcast<String, String> first = ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        // C1@5 is held; C1@6, C1@7, C1@8 each forward immediately, piling up above the gap.
        first.receive(incomingRecord(C1, 5, ParsleyVectorClock.empty().observe(C2_ID, 0, 0)));
        first.receive(incomingRecord(C1, 6, ParsleyVectorClock.empty()));
        first.receive(incomingRecord(C1, 7, ParsleyVectorClock.empty()));
        first.receive(incomingRecord(C1, 8, ParsleyVectorClock.empty()));
        ParsleyVectorClock persistedFrontier = first.frontier();
        assertEquals(4L, persistedFrontier.offsetFor(C1_ID, 0),
                "frontier persisted at the gap, as it would be before a crash");

        // Simulate a restart: a brand-new core instance, seeded with the persisted frontier and a
        // buffer restored from the changelog (still holding C1@5), and the SAME forwarded-index
        // contents (standing in for "restored from its own changelog") — no separate "ceiling"
        // value is needed; the forwarded index alone remembers that 6, 7, and 8 already went out.
        ParsleyCausalBroadcast<String, String> restarted = ParsleyTestFixtures.broadcast(persistedFrontier, sharedBuffer, new MockCandidateIndex(),
                sharedForwardedIndex, ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> released =
                restarted.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty())).delivered();

        assertEquals(8L, restarted.frontier().offsetFor(C1_ID, 0),
                "the restarted instance must still catch up through 6, 7, and 8 via the surviving forwarded index");
        assertEquals(List.of(0L, 5L), released.stream().map(ParsleyMessage::offset).toList(),
                "C2@0 forwards and releases C1@5 on the restarted instance");
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
     * <p>Asserts the "crashed" instance's frontier already reflects C2@0 as delivered even though it
     * is still sitting in the buffer, and that a "restarted" instance redelivers it (a duplicate)
     * rather than wedging forever.
     */
    @Test
    void aCrashBetweenFrontierPersistAndBufferRemovalRedeliversAsAHarmlessDuplicateNeverAWedge() {
        SwallowingRemoveBufferStore<String, String> crashyBuffer = new SwallowingRemoveBufferStore<>(0L);

        ParsleyCausalBroadcast<String, String> beforeCrash = ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), crashyBuffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP);

        // C2@0 depends on C1@5 and is held (sequence 0 in the buffer).
        beforeCrash.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 5)));
        assertEquals(1, crashyBuffer.size(), "C2@0 must be held");

        // C1@5 satisfies it: propagate() releases C2@0, persisting the frontier advance before its
        // (swallowed) buffer removal — simulating a crash landing in that exact window.
        List<ParsleyMessage<String, String>> releasedBeforeCrash =
                beforeCrash.receive(incomingRecord(C1, 5, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of(5L, 0L), releasedBeforeCrash.stream().map(ParsleyMessage::offset).toList(),
                "both C1@5 and C2@0 are delivered in-process before the simulated crash");
        assertEquals(0L, beforeCrash.frontier().offsetFor(C2_ID, 0),
                "the frontier already recorded C2@0 as delivered — persisted before the swallowed removal");
        assertEquals(1, crashyBuffer.size(),
                "C2@0's buffer removal never landed (the simulated crash), so it is still sitting in "
                        + "the buffer");

        // "Restart": a fresh core over the persisted (torn) frontier and a normal buffer store
        // standing in for the buffer changelog restoring the same still-held record.
        ParsleyVectorClock persistedFrontier = beforeCrash.frontier();
        MockBufferStore<String, String> restoredBuffer = new MockBufferStore<>();
        restoredBuffer.add(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 5)), 0L);

        ParsleyCausalBroadcast<String, String> restarted = ParsleyTestFixtures.broadcast(persistedFrontier, restoredBuffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP);

        List<ParsleyMessage<String, String>> releasedAfterRestart = restarted.drainAfterRestore().delivered();

        assertEquals(List.of(0L), releasedAfterRestart.stream().map(ParsleyMessage::offset).toList(),
                "the restarted instance redelivers C2@0 — a harmless at-least-once duplicate — rather "
                        + "than wedging forever");
        assertEquals(0, restoredBuffer.size(), "C2@0 must finally leave the buffer after the redelivery");
    }

    /**
     * Establishing the baseline for a coordinate this core has never observed before can itself
     * release an already-buffered record — before the very record that triggered the baseline seed
     * is even dispositioned by its own dominates check.
     *
     * Asserts that C2@0, depending on a coordinate the core has never seen, is released as a
     * direct effect of C1@5 establishing that coordinate's baseline — and is forwarded ahead of
     * C1@5 itself in the returned order.
     */
    @Test
    void establishingTheBaselineForAFirstSeenCoordinateCanItselfReleaseAWaitingRecord() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();

        // C2@0 depends on C1_ID/0@4 — a coordinate this core has never observed at all yet.
        processRecord(causalBroadcast, incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 4)));
        assertTrue(forwarded.isEmpty(), "C2@0 must be held: C1/0 has never been observed");

        // C1@5 is the very first record this core ever sees on C1/0. Establishing its baseline
        // (frontier = 4) is itself enough to satisfy C2@0's dependency — before C1@5 is even
        // dispositioned by its own dominates check.
        processRecord(causalBroadcast, incomingRecord(C1, 5, ParsleyVectorClock.empty()));

        assertEquals(List.of(0L, 5L), forwarded.stream().map(ParsleyMessage::offset).toList(),
                "establishing C1's baseline must release C2@0 before C1@5 itself forwards");
        assertEquals(0, buffer.size(), "both records must have left the buffer");
    }

    /**
     * The baseline seed must never re-fire once the persisted frontier already reflects real
     * progress on a coordinate — even progress as low as offset 0 exactly. A restarted core
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
        ParsleyVectorClock restoredFrontier = ParsleyVectorClock.empty().observe(C1_ID, 0, 0);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(restoredFrontier, buffer, new MockCandidateIndex(),
                forwardedIndex, ParsleyMetrics.NOOP);

        // The first record this (restarted) instance ever sees on C1/0 is offset 10 — far above
        // the restored frontier of 0. There is a real, unaccounted-for gap from 1 through 9.
        processRecord(causalBroadcast, incomingRecord(C1, 10, ParsleyVectorClock.empty()));

        assertEquals(0L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "the frontier must stay at the restored value (0); it must not be corrupted into "
                        + "treating the unaccounted-for gap from 1-9 as moot just because "
                        + "seenCoordinates was fresh");
    }

    /**
     * Restart regression: the "coordinate marked seen even if the record is held" guard must survive
     * a restart. Pre-crash, C1@0 is held (deps on a third coordinate that never arrives before the
     * crash), so nothing on C1 was ever delivered and the persisted frontier has NO entry for C1 (the
     * offset-0 seed is a no-op); a record d on C2 depending on C1@0 is held too — correctly, since
     * its cause has not been delivered. After a restart (a fresh core over the restored buffer,
     * with a fresh in-memory seen-set), the first new record on C1 arrives at offset 5. Without the
     * core constructor re-marking C1 seen from the restored buffer, the baseline seed would fold
     * offsets 0-4 into the frontier as "outside the core's purview" and release d before its
     * still-held cause C1@0 — an effect-before-cause delivery of exactly the kind the library exists
     * to prevent.
     *
     * Asserts the post-restart record does not re-trigger the seed (the C1 frontier stays at -1, the
     * gap at 0 intact), d stays held, and the whole chain then drains in causal order — d only after
     * C1@0 — once the third coordinate finally delivers.
     */
    @Test
    void restartDoesNotReSeedPastAStillHeldRecordAndReleaseItsDependents() {
        TopicPartition c3 = new TopicPartition("c3", 0);
        MockBufferStore<String, String> sharedBuffer = new MockBufferStore<>();
        MockForwardedIndex sharedIndex = new MockForwardedIndex();
        ParsleyCausalBroadcast<String, String> first = ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), sharedBuffer,
                new MockCandidateIndex(), sharedIndex, ParsleyMetrics.NOOP);

        // C1@0 held on C3@0 (never arriving pre-crash); d = C2@0 held on its cause C1@0.
        first.receive(incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(C3_ID, 0, 0)));
        first.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 0)));
        assertEquals(2, sharedBuffer.size(), "both records must be held before the crash");
        assertEquals(-1L, first.frontier().offsetFor(C1_ID, 0),
                "nothing on C1 was delivered, so the persisted frontier must have no C1 entry");

        // Restart: fresh core (fresh seen-set) over the restored buffer and persisted frontier.
        ParsleyCausalBroadcast<String, String> restarted = ParsleyTestFixtures.broadcast(first.frontier(), sharedBuffer,
                new MockCandidateIndex(), sharedIndex, ParsleyMetrics.NOOP);

        // The first post-restart record on C1 arrives at offset 5, dependency-free.
        List<ParsleyMessage<String, String>> onC1At5 =
                restarted.receive(incomingRecord(C1, 5, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of(5L), onC1At5.stream().map(ParsleyMessage::offset).toList(),
                "only C1@5 itself may deliver; d must stay held because its cause C1@0 is still held");
        assertEquals(-1L, restarted.frontier().offsetFor(C1_ID, 0),
                "the baseline seed must not re-fire past the still-held C1@0: the contiguous frontier "
                        + "keeps the gap at offset 0 open");
        assertEquals(2, sharedBuffer.size(), "C1@0 and d must both still be held");

        // C3@0 finally arrives: C1@0 releases, then d — cause strictly before effect.
        List<ParsleyMessage<String, String>> drained = restarted.receive(
                incomingRecordWithId(c3, 0, C3_ID, ParsleyVectorClock.empty())).delivered();

        assertEquals(List.of("c3", "c1", "c2"), drained.stream().map(ParsleyMessage::topic).toList(),
                "the chain must drain in causal order: C3@0, then C1@0, then its dependent d");
        assertEquals(0, sharedBuffer.size(), "everything must have left the buffer");
    }

    /**
     * A poison record (undecodable on the forward path) always fails the task — the regression guard for
     * the mutation-before-throw hazard: the buffer must not be touched before the throw.
     *
     * Asserts {@code receive} throws {@link CausalBufferDeserializationException} and the poisoned
     * record remains in the buffer (not removed).
     */
    @Test
    void poisonFailsTheTask() {
        TopicPartition c4 = new TopicPartition("c4", 0);
        Uuid c4Id = Uuid.randomUuid();
        PoisonableBufferStore<String, String> buffer = new PoisonableBufferStore<>();
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);

        ParsleyVectorClock needsC4 = ParsleyVectorClock.empty().observe(c4Id, 0, 0);
        causalBroadcast.receive(incomingRecordWithId(C1, 5, C1_ID, needsC4));
        buffer.poison(0L); // the only sequence added so far

        assertThrows(CausalBufferDeserializationException.class,
                () -> causalBroadcast.receive(incomingRecordWithId(c4, 0, c4Id, ParsleyVectorClock.empty())),
                "a poison record on the forward path must fail the task");
        assertEquals(1, buffer.size(), "the poisoned record must remain in the buffer for recovery, not be removed");
    }

    /**
     * The two-branch gate's ignore branch: a dependency on a coordinate this node does not
     * consume — an undeclared topic, or a partition another task owns — is ignored,
     * unconditionally. With transitively complete stamps carried by unconditional merges,
     * any consumed causal ancestor of the record is claimed directly in the record's own
     * clock, so the unconsumed entry only proxies ancestry the clock already states. Failing the
     * task on such a coordinate instead would add no safety and would make a join require
     * coordination.
     *
     * Asserts the record delivers immediately, is never buffered, and each ignored coordinate is
     * counted by the out-of-scope-ignored metric.
     */
    @Test
    void outOfScopeDependenciesAreIgnoredAndTheRecordDelivers() {
        List<Integer> ignoredCounts = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered() {}
            @Override public void recordReleased(int c) {}
            @Override public void recordDeserializationError() {}
            @Override public void recordClockResolutionError() {}
            @Override public void recordOutOfScopeIgnored(int coordinates) { ignoredCounts.add(coordinates); }
            @Override public void recordReplaySkipped() {}
            @Override public void recordReflectedClaimAboveOwnOutputs() {}
            @Override public void reportHeldAboveHighestReceived(int count) {}
            @Override public void reportState(int depth, OptionalLong oldest) {}
        };
        // Only C1 is consumed; C2 and C3 are coordinates this node has no channel for at all.
        ParsleyVectorClock.CoordinatePredicate onlyC1 = (topicId, partition) -> partition == 0 && topicId.equals(C1_ID);
        MockBufferStore<String, String> localBuffer = new MockBufferStore<>();
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex()),
                localBuffer, new MockCandidateIndex(), capturing, System::currentTimeMillis, onlyC1);

        ParsleyVectorClock needsC2AndC3 =
                ParsleyVectorClock.empty().observe(C2_ID, 0, 0).observe(C3_ID, 0, 4);
        ParsleyCausalBroadcast.Outcome<String, String> outcome =
                causalBroadcast.receive(incomingRecord(C1, 0, needsC2AndC3));

        assertEquals(1, outcome.delivered().size(),
                "dependencies on unconsumed coordinates must fall to the ignore branch, not gate "
                        + "or fail the record");
        assertEquals(0, localBuffer.size(), "a record held back by nothing consumed must never be buffered");
        assertEquals(List.of(2), ignoredCounts,
                "every ignored coordinate must count the out-of-scope-ignored metric, once per received record");
    }

    /**
     * The ignore branch never bleeds into the consumed branch: a record claiming both an
     * unconsumed coordinate (ignored) and an unmet consumed coordinate is still held on the
     * consumed one — the dispatch is per coordinate, not per record — and releases the moment the
     * consumed cause is locally delivered.
     *
     * Asserts the record is held despite the ignored coordinate, then delivers once the consumed
     * dependency is satisfied.
     */
    @Test
    void ignoredCoordinateNeverSatisfiesTheConsumedBranch() {
        // C1 and C2 are consumed; C3 is a coordinate this node has no channel for.
        ParsleyVectorClock.CoordinatePredicate scope = (topicId, partition) ->
                partition == 0 && (topicId.equals(C1_ID) || topicId.equals(C2_ID));
        MockBufferStore<String, String> localBuffer = new MockBufferStore<>();
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex()),
                localBuffer, new MockCandidateIndex(), ParsleyMetrics.NOOP, System::currentTimeMillis, scope);

        // C3@9 is unconsumed (ignored); C2@0 is consumed and not yet delivered here (gates).
        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(C3_ID, 0, 9).observe(C2_ID, 0, 0);
        ParsleyCausalBroadcast.Outcome<String, String> held =
                causalBroadcast.receive(incomingRecord(C1, 0, deps));
        assertEquals(0, held.delivered().size(),
                "the unmet consumed dependency must hold the record — ignoring C3 must not admit it");
        assertEquals(1, localBuffer.size(), "the held record must be buffered on the consumed branch");

        ParsleyCausalBroadcast.Outcome<String, String> released =
                causalBroadcast.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty()));
        assertEquals(2, released.delivered().size(),
                "local delivery of the consumed cause must release the held record");
    }

    // --- helpers --------------------------------------------------------------------------------

    // These helpers build an core over an untracked in-memory frontier — vectorTime() is the node's
    // own frontier — exercising the frontier/buffer mechanics in isolation. The cross-channel
    // merge layer is covered by ParsleyCausalBroadcastVectorTimeTest.

    /**
     * {@code broadcast()} — the single stamping site — runs the crossing wait, drains the bound
     * acknowledged-outputs source into the {@code ownOutputs} clock ("folded before each stamp"),
     * and attaches the node's vector time, own outputs included: the acked sink
     * coordinate must appear both in {@code ownOutputs()} and in the attached dependency header,
     * and the crossing wait must have been invoked before the stamp was read.
     */
    @Test
    void broadcastWaitsFoldsAndStampsTheVectorTime() {
        Uuid sinkId = Uuid.randomUuid();
        List<Set<TopicPartition>> waits = new ArrayList<>();
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        channels.bindOwnOutputSource(consumer -> consumer.accept("c5", 0, 11),
                (except, timeoutMs) -> waits.add(except), Map.of("c5", sinkId), 1_000L);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);
        causalBroadcast.receive(incomingRecord(C1, 3, ParsleyVectorClock.empty()));

        Record<String, String> stamped =
                causalBroadcast.broadcast(new Record<>("k", "v", 0L, ParsleyHeader.mutableHeaders()));

        assertEquals(List.of(Set.<TopicPartition>of()), waits,
                "a business broadcast must run the crossing wait with the conservative empty "
                        + "exclusion (its destination partition is unknowable at stamp time)");
        assertEquals(11L, channels.ownOutputs().offsetFor(sinkId, 0),
                "broadcast must fold the pending ack into ownOutputs before stamping");
        byte[] stampBytes = stamped.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK).value();
        ParsleyVectorClock stamp = ParsleyVectorClock.fromBytes(stampBytes);
        assertEquals(causalBroadcast.vectorTime(), stamp,
                "the stamp must be the node's vector time, own outputs included");
        assertEquals(11L, stamp.offsetFor(sinkId, 0),
                "the acked own-output coordinate must ride the stamp — the #22 fix");
    }

    /**
     * The never-stamp-and-proceed rule at the stamping site: a crossing wait that throws (timeout
     * or observed ack
     * failure) propagates out of {@code broadcast()} — the record is never stamped, so the EOS
     * transaction dies rather than carry a potentially under-claiming clock.
     */
    @Test
    void broadcastPropagatesACrossingWaitFailureWithoutStamping() {
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        channels.bindOwnOutputSource(consumer -> { },
                (except, timeoutMs) -> {
                    throw new CausalPendingAckException("pending ack failed (test)");
                }, Map.of(), 1_000L);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);

        assertThrows(CausalPendingAckException.class,
                () -> causalBroadcast.broadcast(new Record<>("k", "v", 0L, ParsleyHeader.mutableHeaders())),
                "a failed crossing wait must fail the broadcast — never stamp-and-proceed");
    }

    /**
     * A marker broadcast passes its exact destination set to the crossing wait — every sink at the
     * task's own partition — where a business broadcast passes the conservative empty set (see
     * {@code broadcastWaitsFoldsAndStampsCompletenessUnionOwnOutputs}): same-coordinate pending
     * sends are covered by partition FIFO plus per-producer stamp monotonicity, and the cross-sink
     * exemption for a fanned-out marker is safe because its destinations are known exactly.
     */
    @Test
    void markerBroadcastExcludesItsDestinationsFromTheCrossingWait() {
        List<Set<TopicPartition>> waits = new ArrayList<>();
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        channels.bindOwnOutputSource(consumer -> { },
                (except, timeoutMs) -> waits.add(except), Map.of(), 1_000L);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);

        Set<TopicPartition> destinations = Set.of(new TopicPartition("c6", 3), new TopicPartition("c7", 3));
        causalBroadcast.broadcast(new Record<>("k", "v", 0L, ParsleyHeader.mutableHeaders()), destinations);

        assertEquals(List.of(destinations), waits,
                "the marker's destination set must be excluded from its crossing wait");
    }

    /**
     * A reflected own-sink claim is ordinary ancestry under the two-branch gate: when the
     * sink is not consumed here it falls to the ignore branch — the record delivers — while the
     * claim still folds into the delivered record's channel clock unstripped and rides the
     * outbound vector time, the custody chain a third party consuming the shared sink gates on
     * (#22).
     */
    @Test
    void ownSinkClaimInADeliveredRecordsClockRidesTheStampUnstripped() {
        Uuid sinkId = Uuid.randomUuid();
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis,
                (topicId, partition) -> partition == 0 && topicId.equals(C1_ID),
                (topicId, partition) -> topicId.equals(sinkId));

        ParsleyCausalBroadcast.Outcome<String, String> outcome = causalBroadcast.receive(
                incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(sinkId, 0, 5)));

        assertEquals(1, outcome.delivered().size(),
                "the reflected claim on an unconsumed own sink must fall to the ignore branch and "
                        + "deliver the record");
        assertEquals(5L, causalBroadcast.vectorTime().offsetFor(sinkId, 0),
                "the own-sink claim must fold into the advertised channel clock and ride the "
                        + "stamp — stripping it erased a real ancestor for third parties (#22)");
    }

    /**
     * The shared-sink blindspot: a
     * node that <em>consumes</em> its own sink topic genuinely gates a claim about that sink —
     * another producer's record on the shared topic is a real, possibly unseen cause, so stripping
     * own-sink coordinates at the gate would satisfy exactly this claim vacuously. The record must
     * hold until this node has
     * itself delivered the claimed sink offset, then release.
     */
    @Test
    void consumedOwnSinkClaimIsGenuinelyGatedNotVacuouslySatisfied() {
        Uuid sinkId = Uuid.randomUuid();
        TopicPartition sink = new TopicPartition("c8", 0);
        MockBufferStore<String, String> localBuffer = new MockBufferStore<>();
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex()),
                localBuffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis,
                (topicId, partition) -> partition == 0 && (topicId.equals(C1_ID) || topicId.equals(sinkId)),
                (topicId, partition) -> topicId.equals(sinkId));

        // A C1 record claims a sibling producer's record at sink@0, not yet delivered here.
        ParsleyCausalBroadcast.Outcome<String, String> held = causalBroadcast.receive(
                incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(sinkId, 0, 0)));
        assertEquals(0, held.delivered().size(),
                "a claim about another producer's record on the consumed shared sink must gate, "
                        + "never be vacuously satisfied (finding (iii))");
        assertEquals(1, localBuffer.size(), "the held effect must be buffered until its cause is delivered");

        ParsleyCausalBroadcast.Outcome<String, String> released = causalBroadcast.receive(
                incomingRecordWithId(sink, 0, sinkId, ParsleyVectorClock.empty()));
        assertEquals(2, released.delivered().size(),
                "local delivery of the shared-sink cause must release the held effect, in cause-first order");
    }

    /**
     * The over-claim diagnostic: an inbound clock claiming one of this node's own sink coordinates
     * ABOVE
     * the ownOutputs clock counts the reflected-claim metric — the own-output view is stale or the
     * peer's stamp untruthful; worth seeing, never a failure — while a claim at or below it (the
     * truthful reflection) counts nothing.
     */
    @Test
    void reflectedClaimAboveOwnOutputsCountsTheDiagnosticMetric() {
        Uuid sinkId = Uuid.randomUuid();
        List<Integer> reflectedCounts = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered() {}
            @Override public void recordReleased(int c) {}
            @Override public void recordDeserializationError() {}
            @Override public void recordClockResolutionError() {}
            @Override public void recordOutOfScopeIgnored(int coordinates) {}
            @Override public void recordReplaySkipped() {}
            @Override public void recordReflectedClaimAboveOwnOutputs() { reflectedCounts.add(1); }
            @Override public void reportHeldAboveHighestReceived(int count) {}
            @Override public void reportState(int depth, OptionalLong oldest) {}
        };
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        channels.acknowledge(sinkId, 0, 7);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), capturing,
                System::currentTimeMillis, (topicId, partition) -> true,
                (topicId, partition) -> topicId.equals(sinkId));

        causalBroadcast.receive(incomingRecord(C1, 0, ParsleyVectorClock.empty().observe(sinkId, 0, 7)));
        assertEquals(List.of(), reflectedCounts,
                "a reflected claim at or below ownOutputs is the truthful case — no diagnostic");

        causalBroadcast.receive(incomingRecord(C1, 1, ParsleyVectorClock.empty().observe(sinkId, 0, 9)));
        assertEquals(List.of(1), reflectedCounts,
                "a reflected claim above ownOutputs must count the diagnostic metric");
    }

    /**
     * The stalled-dependency scan: a record held past the threshold on a dependency ABOVE its
     * channel's highest physically received offset (nothing received so far can satisfy the claim
     * — an aborted tail, a dead producer) is counted; a record held on a dependency at or below
     * the highest received (its cause arrived and is merely still held) is not; and nothing counts
     * before the threshold elapses.
     */
    @Test
    void heldDependencyStallScanCountsOnlyClaimsAboveHighestReceivedPastTheThreshold() {
        List<Integer> stallCounts = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered() {}
            @Override public void recordReleased(int c) {}
            @Override public void recordDeserializationError() {}
            @Override public void recordClockResolutionError() {}
            @Override public void recordOutOfScopeIgnored(int coordinates) {}
            @Override public void recordReplaySkipped() {}
            @Override public void recordReflectedClaimAboveOwnOutputs() {}
            @Override public void reportHeldAboveHighestReceived(int count) { stallCounts.add(count); }
            @Override public void reportState(int depth, OptionalLong oldest) {}
        };
        AtomicLong now = new AtomicLong(0);
        ParsleyChannels channels = ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), forwardedIndex);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), capturing, now::get);

        // C1@5 delivers (frontier and highest received at 5); C1@8 arrives with an unsatisfiable
        // foreign dependency and is held — the bridge folds the skipped 6..7, so highest received
        // on C1 is 8 while the record itself waits on C3@0, a channel never received on (a stall).
        causalBroadcast.receive(incomingRecord(C1, 5, ParsleyVectorClock.empty()));
        causalBroadcast.receive(incomingRecord(C1, 8, ParsleyVectorClock.empty().observe(C3_ID, 0, 0)));
        // C2@0 waits on C1@8 — at or below C1's highest received: its cause physically arrived and
        // is merely held, NOT a stall. C2@1 waits on C1@11 — above it: a genuine stall.
        causalBroadcast.receive(incomingRecord(C2, 0, ParsleyVectorClock.empty().observe(C1_ID, 0, 8)));
        causalBroadcast.receive(incomingRecord(C2, 1, ParsleyVectorClock.empty().observe(C1_ID, 0, 11)));

        causalBroadcast.reportHeldDependencyStalls(1_000L);
        assertEquals(List.of(0), stallCounts,
                "nothing may count as stalled before the threshold has elapsed");

        now.set(1_001L);
        causalBroadcast.reportHeldDependencyStalls(1_000L);
        assertEquals(List.of(0, 2), stallCounts,
                "exactly the two records waiting on never-received positions (C1@8 on C3@0, C2@1 "
                        + "on C1@11) must count — the record whose cause arrived but is still held "
                        + "(C2@0 on C1@8) must not");
    }

    /**
     * The input-side sibling of the own-output gap that folding own outputs into the stamp closes:
     * a record delivered out of order above
     * a contiguous-frontier gap (c1:3 held on an unseen c2 dependency; c1:4 from another producer
     * delivers past it) must be claimed by the stamp {@code broadcast()} attaches — the delegate
     * has seen c1:4, so any output emitted now is causally after it, and a downstream consumer of
     * both the sink and c1 gates only on the stamp. Before the {@code highestDelivered} repair the
     * stamp claimed only the contiguous prefix (c1@2), so the derived output could be delivered
     * downstream before its cause c1:4.
     *
     * Asserts the broadcast stamp dominates the out-of-order-delivered coordinate while the gate's
     * frontier stays below the gap.
     */
    @Test
    void broadcastStampClaimsARecordDeliveredAboveTheContiguousFrontierGap() {
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastWith();
        for (long offset = 0; offset <= 2; offset++) {
            processRecord(causalBroadcast, incomingRecord(C1, offset, ParsleyVectorClock.empty()));
        }
        processRecord(causalBroadcast, incomingRecord(C1, 3, ParsleyVectorClock.empty().observe(C2_ID, 0, 9)));
        processRecord(causalBroadcast, incomingRecord(C1, 4, ParsleyVectorClock.empty()));
        assertEquals(4, forwarded.size(), "c1:0..2 and the out-of-order c1:4 must have been delivered");
        assertEquals(4L, forwarded.get(3).offset(), "the out-of-order delivery must be c1:4");

        Record<String, String> stamped = causalBroadcast.broadcast(new Record<>("k", "v", 0L));

        ParsleyVectorClock stamp = ParsleyVectorClock.fromBytes(
                stamped.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK).value());
        assertEquals(2L, causalBroadcast.frontier().offsetFor(C1_ID, 0),
                "the gate's contiguous frontier must stay below the gap at c1:3");
        assertTrue(stamp.dominates(ParsleyVectorClock.empty().observe(C1_ID, 0, 4)),
                "the outbound stamp must claim the delivered record c1:4 — an output emitted after "
                        + "its delivery is causally after it");
    }

    private ParsleyCausalBroadcast<String, String> causalBroadcastWith() {
        return ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyCausalBroadcast<String, String> causalBroadcastConsuming(ParsleyVectorClock.CoordinatePredicate inScope) {
        return ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyCausalBroadcast<String, String> causalBroadcastWithClock(java.util.function.LongSupplier clock) {
        return ParsleyTestFixtures.broadcast(ParsleyVectorClock.empty(), buffer,
                new MockCandidateIndex(), forwardedIndex, ParsleyMetrics.NOOP, clock);
    }

    private void processRecord(ParsleyCausalBroadcast<String, String> causalBroadcast, ParsleyMessage<String, String> message) {
        forwarded.addAll(causalBroadcast.receive(message).delivered());
    }

    private static TopicPartition tp(ParsleyMessage<String, String> m) {
        return new TopicPartition(m.topic(), m.partition());
    }

    private static Uuid idFor(TopicPartition tp) {
        if (C1.topic().equals(tp.topic())) return C1_ID;
        if (C2.topic().equals(tp.topic())) return C2_ID;
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
     * eviction guard short-circuits before consulting the buffer's index at all (beyond the core
     * constructor's own one-time wait-index rebuild scan), rather than relying on a downstream
     * computation that happens to converge on the same (empty) result.
     */
    private static final class CountingIndexEntriesBufferStore<K, V> implements ParsleyBufferStore<K, V> {
        private final MockBufferStore<K, V> delegate = new MockBufferStore<>();
        int indexEntriesCalls = 0;
        @Override public long add(ParsleyMessage<K, V> record, long bufferedAt) { return delegate.add(record, bufferedAt); }
        @Override public @Nullable Entry<K, V> get(long sequence) { return delegate.get(sequence); }
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
        @Override public @Nullable Entry<K, V> get(long sequence) { return delegate.get(sequence); }
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
        @Override public @Nullable Entry<K, V> get(long sequence) { return delegate.get(sequence); }
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
