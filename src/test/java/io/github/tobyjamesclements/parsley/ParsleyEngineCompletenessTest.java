package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyEngine#completeness()}: the per-coordinate minimum across <em>all</em>
 * input channels that produces the node's causal completeness frontier.
 *
 * <p>Each channel contributes the dependencies its records have advertised (the pairwise-max over
 * records on that channel) plus its own delivered position. {@code completeness()} is the
 * {@link ParsleyClock#intersectMin intersection-minimum} across those channels: a coordinate is
 * carried only when <em>every</em> channel has observed it, valued at the slowest branch. A
 * coordinate observed by only one channel is dropped entirely — it is not confirmed by all branches,
 * so a dependency on it is not satisfied until the other channels also advertise it (the topology
 * contract). This is strictly stronger than a min that keeps coordinates seen on a single branch.
 *
 * <p>The per-channel value is a max (not a running min) so a channel's view can always rise as it
 * advances; the cross-channel combination is the min so the boundary is bounded by the slowest branch.
 */
class ParsleyEngineCompletenessTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    // T3 is a shared ancestor: not consumed by the fan-in engine, but referenced in deps.
    private static final Uuid T3_ID = Uuid.randomUuid();
    // T4 is a coordinate unique to one branch (appears only in T1 records).
    private static final Uuid T4_ID = Uuid.randomUuid();

    // The node consumes T1 and T2; T3/T4 are upstream ancestors carried in deps. SCOPE is used only
    // for the engine's diagnostics/pruning, not as a delivery filter (the gate waits for all channels).
    private static final ParsleyClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID));

    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    // --- tests ----------------------------------------------------------------------------------

    /**
     * A fan-in node consuming T1 and T2, both of which descend from a shared ancestor T3.
     * T1 records carry a dep on T3 at offset 5; T2 records carry a dep on T3 at offset 3.
     *
     * <p>T3 is the only coordinate <em>both</em> channels observe, so {@code completeness()} reports
     * it at the minimum (3) across the two branches — bounded by the slowest branch. T1 and T2 each
     * appear only on their own channel, so the strict intersection drops them: completeness carries
     * only coordinates every input channel has confirmed.
     *
     * Asserts T3 appears at min(5, 3)=3 and that the single-channel coordinates T1/T2 are absent.
     */
    @Test
    void fanInCompleteness_reportsSharedAncestorAtMinAcrossBranches() {
        ParsleyEngine<String, String> engine = fanInEngine();

        // T1@10 carries dep {T3: 5} — the fast branch advanced T3 further.
        engine.onRecord(record(T1, 10, T1_ID, clock(T3_ID, 5)));
        // T2@7 carries dep {T3: 3} — the slow branch advanced T3 less.
        engine.onRecord(record(T2, 7, T2_ID, clock(T3_ID, 3)));

        ParsleyClock completeness = engine.completeness();

        assertEquals(3L, completeness.offsetFor(T3_ID, 0),
                "T3 (shared ancestor) must appear at the minimum across branches: min(5,3)=3");
        assertEquals(-1L, completeness.offsetFor(T1_ID, 0),
                "T1 is observed only by its own channel, so it is not confirmed by all channels");
        assertEquals(-1L, completeness.offsetFor(T2_ID, 0),
                "T2 is observed only by its own channel, so it is not confirmed by all channels");
    }

    /**
     * A coordinate that appears in one input channel's deps but not the other is <em>dropped</em>
     * from completeness — under the strict min, a coordinate is carried only when every channel has
     * confirmed it.
     *
     * <p>T1 records carry a dep on T4; T2 records never mention T4. After delivering both, the
     * completeness frontier must not carry T4, because the T2 branch has not confirmed it.
     *
     * Asserts T4 is absent from completeness, while the shared ancestor T3 is present at the min.
     */
    @Test
    void coordinateOnOnlyOneChannelIsDroppedNotConfirmedByAllChannels() {
        ParsleyEngine<String, String> engine = fanInEngine();

        // T1@10 carries deps {T3: 5, T4: 2}; T4 is unique to this branch.
        engine.onRecord(record(T1, 10, T1_ID, clock(T3_ID, 5).observe(T4_ID, 0, 2)));
        // T2@7 carries dep {T3: 3}; T4 is absent.
        engine.onRecord(record(T2, 7, T2_ID, clock(T3_ID, 3)));

        ParsleyClock completeness = engine.completeness();

        assertEquals(-1L, completeness.offsetFor(T4_ID, 0),
                "T4 (on only one channel) must be absent from completeness — not confirmed by the "
                        + "channel that never mentioned it");
        assertEquals(3L, completeness.offsetFor(T3_ID, 0),
                "T3 (shared ancestor) must still appear at min(5,3)=3");
    }

    /**
     * In a single-input relay (one channel), the completeness frontier rises to match the latest
     * delivered record's deps — it is not pinned at the smallest value ever seen.
     *
     * <p>This is the guard against a running-min regression: if the channel clock were a running
     * minimum across all records on a channel (rather than a per-channel max), the first record's
     * low T3 value would pin completeness there forever. The per-channel max ensures completeness
     * rises as the relay advances.
     *
     * Asserts completeness after two deliveries rises to the second (higher) T3 dep, not the first.
     */
    @Test
    void singleInputRelayCompletenessRisesToLatestDeps() {
        // Single-channel engine: only T1 in scope.
        ParsleyClock.CoordinatePredicate singleScope =
                (topicId, partition) -> partition == 0 && topicId.equals(T1_ID);
        ParsleyEngine<String, String> engine = engineOver(newFrontier(), singleScope);

        // First record: T1@0 with dep {T3: 3}. Use offset 0 so the frontier seeds cleanly.
        engine.onRecord(record(T1, 0, T1_ID, clock(T3_ID, 3)));
        ParsleyClock afterFirst = engine.completeness();

        assertEquals(3L, afterFirst.offsetFor(T3_ID, 0),
                "completeness after first delivery must report T3 at 3");

        // Second record: T1@1 (consecutive) with dep {T3: 7} — a later, higher dep.
        // Consecutive offsets ensure no gap so the frontier actually advances to offset 1.
        engine.onRecord(record(T1, 1, T1_ID, clock(T3_ID, 7)));
        ParsleyClock afterSecond = engine.completeness();

        assertEquals(7L, afterSecond.offsetFor(T3_ID, 0),
                "completeness after second delivery must rise to T3=7, not remain pinned at 3 "
                        + "(per-channel max ensures the channel clock advances, not a running min)");
        assertEquals(1L, afterSecond.offsetFor(T1_ID, 0),
                "T1 coordinate must reflect the latest delivered offset");
    }

    /**
     * After a simulated restart — a new {@link ParsleyFrontier} over the same changelog-backed store —
     * {@code completeness()} returns an identical result to the pre-restart value, without replaying
     * any records: the frontier clock and the per-channel clocks both restore from the single "f" blob.
     *
     * Asserts that the completeness frontier is identical after restart.
     */
    @Test
    void completenessRestoredIdenticallyAfterSimulatedRestart() {
        TestKeyValueStore<String, byte[]> sharedStore =
                new TestKeyValueStore<String, byte[]>(java.util.Comparator.naturalOrder());
        ParsleyFrontier firstFrontier = new ParsleyFrontier(sharedStore, new MockForwardedIndex(), new MockOrphanIndex());
        ParsleyEngine<String, String> first = engineOver(firstFrontier, SCOPE);

        // Deliver one record from each branch.
        first.onRecord(record(T1, 10, T1_ID, clock(T3_ID, 5)));
        first.onRecord(record(T2, 7, T2_ID, clock(T3_ID, 3)));

        ParsleyClock completenessBeforeRestart = first.completeness();

        // Simulate restart: a fresh ParsleyFrontier over the same store reloads the "f" blob (frontier
        // clock + channel clocks). A fresh forwarded/orphan index is fine — it only affects future
        // deliveries.
        ParsleyFrontier restartedFrontier = new ParsleyFrontier(sharedStore, new MockForwardedIndex(), new MockOrphanIndex());

        assertEquals(completenessBeforeRestart, restartedFrontier.completeness(),
                "completeness must be identical after restart when the frontier store is restored");
    }

    /**
     * With no channel clocks recorded yet (empty channel store), {@code completeness()} equals the
     * node's own delivered frontier — no ancestors have been observed, so no ancestor coordinates
     * narrow the boundary.
     *
     * Asserts completeness equals frontier when no records have been delivered.
     */
    @Test
    void completenessEqualsOwnFrontierWhenNoChannelClocksRecorded() {
        ParsleyEngine<String, String> engine = fanInEngine();

        assertEquals(engine.frontier(), engine.completeness(),
                "completeness must equal the node's own frontier when no channel clocks have been recorded");
    }

    /**
     * The strict gate end to end: a fan-in record depending on a shared ancestor is held until
     * <em>every</em> input channel has confirmed that ancestor, then released.
     *
     * <p>Both input channels are seeded (as the processor does at registration) so a silent channel
     * holds the completeness min rather than being absent from it. A T1 record depending on the
     * shared ancestor T3@5 is held while the T2 channel has not confirmed T3, and releases as soon as
     * a T2 record advertises T3@5 — the cross-channel confirmation that completeness requires.
     *
     * Asserts the record is buffered until the sibling channel confirms the ancestor, then both deliver.
     */
    @Test
    void fanInRecordHeldUntilEveryChannelConfirmsSharedAncestor() {
        ParsleyFrontier frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier, SCOPE);

        // T1@0 depends on shared ancestor T3@5; the T2 channel has not confirmed T3 → held.
        List<ParsleyMessage<String, String>> out1 = engine.onRecord(record(T1, 0, T1_ID, clock(T3_ID, 5))).delivered();
        assertEquals(List.of(), out1, "T1@0 must be held: the T2 channel has not confirmed T3@5");

        // T2@0 advertises T3@5 on the T2 channel → completeness[T3] reaches 5 → both deliver.
        List<ParsleyMessage<String, String>> out2 = engine.onRecord(record(T2, 0, T2_ID, clock(T3_ID, 5))).delivered();
        assertEquals(2, out2.size(),
                "T2@0 delivers and releases the held T1@0 once both channels confirm T3@5");
    }

    /**
     * An intra-topic dependency — a record depending on an earlier offset of its <em>own</em> input
     * topic — is satisfied immediately. It is treated like any other dependency (no special stripping
     * beyond the exact self-cycle), but the record's own channel already confirms its own topic's
     * progress, so a backward dependency on that same topic never has to wait on a sibling.
     *
     * Asserts a single-input node forwards a record that depends on an earlier offset of its own topic.
     */
    @Test
    void intraTopicDependencyOnOwnTopicIsSatisfiedImmediately() {
        // Single-input node consuming only T1.
        ParsleyClock.CoordinatePredicate t1Only =
                (topicId, partition) -> partition == 0 && topicId.equals(T1_ID);
        ParsleyFrontier frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier, t1Only);

        assertEquals(1, engine.onRecord(record(T1, 0, T1_ID, ParsleyClock.empty())).delivered().size(),
                "T1@0 with no dependencies delivers immediately");
        // T1@1 depends on T1@0 — an earlier offset of its own topic (intra-topic).
        assertEquals(1, engine.onRecord(record(T1, 1, T1_ID, clock(T1_ID, 0))).delivered().size(),
                "an intra-topic dependency (on the record's own topic) is satisfied immediately");
    }

    /**
     * An inter-topic dependency on a coordinate a sibling channel owns is held until that sibling
     * confirms it — the strict cross-channel gate. Distinct from the intra-topic case above: here the
     * dependency names a different input topic, whose channel must catch up first.
     *
     * Asserts a T1 record depending on T2@0 is held until the T2 channel delivers T2@0, then releases.
     */
    @Test
    void interTopicDependencyIsHeldUntilTheSiblingChannelConfirmsIt() {
        ParsleyFrontier frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier, SCOPE);

        assertEquals(List.of(), engine.onRecord(record(T1, 0, T1_ID, clock(T2_ID, 0))).delivered(),
                "an inter-topic dependency is held until the sibling channel confirms it");
        assertEquals(2, engine.onRecord(record(T2, 0, T2_ID, ParsleyClock.empty())).delivered().size(),
                "T2@0 delivers and releases the held T1@0 once the T2 channel confirms T2@0");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyFrontier newFrontier() {
        return new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), new MockOrphanIndex());
    }

    private ParsleyEngine<String, String> engineOver(ParsleyFrontier frontier,
                                                     ParsleyClock.CoordinatePredicate scope) {
        return new ParsleyEngine<>(frontier, buffer,
                new MockCandidateIndex(), ParsleyMetrics.NOOP, CausalAudit.NOOP,
                System::currentTimeMillis);
    }

    private ParsleyEngine<String, String> fanInEngine() {
        return engineOver(newFrontier(), SCOPE);
    }

    private static ParsleyMessage<String, String> record(TopicPartition tp, long offset,
                                                          Uuid topicId, ParsleyClock deps) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), deps);
    }

    private static ParsleyClock clock(Uuid topicId, long offset) {
        return ParsleyClock.empty().observe(topicId, 0, offset);
    }
}
