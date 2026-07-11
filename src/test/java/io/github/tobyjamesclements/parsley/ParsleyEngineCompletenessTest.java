package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyEngine#completeness()}: this node's own frontier max-merged with every
 * input channel's advertised dependencies.
 *
 * <p>Each channel contributes the dependencies its records have advertised (the pairwise-max over
 * records on that channel). {@code completeness()} is {@link ParsleyClock#merge}, not an
 * intersection: a coordinate counts the moment <em>any</em> channel has genuinely advertised it —
 * there is no requirement that every one of a node's channels independently repeat the same
 * confirmation. A node's own directly-consumed coordinates (part of the frontier) are always present,
 * since they are this node's own proven delivery history, not something any channel needs to confirm.
 *
 * <p>Genuine confirmation still matters: a channel only advertises a dependency once a record
 * carrying it has actually, gatedly delivered through that channel (see
 * {@link ParsleyEngineTest}/{@link ParsleyEngineDeadLetterTest} for the delivery gate itself — every
 * record is checked against this node's current, already-proven state, never against a stamp the same
 * record just supplied). Several tests here pre-establish channel state directly via
 * {@link ParsleyFrontier#channelUpdate}/{@link ParsleyFrontier#deliver}, standing in for "some record
 * already, genuinely delivered this" so the merge logic itself can be tested in isolation from gate
 * timing.
 *
 * <p>The per-channel value is a max (not a running min) so a channel's view can always rise as it
 * advances; the cross-channel combination is also a max (not a min) so a single genuine witness is
 * never held back by a sibling channel that simply hasn't mentioned the same coordinate.
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

    // The node consumes T1 and T2; T3/T4 are upstream ancestors carried in deps, out of this node's
    // own scope (it has no channel for them) — genuinely unreachable, not merely unconfirmed yet.
    private static final ParsleyClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID));

    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    // --- tests ----------------------------------------------------------------------------------

    /**
     * A fan-in node consuming T1 and T2, both of which descend from a shared ancestor T3. Channel T1
     * has already, genuinely delivered a record advertising T3 at 5; channel T2 has already,
     * genuinely delivered one advertising T3 at 3 (pre-established directly, standing in for genuine
     * prior delivery through each channel).
     *
     * <p>{@code completeness()} reports T3 at the maximum (5) across the two branches — a single
     * genuine witness (T1's channel) is enough, the slower branch's silence does not hold it back.
     * T1 and T2 each appear at their own delivered offset (10 and 7 respectively): a node's own
     * directly-consumed coordinates are always present in completeness, not dependent on any channel
     * advertising them.
     *
     * Asserts T3 appears at max(5,3)=5, and T1/T2 appear at their own delivered offsets.
     */
    @Test
    void fanInCompleteness_reportsSharedAncestorAtMaxAcrossBranches() {
        ParsleyFrontier frontier = newFrontier();
        deliverSequentially(frontier, T1_ID, 10);
        frontier.channelUpdate(T1_ID, 0, clock(T3_ID, 5));
        deliverSequentially(frontier, T2_ID, 7);
        frontier.channelUpdate(T2_ID, 0, clock(T3_ID, 3));
        ParsleyEngine<String, String> engine = engineOver(frontier, SCOPE);

        ParsleyClock completeness = engine.completeness();

        assertEquals(5L, completeness.offsetFor(T3_ID, 0),
                "T3 (shared ancestor) must appear at the maximum across branches: max(5,3)=5 — a "
                        + "single genuine witness suffices, the slower branch does not hold it back");
        assertEquals(10L, completeness.offsetFor(T1_ID, 0),
                "T1 is this node's own directly-consumed coordinate, always present at its delivered offset");
        assertEquals(7L, completeness.offsetFor(T2_ID, 0),
                "T2 is this node's own directly-consumed coordinate, always present at its delivered offset");
    }

    /**
     * A coordinate that a genuine delivery on one channel advertised, but the other never mentioned,
     * is still <em>included</em> in completeness — a single genuine witness is enough, there is no
     * cross-channel unanimity requirement.
     *
     * Asserts T4 is present at 2 (from T1's channel alone), while the shared ancestor T3 is present at the max.
     */
    @Test
    void coordinateOnOnlyOneChannelIsIncludedSingleWitnessSuffices() {
        ParsleyFrontier frontier = newFrontier();
        deliverSequentially(frontier, T1_ID, 10);
        frontier.channelUpdate(T1_ID, 0, clock(T3_ID, 5).observe(T4_ID, 0, 2));
        deliverSequentially(frontier, T2_ID, 7);
        frontier.channelUpdate(T2_ID, 0, clock(T3_ID, 3));
        ParsleyEngine<String, String> engine = engineOver(frontier, SCOPE);

        ParsleyClock completeness = engine.completeness();

        assertEquals(2L, completeness.offsetFor(T4_ID, 0),
                "T4 (advertised by only one channel) must still be included — a single genuine "
                        + "witness suffices, no cross-channel corroboration is required");
        assertEquals(5L, completeness.offsetFor(T3_ID, 0),
                "T3 (shared ancestor) must still appear at max(5,3)=5");
    }

    /**
     * A channel's clock is a per-channel <em>max</em> across every record genuinely delivered through
     * it, not a running minimum — the guard against a running-min regression: if it were a minimum, the
     * first genuine delivery's low T3 value would pin completeness there forever. Two genuine
     * deliveries are pre-established directly via {@link ParsleyFrontier#channelUpdate} (standing in
     * for two records having actually, gatedly delivered through the T1 channel in sequence, the second
     * carrying a higher T3 value than the first — see the class Javadoc on pre-establishing genuine
     * channel state directly).
     *
     * Asserts completeness after the second genuine delivery rises to the higher T3 value, not the
     * first.
     */
    @Test
    void singleInputRelayCompletenessRisesToLatestDeps() {
        ParsleyClock.CoordinatePredicate singleScope =
                (topicId, partition) -> partition == 0 && topicId.equals(T1_ID);
        ParsleyFrontier frontier = newFrontier();
        deliverSequentially(frontier, T1_ID, 1);
        ParsleyEngine<String, String> engine = engineOver(frontier, singleScope);

        // First genuine delivery on the T1 channel advertises T3 at 3.
        frontier.channelUpdate(T1_ID, 0, clock(T3_ID, 3));
        assertEquals(3L, engine.completeness().offsetFor(T3_ID, 0),
                "completeness after the first genuine delivery must report T3 at 3");

        // A second genuine delivery on the same channel advertises a higher T3 value.
        frontier.channelUpdate(T1_ID, 0, clock(T3_ID, 7));
        assertEquals(7L, engine.completeness().offsetFor(T3_ID, 0),
                "completeness after the second delivery must rise to T3=7, not remain pinned at 3 "
                        + "(per-channel max ensures the channel clock advances, not a running min)");
    }

    /**
     * After a simulated restart — a new {@link ParsleyFrontier} over the same changelog-backed store —
     * {@code completeness()} returns an identical result to the pre-restart value, without replaying
     * any records: the frontier clock and the per-channel clocks both restore from the single "f" blob.
     *
     * Asserts that the completeness clock is identical after restart.
     */
    @Test
    void completenessRestoredIdenticallyAfterSimulatedRestart() {
        TestKeyValueStore<String, byte[]> sharedStore =
                new TestKeyValueStore<String, byte[]>(java.util.Comparator.naturalOrder());
        ParsleyFrontier firstFrontier = new ParsleyFrontier(sharedStore, new MockForwardedIndex(), new MockOrphanIndex());
        deliverSequentially(firstFrontier, T1_ID, 10);
        firstFrontier.channelUpdate(T1_ID, 0, clock(T3_ID, 5));
        deliverSequentially(firstFrontier, T2_ID, 7);
        firstFrontier.channelUpdate(T2_ID, 0, clock(T3_ID, 3));
        ParsleyEngine<String, String> first = engineOver(firstFrontier, SCOPE);

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
     * A fan-in record depending on a shared ancestor is held until a genuine witness — a channel that
     * has actually, gatedly delivered a record reaching the required offset — exists. It cannot prove
     * its own claim merely by declaring it: every record is checked against this node's current,
     * already-proven state, never against a stamp the record itself just supplied. Nor can a sibling's
     * declared claim about the same not-yet-proven ancestor serve as the witness — that sibling would
     * need the identical proof first, which is exactly the mutual-deadlock shape a genuine, direct
     * witness (here, this node's own third channel directly consuming T3) breaks.
     *
     * <p>T3 must be in scope here (this specific engine directly consumes T1, T2, <em>and</em> T3) —
     * a dependency on a coordinate outside scope entirely is a different, fail-closed case (see
     * {@link ParsleyEngineDeadLetterTest}), not vacuous satisfaction, and would defeat the point of
     * this test either way (a thrown/dead-lettered T1@0 rather than a genuinely held one).
     *
     * Asserts T1@0 depending on T3@5 is held (no genuine witness anywhere yet), then releases once a
     * real T3 record genuinely, contiguously delivers up to offset 5.
     */
    @Test
    void fanInRecordHeldUntilAGenuineWitnessProvesTheSharedAncestor() {
        ParsleyClock.CoordinatePredicate scopeIncludingAncestor = (topicId, partition) ->
                partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID) || topicId.equals(T3_ID));
        ParsleyFrontier frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T3_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier, scopeIncludingAncestor);
        TopicPartition t3 = new TopicPartition("t3", 0);

        // T1@0 depends on shared ancestor T3@5 — nothing has genuinely proven T3@5 yet, so it is held.
        List<ParsleyMessage<String, String>> out1 = engine.onRecord(record(T1, 0, T1_ID, clock(T3_ID, 5))).delivered();
        assertEquals(List.of(), out1, "T1@0 must be held: no channel has genuinely proven T3@5 yet");

        // A real T3 record, genuinely and contiguously delivered up to offset 5, is the direct witness
        // that releases the held T1@0.
        for (long offset = 0; offset < 5; offset++) {
            engine.onRecord(record(t3, offset, T3_ID, ParsleyClock.empty()));
        }
        List<ParsleyMessage<String, String>> out2 = engine.onRecord(record(t3, 5, T3_ID, ParsleyClock.empty())).delivered();
        assertEquals(2, out2.size(),
                "T3@5 delivers genuinely and releases the held T1@0, which depended on exactly that");
    }

    /**
     * An intra-topic dependency — a record depending on an earlier offset of its <em>own</em> input
     * topic — is satisfied immediately. It is treated like any other dependency (no special stripping
     * beyond the exact self-cycle), but the record's own channel already, genuinely confirms its own
     * topic's progress via ordinary contiguous delivery, so a backward dependency on that same topic
     * never has to wait on a sibling.
     *
     * Asserts a single-input node forwards a record that depends on an earlier offset of its own topic.
     */
    @Test
    void intraTopicDependencyOnOwnTopicIsSatisfiedImmediately() {
        // Single-input node consuming only T1.
        ParsleyClock.CoordinatePredicate t1Only =
                (topicId, partition) -> partition == 0 && topicId.equals(T1_ID);
        ParsleyEngine<String, String> engine = engineOver(newFrontier(), t1Only);

        assertEquals(1, engine.onRecord(record(T1, 0, T1_ID, ParsleyClock.empty())).delivered().size(),
                "T1@0 with no dependencies delivers immediately");
        // T1@1 depends on T1@0 — an earlier offset of its own topic (intra-topic), already genuinely
        // delivered above.
        assertEquals(1, engine.onRecord(record(T1, 1, T1_ID, clock(T1_ID, 0))).delivered().size(),
                "an intra-topic dependency (on the record's own topic) is satisfied immediately");
    }

    /**
     * An inter-topic dependency on a coordinate a sibling channel owns is held until that sibling
     * genuinely, gatedly delivers a record reaching it — the record's own claim about T2 is not
     * itself proof, since T2's own channel has never actually delivered anything yet.
     *
     * Asserts a T1 record depending on T2@0 is held until T2@0 genuinely delivers, then releases.
     */
    @Test
    void interTopicDependencyHeldUntilTheSiblingChannelGenuinelyDelivers() {
        ParsleyFrontier frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier, SCOPE);

        List<ParsleyMessage<String, String>> out1 = engine.onRecord(record(T1, 0, T1_ID, clock(T2_ID, 0))).delivered();
        assertEquals(List.of(), out1, "T1@0 must be held: T2 has not genuinely delivered anything yet");

        List<ParsleyMessage<String, String>> out2 = engine.onRecord(record(T2, 0, T2_ID, ParsleyClock.empty())).delivered();
        assertEquals(2, out2.size(),
                "T2@0 delivers genuinely and releases the held T1@0, which depended on exactly that");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyFrontier newFrontier() {
        return new ParsleyFrontier(ParsleyClock.empty(), new MockForwardedIndex(), new MockOrphanIndex());
    }

    /** Genuinely, contiguously delivers offsets {@code 0..upTo} on {@code topicId}'s channel. */
    private static void deliverSequentially(ParsleyFrontier frontier, Uuid topicId, long upTo) {
        for (long offset = 0; offset <= upTo; offset++) {
            frontier.deliver(topicId, 0, offset);
        }
    }

    private ParsleyEngine<String, String> engineOver(ParsleyFrontier frontier,
                                                     ParsleyClock.CoordinatePredicate scope) {
        return new ParsleyEngine<>(frontier, buffer,
                new MockCandidateIndex(), ParsleyMetrics.NOOP, CausalAudit.NOOP,
                System::currentTimeMillis, false, scope);
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
