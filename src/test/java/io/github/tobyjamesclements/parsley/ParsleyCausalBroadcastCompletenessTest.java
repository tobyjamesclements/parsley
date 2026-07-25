package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyCausalBroadcast#completeness()}: this node's own frontier max-merged with every
 * input channel's advertised dependencies.
 *
 * <p>Each channel contributes the dependencies its records have advertised (the pairwise-max over
 * records on that channel). {@code completeness()} is {@link ParsleyVectorClock#merge}, not an
 * intersection: a coordinate counts the moment <em>any</em> channel has genuinely advertised it —
 * there is no requirement that every one of a node's channels independently repeat the same
 * confirmation. A node's own directly-consumed coordinates (part of the frontier) are always present,
 * since they are this node's own proven delivery history, not something any channel needs to confirm.
 *
 * <p>Genuine confirmation still matters: a channel only advertises a dependency once a record
 * carrying it has actually, gatedly delivered through that channel (see {@link ParsleyCausalBroadcastTest} for
 * the delivery gate itself — every record is checked against this node's current, already-proven
 * state, never against a stamp the same record just supplied). Several tests here pre-establish
 * channel state directly via
 * {@link ParsleyChannels#channelUpdate}/{@link ParsleyChannels#delivered}, standing in for "some record
 * already, genuinely delivered this" so the merge logic itself can be tested in isolation from gate
 * timing.
 *
 * <p>The per-channel value is a max (not a running min) so a channel's view can always rise as it
 * advances; the cross-channel combination is also a max (not a min) so a single genuine witness is
 * never held back by a sibling channel that simply hasn't mentioned the same coordinate.
 */
class ParsleyCausalBroadcastCompletenessTest {

    private static final TopicPartition C1 = new TopicPartition("c1", 0);
    private static final TopicPartition C2 = new TopicPartition("c2", 0);
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    // C3 is a shared ancestor: not consumed by the fan-in core, but referenced in deps.
    private static final Uuid C3_ID = Uuid.randomUuid();
    // C4 is a coordinate unique to one branch (appears only in C1 records).
    private static final Uuid C4_ID = Uuid.randomUuid();

    // The node consumes C1 and C2; C3/C4 are upstream ancestors carried in deps, out of this node's
    // own scope (it has no channel for them) — genuinely unreachable, not merely unconfirmed yet.
    private static final ParsleyVectorClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(C1_ID) || topicId.equals(C2_ID));

    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    // --- tests ----------------------------------------------------------------------------------

    /**
     * A fan-in node consuming C1 and C2, both of which descend from a shared ancestor C3. Channel C1
     * has already, genuinely delivered a record advertising C3 at 5; channel C2 has already,
     * genuinely delivered one advertising C3 at 3 (pre-established directly, standing in for genuine
     * prior delivery through each channel).
     *
     * <p>{@code completeness()} reports C3 at the maximum (5) across the two branches — a single
     * genuine witness (C1's channel) is enough, the slower branch's silence does not hold it back.
     * C1 and C2 each appear at their own delivered offset (10 and 7 respectively): a node's own
     * directly-consumed coordinates are always present in completeness, not dependent on any channel
     * advertising them.
     *
     * Asserts C3 appears at max(5,3)=5, and C1/C2 appear at their own delivered offsets.
     */
    @Test
    void fanInCompleteness_reportsSharedAncestorAtMaxAcrossBranches() {
        ParsleyChannels frontier = newFrontier();
        deliverSequentially(frontier, C1_ID, 10);
        frontier.channelUpdate(C1_ID, 0, clock(C3_ID, 5));
        deliverSequentially(frontier, C2_ID, 7);
        frontier.channelUpdate(C2_ID, 0, clock(C3_ID, 3));
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier, SCOPE);

        ParsleyVectorClock completeness = causalBroadcast.completeness();

        assertEquals(5L, completeness.offsetFor(C3_ID, 0),
                "C3 (shared ancestor) must appear at the maximum across branches: max(5,3)=5 — a "
                        + "single genuine witness suffices, the slower branch does not hold it back");
        assertEquals(10L, completeness.offsetFor(C1_ID, 0),
                "C1 is this node's own directly-consumed coordinate, always present at its delivered offset");
        assertEquals(7L, completeness.offsetFor(C2_ID, 0),
                "C2 is this node's own directly-consumed coordinate, always present at its delivered offset");
    }

    /**
     * A coordinate that a genuine delivery on one channel advertised, but the other never mentioned,
     * is still <em>included</em> in completeness — a single genuine witness is enough, there is no
     * cross-channel unanimity requirement.
     *
     * Asserts C4 is present at 2 (from C1's channel alone), while the shared ancestor C3 is present at the max.
     */
    @Test
    void coordinateOnOnlyOneChannelIsIncludedSingleWitnessSuffices() {
        ParsleyChannels frontier = newFrontier();
        deliverSequentially(frontier, C1_ID, 10);
        frontier.channelUpdate(C1_ID, 0, clock(C3_ID, 5).observe(C4_ID, 0, 2));
        deliverSequentially(frontier, C2_ID, 7);
        frontier.channelUpdate(C2_ID, 0, clock(C3_ID, 3));
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier, SCOPE);

        ParsleyVectorClock completeness = causalBroadcast.completeness();

        assertEquals(2L, completeness.offsetFor(C4_ID, 0),
                "C4 (advertised by only one channel) must still be included — a single genuine "
                        + "witness suffices, no cross-channel corroboration is required");
        assertEquals(5L, completeness.offsetFor(C3_ID, 0),
                "C3 (shared ancestor) must still appear at max(5,3)=5");
    }

    /**
     * A channel's clock is a per-channel <em>max</em> across every record genuinely delivered through
     * it, not a running minimum — the guard against a running-min regression: if it were a minimum, the
     * first genuine delivery's low C3 value would pin completeness there forever. Two genuine
     * deliveries are pre-established directly via {@link ParsleyChannels#channelUpdate} (standing in
     * for two records having actually, gatedly delivered through the C1 channel in sequence, the second
     * carrying a higher C3 value than the first — see the class Javadoc on pre-establishing genuine
     * channel state directly).
     *
     * Asserts completeness after the second genuine delivery rises to the higher C3 value, not the
     * first.
     */
    @Test
    void singleInputRelayCompletenessRisesToLatestDeps() {
        ParsleyVectorClock.CoordinatePredicate singleScope =
                (topicId, partition) -> partition == 0 && topicId.equals(C1_ID);
        ParsleyChannels frontier = newFrontier();
        deliverSequentially(frontier, C1_ID, 1);
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier, singleScope);

        // First genuine delivery on the C1 channel advertises C3 at 3.
        frontier.channelUpdate(C1_ID, 0, clock(C3_ID, 3));
        assertEquals(3L, causalBroadcast.completeness().offsetFor(C3_ID, 0),
                "completeness after the first genuine delivery must report C3 at 3");

        // A second genuine delivery on the same channel advertises a higher C3 value.
        frontier.channelUpdate(C1_ID, 0, clock(C3_ID, 7));
        assertEquals(7L, causalBroadcast.completeness().offsetFor(C3_ID, 0),
                "completeness after the second delivery must rise to C3=7, not remain pinned at 3 "
                        + "(per-channel max ensures the channel clock advances, not a running min)");
    }

    /**
     * After a simulated restart — a new {@link ParsleyChannels} over the same changelog-backed store —
     * {@code completeness()} returns an identical result to the pre-restart value, without replaying
     * any records: the frontier clock and the per-channel clocks both restore from the single frontier value.
     *
     * Asserts that the completeness clock is identical after restart.
     */
    @Test
    void completenessRestoredIdenticallyAfterSimulatedRestart() {
        TestKeyValueStore<String, byte[]> sharedStore =
                new TestKeyValueStore<String, byte[]>(java.util.Comparator.naturalOrder());
        ParsleyChannels firstFrontier = new ParsleyChannels(sharedStore, new MockForwardedIndex());
        deliverSequentially(firstFrontier, C1_ID, 10);
        firstFrontier.channelUpdate(C1_ID, 0, clock(C3_ID, 5));
        deliverSequentially(firstFrontier, C2_ID, 7);
        firstFrontier.channelUpdate(C2_ID, 0, clock(C3_ID, 3));
        ParsleyCausalBroadcast<String, String> first = causalBroadcastOver(firstFrontier, SCOPE);

        ParsleyVectorClock completenessBeforeRestart = first.completeness();

        // Simulate restart: a fresh ParsleyChannels over the same store reloads the frontier value (frontier
        // clock + channel clocks). A fresh forwarded index is fine — it only affects future deliveries.
        ParsleyChannels restartedFrontier = new ParsleyChannels(sharedStore, new MockForwardedIndex());

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
        ParsleyCausalBroadcast<String, String> causalBroadcast = fanInCausalBroadcast();

        assertEquals(causalBroadcast.frontier(), causalBroadcast.completeness(),
                "completeness must equal the node's own frontier when no channel clocks have been recorded");
    }

    /**
     * A fan-in record depending on a shared ancestor is held until a genuine witness — a channel that
     * has actually, gatedly delivered a record reaching the required offset — exists. It cannot prove
     * its own claim merely by declaring it: every record is checked against this node's current,
     * already-proven state, never against a stamp the record itself just supplied. Nor can a sibling's
     * declared claim about the same not-yet-proven ancestor serve as the witness — that sibling would
     * need the identical proof first, which is exactly the mutual-deadlock shape a genuine, direct
     * witness (here, this node's own third channel directly consuming C3) breaks.
     *
     * <p>C3 must be in scope here (this specific core directly consumes C1, C2, <em>and</em> C3) —
     * a dependency on a coordinate outside scope entirely is a different, fail-closed case (see
     * {@link ParsleyCausalBroadcastTest}), not vacuous satisfaction, and would defeat the point of this test
     * either way (a thrown C1@0 rather than a genuinely held one).
     *
     * Asserts C1@0 depending on C3@5 is held (no genuine witness anywhere yet), then releases once a
     * real C3 record genuinely, contiguously delivers up to offset 5.
     */
    @Test
    void fanInRecordHeldUntilAGenuineWitnessProvesTheSharedAncestor() {
        ParsleyVectorClock.CoordinatePredicate scopeIncludingAncestor = (topicId, partition) ->
                partition == 0 && (topicId.equals(C1_ID) || topicId.equals(C2_ID) || topicId.equals(C3_ID));
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(C1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(C2_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(C3_ID, 0, ParsleyVectorClock.empty());
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier, scopeIncludingAncestor);
        TopicPartition c3 = new TopicPartition("c3", 0);

        // C1@0 depends on shared ancestor C3@5 — nothing has genuinely proven C3@5 yet, so it is held.
        List<ParsleyMessage<String, String>> out1 = causalBroadcast.receive(record(C1, 0, C1_ID, clock(C3_ID, 5))).delivered();
        assertEquals(List.of(), out1, "C1@0 must be held: no channel has genuinely proven C3@5 yet");

        // A real C3 record, genuinely and contiguously delivered up to offset 5, is the direct witness
        // that releases the held C1@0.
        for (long offset = 0; offset < 5; offset++) {
            causalBroadcast.receive(record(c3, offset, C3_ID, ParsleyVectorClock.empty()));
        }
        List<ParsleyMessage<String, String>> out2 = causalBroadcast.receive(record(c3, 5, C3_ID, ParsleyVectorClock.empty())).delivered();
        assertEquals(2, out2.size(),
                "C3@5 delivers genuinely and releases the held C1@0, which depended on exactly that");
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
        // Single-input node consuming only C1.
        ParsleyVectorClock.CoordinatePredicate c1Only =
                (topicId, partition) -> partition == 0 && topicId.equals(C1_ID);
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(newFrontier(), c1Only);

        assertEquals(1, causalBroadcast.receive(record(C1, 0, C1_ID, ParsleyVectorClock.empty())).delivered().size(),
                "C1@0 with no dependencies delivers immediately");
        // C1@1 depends on C1@0 — an earlier offset of its own topic (intra-topic), already genuinely
        // delivered above.
        assertEquals(1, causalBroadcast.receive(record(C1, 1, C1_ID, clock(C1_ID, 0))).delivered().size(),
                "an intra-topic dependency (on the record's own topic) is satisfied immediately");
    }

    /**
     * An inter-topic dependency on a coordinate a sibling channel owns is held until that sibling
     * genuinely, gatedly delivers a record reaching it — the record's own claim about C2 is not
     * itself proof, since C2's own channel has never actually delivered anything yet.
     *
     * Asserts a C1 record depending on C2@0 is held until C2@0 genuinely delivers, then releases.
     */
    @Test
    void interTopicDependencyHeldUntilTheSiblingChannelGenuinelyDelivers() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(C1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(C2_ID, 0, ParsleyVectorClock.empty());
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier, SCOPE);

        List<ParsleyMessage<String, String>> out1 = causalBroadcast.receive(record(C1, 0, C1_ID, clock(C2_ID, 0))).delivered();
        assertEquals(List.of(), out1, "C1@0 must be held: C2 has not genuinely delivered anything yet");

        List<ParsleyMessage<String, String>> out2 = causalBroadcast.receive(record(C2, 0, C2_ID, ParsleyVectorClock.empty())).delivered();
        assertEquals(2, out2.size(),
                "C2@0 delivers genuinely and releases the held C1@0, which depended on exactly that");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyChannels newFrontier() {
        return ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
    }

    /** Genuinely, contiguously delivers offsets {@code 0..upTo} on {@code topicId}'s channel. */
    private static void deliverSequentially(ParsleyChannels frontier, Uuid topicId, long upTo) {
        for (long offset = 0; offset <= upTo; offset++) {
            frontier.delivered(topicId, 0, offset);
        }
    }

    private ParsleyCausalBroadcast<String, String> causalBroadcastOver(ParsleyChannels frontier,
                                                     ParsleyVectorClock.CoordinatePredicate scope) {
        return ParsleyTestFixtures.broadcast(frontier, buffer,
                new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis, scope);
    }

    private ParsleyCausalBroadcast<String, String> fanInCausalBroadcast() {
        return causalBroadcastOver(newFrontier(), SCOPE);
    }

    private static ParsleyMessage<String, String> record(TopicPartition tp, long offset,
                                                          Uuid topicId, ParsleyVectorClock deps) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), deps);
    }

    private static ParsleyVectorClock clock(Uuid topicId, long offset) {
        return ParsleyVectorClock.empty().observe(topicId, 0, offset);
    }
}
