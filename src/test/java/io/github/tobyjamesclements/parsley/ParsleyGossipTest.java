package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyGossip#receive}: a null message's carried clock feeds the outbound stamp
 * ({@link ParsleyCausalBroadcast#completeness()}) and the {@code advancedConsumedChannel} relay
 * signal (I6 — only an advance of a channel this node consumes obliges a relay; custody folds but
 * never obliges), but never the delivery gate — a peer's claim that a coordinate was delivered <em>there</em>
 * is not proof it was delivered <em>here</em>, so a held record releases only once this node's own
 * contiguous frontier genuinely reaches its dependencies. Gating on the max-merged completeness
 * used to let a null message claiming a sibling channel's coordinate release a held record before
 * this node had itself delivered that cause — an effect-before-cause delivery to the delegate,
 * violating the causal-order guarantee for a processor subscribing to both topics.
 */
class ParsleyGossipTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    // A foreign upstream ancestor: referenced by carried clocks, never consumed by this node.
    private static final Uuid T3_ID = Uuid.randomUuid();

    // The node consumes T1 and T2 on partition 0.
    private static final ParsleyVectorClock.CoordinatePredicate SCOPE = (topicId, partition) ->
            partition == 0 && (topicId.equals(T1_ID) || topicId.equals(T2_ID));

    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    /**
     * A held T1 record depending on sibling coordinate T2@3 must NOT be released by a null message
     * on the T1 channel whose carried clock merely claims T2@3 — the claim proves a peer delivered
     * T2@3, not that this node did, and the delegate would otherwise process the effect before its
     * cause. The record releases only once T2@0..3 genuinely deliver on this node's own T2 channel.
     */
    @Test
    void carriedClaimOnASiblingChannelDoesNotReleaseAHeldRecord() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier);
        ParsleyGossip<String, String> gossip = gossipOver(frontier, causalBroadcast);

        List<ParsleyMessage<String, String>> held =
                causalBroadcast.receive(record(T1, 0, T1_ID, clock(T2_ID, 3))).delivered();
        assertEquals(List.of(), held, "T1@0 must be held: this node has not delivered T2@3 itself");

        ParsleyGossip.Reception<String, String> reception = gossip.receive(T1_ID, 0, 1, clock(T2_ID, 3));
        assertEquals(List.of(), reception.delivered(),
                "a null message's carried claim of T2@3 must not release the held record — a peer's "
                        + "delivery is not this node's delivery");
        assertTrue(reception.advancedConsumedChannel(),
                "the carried clock advanced consumed sibling channel T2, so the relay signal is "
                        + "true even though nothing was released");

        for (long offset = 0; offset < 3; offset++) {
            assertEquals(List.of(), causalBroadcast.receive(record(T2, offset, T2_ID, ParsleyVectorClock.empty())).delivered()
                            .stream().filter(m -> m.topicId().equals(T1_ID)).toList(),
                    "the held T1@0 must stay held until T2 contiguously reaches 3, not before");
        }
        List<ParsleyMessage<String, String>> released =
                causalBroadcast.receive(record(T2, 3, T2_ID, ParsleyVectorClock.empty())).delivered();
        assertEquals(2, released.size(),
                "T2@3 genuinely delivering on this node's own channel releases the held T1@0 "
                        + "(plus T2@3 itself), in causal order");
    }

    /**
     * The carried clock still feeds the outbound stamp: a null message claiming foreign ancestor
     * T3@5 surfaces in {@link ParsleyCausalBroadcast#completeness()} (transitive ancestry a
     * downstream node's own gate verifies for itself) without releasing anything here.
     */
    @Test
    void carriedClaimStillEntersTheOutboundStampCompleteness() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier);
        ParsleyGossip<String, String> gossip = gossipOver(frontier, causalBroadcast);

        gossip.receive(T1_ID, 0, 0, clock(T3_ID, 5));

        assertEquals(5L, causalBroadcast.completeness().offsetFor(T3_ID, 0),
                "the carried foreign-ancestor claim must surface in the outbound stamp for "
                        + "transitive downstream propagation");
        assertEquals(-1L, causalBroadcast.frontier().offsetFor(T3_ID, 0),
                "the claim must never enter this node's own delivered frontier — the gate's clock");
    }

    /**
     * A null message's own offset is genuinely delivered on its source channel (it occupies a real
     * offset this node consumed), so a held record depending on that channel's coordinate releases
     * through the ordinary frontier advance — null-message-only (non-emitting-path) channel
     * liveness.
     */
    @Test
    void nullMessageOwnOffsetAdvancesItsChannelAndReleasesDependents() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier);
        ParsleyGossip<String, String> gossip = gossipOver(frontier, causalBroadcast);

        assertEquals(List.of(), causalBroadcast.receive(record(T2, 0, T2_ID, clock(T1_ID, 0))).delivered(),
                "T2@0 must be held: T1@0 has not been delivered yet");

        ParsleyGossip.Reception<String, String> reception =
                gossip.receive(T1_ID, 0, 0, ParsleyVectorClock.empty());

        assertEquals(1, reception.delivered().size(),
                "the null message's own offset genuinely delivers T1@0, releasing the held T2@0 — "
                        + "a null-message-only channel still advances its own frontier");
        assertFalse(reception.advancedConsumedChannel(),
                "an empty carried clock advanced nothing, so the relay signal stays false "
                        + "even though the message's own delivery released a record");
    }

    /**
     * The I6 relay rule with own outputs, in the one shape where {@code ownOutputs} is
     * load-bearing for the trigger: a <em>consumed own sink</em> (the self-cycle). A null message
     * reflecting this node's own produced coordinate back at it — on a channel inside the consumed
     * trigger scope, while the append itself is still above the frontier — at or below the
     * {@code ownOutputs} clock teaches nothing (this node produced that position), so the relay
     * signal settles {@code false} on first sight, without the old stamp-side own-sink strip. The
     * unstripped claim still folds into the channel clock and rides the outbound stamp (I9). A
     * claim on a non-consumed sink would be filtered by the trigger scope before ownOutputs even
     * mattered; this test pins the domination itself.
     */
    @Test
    void reflectedOwnOutputClaimTeachesNothingSoTheRelaySettles() {
        ParsleyChannels channels = newFrontier();
        // T2 is consumed (in SCOPE) and also this node's own sink — the self-cycle shape. The
        // acked append at T2@7 has not been delivered here (frontier T2 = -1), so only the
        // ownOutputs side of stamp() can dominate the reflected claim.
        channels.acknowledge(T2_ID, 0, 7);
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                channels, buffer, new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis, SCOPE, (topicId, partition) -> topicId.equals(T2_ID));
        ParsleyGossip<String, String> gossip = gossipOver(channels, causalBroadcast);

        ParsleyGossip.Reception<String, String> reflected = gossip.receive(T1_ID, 0, 0, clock(T2_ID, 7));

        assertFalse(reflected.advancedConsumedChannel(),
                "a consumed-own-sink claim at or below ownOutputs is this node's own knowledge — "
                        + "relaying it would ping-pong forever around a self-cycle (I6: the "
                        + "comparison is against total knowledge, ownOutputs included)");
        assertEquals(7L, causalBroadcast.completeness().offsetFor(T2_ID, 0),
                "the reflected claim must still fold into the channel clock unstripped (I9) — "
                        + "only the relay is suppressed, never the merge");
    }

    /**
     * The trigger scope split, both directions. A claim on foreign ancestor T3 — a channel this
     * node neither consumes nor produces — folds into the stamp (I9) but never obliges a relay,
     * first sight included: custody coverage is hearsay that structurally lags its source, and
     * relaying on it is what let ≥3-node topic cycles storm forever (each relay's own offset was
     * the next blind member's news). A claim on consumed channel T2 obliges a relay on first
     * sight only: the fold makes it total knowledge, so the identical claim settles on repeat —
     * even arriving on a different channel (the comparison is against total knowledge, never a
     * single channel's clock).
     */
    @Test
    void custodyNeverObligesARelayAndConsumedChannelNewsObligesOnce() {
        ParsleyChannels frontier = newFrontier();
        ParsleyCausalBroadcast<String, String> causalBroadcast = causalBroadcastOver(frontier);
        ParsleyGossip<String, String> gossip = gossipOver(frontier, causalBroadcast);

        assertFalse(gossip.receive(T1_ID, 0, 0, clock(T3_ID, 5)).advancedConsumedChannel(),
                "a custody claim (T3 is neither consumed nor produced) must never oblige a relay, "
                        + "even on first sight — relaying hearsay is the ≥3-cycle storm");
        assertEquals(5L, causalBroadcast.completeness().offsetFor(T3_ID, 0),
                "the custody claim still folds into the channel clock and the stamp (I9) — only "
                        + "the relay obligation is scoped, never the merge");
        assertTrue(gossip.receive(T1_ID, 0, 1, clock(T2_ID, 3)).advancedConsumedChannel(),
                "first sight of a consumed-channel claim (T2@3 above this node's knowledge) is "
                        + "genuine input news — it must relay");
        assertFalse(gossip.receive(T1_ID, 0, 2, clock(T2_ID, 3)).advancedConsumedChannel(),
                "the identical consumed-channel claim taught nothing the second time — it must "
                        + "settle, not ping-pong");
        assertFalse(gossip.receive(T2_ID, 0, 0, clock(T2_ID, 3)).advancedConsumedChannel(),
                "the same claim arriving on a DIFFERENT channel is still already-known — I6 "
                        + "compares against total knowledge, never a single channel's clock");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyChannels newFrontier() {
        return ParsleyTestFixtures.channels(ParsleyVectorClock.empty(), new MockForwardedIndex());
    }

    private ParsleyCausalBroadcast<String, String> causalBroadcastOver(ParsleyChannels frontier) {
        return ParsleyTestFixtures.broadcast(frontier, buffer,
                new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis, SCOPE);
    }

    private static ParsleyGossip<String, String> gossipOver(ParsleyChannels channels,
                                                            ParsleyCausalBroadcast<String, String> broadcast) {
        return new ParsleyGossip<>(broadcast, Set.of(), SCOPE);
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
