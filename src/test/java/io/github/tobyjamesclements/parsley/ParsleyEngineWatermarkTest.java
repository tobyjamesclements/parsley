package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Engine-level tests for {@link ParsleyEngine#onWatermark}: a watermark's carried clock feeds the
 * outbound stamp ({@link ParsleyEngine#completeness()}) and the {@code channelAdvanced} relay signal,
 * but never the delivery gate — a peer's claim that a coordinate was delivered <em>there</em> is not
 * proof it was delivered <em>here</em>, so a held record releases only once this node's own contiguous
 * frontier genuinely reaches its dependencies. Gating on the max-merged completeness used to let a
 * watermark claiming a sibling channel's coordinate release a held record before this node had itself
 * delivered that cause — an effect-before-cause delivery to the delegate, violating the causal-order
 * guarantee for a processor subscribing to both topics.
 */
class ParsleyEngineWatermarkTest {

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
     * A held T1 record depending on sibling coordinate T2@3 must NOT be released by a watermark on the
     * T1 channel whose carried clock merely claims T2@3 — the claim proves a peer delivered T2@3, not
     * that this node did, and the delegate would otherwise process the effect before its cause. The
     * record releases only once T2@0..3 genuinely deliver on this node's own T2 channel.
     */
    @Test
    void watermarkClaimOnASiblingChannelDoesNotReleaseAHeldRecord() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier);

        List<ParsleyMessage<String, String>> held =
                engine.receive(record(T1, 0, T1_ID, clock(T2_ID, 3))).delivered();
        assertEquals(List.of(), held, "T1@0 must be held: this node has not delivered T2@3 itself");

        ParsleyEngine.WatermarkOutcome<String, String> watermark =
                engine.onWatermark(T1_ID, 0, 1, clock(T2_ID, 3));
        assertEquals(List.of(), watermark.outcome().delivered(),
                "a watermark's carried claim of T2@3 must not release the held record — a peer's "
                        + "delivery is not this node's delivery");
        assertTrue(watermark.channelAdvanced(),
                "the carried clock did teach the channel something new, so the relay signal is true "
                        + "even though nothing was released");

        for (long offset = 0; offset < 3; offset++) {
            assertEquals(List.of(), engine.receive(record(T2, offset, T2_ID, ParsleyVectorClock.empty())).delivered()
                            .stream().filter(m -> m.topicId().equals(T1_ID)).toList(),
                    "the held T1@0 must stay held until T2 contiguously reaches 3, not before");
        }
        List<ParsleyMessage<String, String>> released =
                engine.receive(record(T2, 3, T2_ID, ParsleyVectorClock.empty())).delivered();
        assertEquals(2, released.size(),
                "T2@3 genuinely delivering on this node's own channel releases the held T1@0 "
                        + "(plus T2@3 itself), in causal order");
    }

    /**
     * The carried clock still feeds the outbound stamp: a watermark claiming foreign ancestor T3@5
     * surfaces in {@link ParsleyEngine#completeness()} (transitive ancestry a downstream node's own
     * gate verifies for itself) without releasing anything here.
     */
    @Test
    void watermarkClaimStillEntersTheOutboundStampCompleteness() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier);

        engine.onWatermark(T1_ID, 0, 0, clock(T3_ID, 5));

        assertEquals(5L, engine.completeness().offsetFor(T3_ID, 0),
                "the carried foreign-ancestor claim must surface in the outbound stamp for "
                        + "transitive downstream propagation");
        assertEquals(-1L, engine.frontier().offsetFor(T3_ID, 0),
                "the claim must never enter this node's own delivered frontier — the gate's clock");
    }

    /**
     * A watermark's own offset is genuinely delivered on its source channel (it occupies a real
     * offset this node consumed), so a held record depending on that channel's coordinate releases
     * through the ordinary frontier advance — marker-only (passthrough) channel liveness.
     */
    @Test
    void watermarkOwnOffsetAdvancesItsChannelAndReleasesDependents() {
        ParsleyChannels frontier = newFrontier();
        frontier.channelUpdate(T1_ID, 0, ParsleyVectorClock.empty());
        frontier.channelUpdate(T2_ID, 0, ParsleyVectorClock.empty());
        ParsleyEngine<String, String> engine = engineOver(frontier);

        assertEquals(List.of(), engine.receive(record(T2, 0, T2_ID, clock(T1_ID, 0))).delivered(),
                "T2@0 must be held: T1@0 has not been delivered yet");

        ParsleyEngine.WatermarkOutcome<String, String> watermark =
                engine.onWatermark(T1_ID, 0, 0, ParsleyVectorClock.empty());

        assertEquals(1, watermark.outcome().delivered().size(),
                "the watermark's own offset genuinely delivers T1@0, releasing the held T2@0 — "
                        + "a marker-only channel still advances its own frontier");
        assertFalse(watermark.channelAdvanced(),
                "an empty carried clock taught the channel nothing, so the relay signal stays false "
                        + "even though the marker's own delivery released a record");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyChannels newFrontier() {
        return new ParsleyChannels(ParsleyVectorClock.empty(), new MockForwardedIndex());
    }

    private ParsleyEngine<String, String> engineOver(ParsleyChannels frontier) {
        return new ParsleyEngine<>(frontier, buffer,
                new MockCandidateIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis, SCOPE);
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
