package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Null-message relay quiescence on topic cycles, surfaced by the random-topology explorer's
 * first sweep (2026-07-21). The I6 relay rule — relay iff {@code !stamp().dominates(carried)},
 * acks folded first ({@code ParsleyGossip.receive}) — reaches knowledge closure on a cycle only
 * if every cycle channel is produced or consumed by every cycle member:
 *
 * <ul>
 *   <li>A cycle member <em>consuming</em> a channel covers its coordinates by frontier; one
 *       <em>producing</em> it covers them by acked {@code ownOutputs}. Self-loops (the
 *       {@code ParsleyCyclicQuiesceIT} shape) and two-node cycles have no other members, so
 *       every echo is eventually dominated and the gossip stops.</li>
 *   <li>On a cycle of length ≥ 3 some member is <em>blind</em> to some cycle channel — it
 *       neither produces nor consumes it, knowing it only through carried-clock custody (I9),
 *       which is always one gossip lap stale. Each relay occupies a fresh offset on its channel;
 *       that offset reaches the blind member as strictly new knowledge next lap; I6 obliges it
 *       to relay, appending the offset that continues the cycle. Knowledge closure is
 *       unattainable because relaying enlarges the state being gossiped: an idle system
 *       generates null-message traffic forever, at one message per loop latency per channel.</li>
 * </ul>
 *
 * <p>This is a liveness/traffic defect, not a safety one — no delivery-order invariant is
 * violated. {@link #threeNodeCycleGossipCurrentlyStormsForever} PINS THE DEFECT: it asserts the
 * storm happens, so the day the protocol closes it (e.g. relaying only coordinates of channels
 * the node consumes or produces, or suppressing echoes of pure-gossip coordinates) this test
 * fails loudly and must be flipped into a quiescence assertion. The topology generator excludes
 * ≥ 3-cycles until then ({@code ParsleyTopologyGen}).
 */
class ParsleyGossipCycleQuiescenceTest {

    /**
     * The two-node cycle quiesces: A and B each produce one cycle channel and consume the other,
     * so after one business record seeds the gossip, each side's echo is dominated once its own
     * send acks — the whole exchange settles in one null message per channel. Also pins the sim
     * fidelity fix that made this observable: null-message sends ack and fold into
     * {@code ownOutputs} exactly like business sends (O4 exempts them from the crossing wait,
     * never from acking).
     */
    @Test
    void twoNodeCycleGossipQuiescesAfterOneExchange() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("t1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c2", "t1"), List.of("c1"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1"), List.of("c2"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("t1");
        sim.drain();
        assertEquals(1, sim.logSize("c1"),
                "one business delivery at A must advertise exactly one null message on c1 — "
                        + "B's echo teaches A nothing new (A produced c1, consumed c2)");
        assertEquals(1, sim.logSize("c2"),
                "B's relay of A's advertisement must be the last message — its echo back to A "
                        + "is dominated after A's ack folds (I6 quenches the two-node cycle)");
    }

    /**
     * KNOWN LIVENESS DEFECT, deliberately pinned — see the class Javadoc. Three silent nodes in
     * a topic cycle with one seeded business record gossip forever: every member is blind to the
     * cycle channel two hops upstream, learns its latest offset one lap late, and must relay by
     * I6, creating the offset that sustains the next lap. The sim's runaway guard (20k records,
     * all-null tail) is what stops it. If this test FAILS the defect has been fixed: flip it to
     * assert quiescence (small log sizes after drain) and re-enable ≥ 3-cycles in
     * {@code ParsleyTopologyGen}.
     */
    @Test
    void threeNodeCycleGossipCurrentlyStormsForever() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("t1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c3", "t1"), List.of("c1"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1"), List.of("c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("c2"), List.of("c3"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1).withNoTrace();
        sim.produceExternal("t1");
        AssertionError storm = assertThrows(AssertionError.class, sim::drain,
                "the three-node gossip cycle currently relays forever (blind-channel custody "
                        + "lag) — if it now quiesces, the defect is FIXED: flip this test");
        assertTrue(storm.getMessage().contains("relay loop"),
                "the runaway must be the all-null relay-loop classification, not business "
                        + "amplification: " + storm.getMessage());
    }
}
