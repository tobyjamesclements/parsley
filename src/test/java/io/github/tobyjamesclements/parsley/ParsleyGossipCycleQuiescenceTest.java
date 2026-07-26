package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Null-message relay quiescence on topic cycles. The relay rule — relay iff the carried clock
 * advanced this node's total knowledge on a channel it <em>consumes</em>
 * ({@code !stamp().dominates(carried.retaining(consumedScope))}, acks folded first;
 * {@code ParsleyGossip.receive}) — reaches knowledge closure on every cycle shape because only
 * coordinates with first-hand, physically-catching-up coverage (the contiguous frontier; plus
 * {@code ownOutputs} where an input is an own sink) can oblige a relay:
 *
 * <ul>
 *   <li>Custody — a claim on a channel the node neither consumes nor produces — folds into the
 *       stamp but never obliges a relay. Under the previous rule (relay on <em>any</em>
 *       new knowledge) custody was always one gossip lap stale on a cycle of length ≥ 3, so
 *       every lap obliged a relay whose own fresh offset was the next blind member's news:
 *       an idle deployment emitted null messages forever, at one message per loop latency per
 *       channel (surfaced by the random-topology explorer's first sweep, 2026-07-21; pinned
 *       here as a storm until fixed, now pinned as quiescent).</li>
 *   <li>Sibling appends on a shared sink the node does not consume are custody too: their
 *       first-hand coverage ({@code ownOutputs}) covers only the node's own appends, so letting
 *       them oblige relays reopens the storm on shared-sink cycles — the
 *       {@link #sharedSinkThreeNodeCycleQuiescesWithoutSiblingEcho} shape, which the
 *       consumed-or-produced trigger variant provably fails.</li>
 * </ul>
 *
 * <p>These specs are the regression pins for the storm fix; {@code ParsleyTopologyGen} generates
 * ≥ 3-cycles and shared-sinks-onto-consumed-topics again since the fix, so the random sweep
 * exercises the same shapes at scale with the runaway guard as the classifier.
 */
class ParsleyGossipCycleQuiescenceTest {

    /**
     * The two-node cycle quiesces after a single advertisement: A's business delivery of c4@0
     * advertises one null message on c1, and B does not echo it — the only news it carries
     * (c4@0) is on a channel B neither consumes nor produces, custody that folds into B's stamp
     * without obliging a relay. (Under the previous rule B echoed on c2 and the exchange settled
     * only when A's ack-folded ownOutputs dominated the echo; the ownOutputs quench itself stays
     * pinned by ParsleyGossipTest's reflected-claim test and the self-cycle IT, where the own
     * sink is consumed and the quench is load-bearing.)
     */
    @Test
    void twoNodeCycleGossipQuiescesAfterOneAdvertisement() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("c4"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c2", "c4"), List.of("c1"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1"), List.of("c2"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("c4");
        sim.drain();
        assertEquals(1, sim.logSize("c1"),
                "one business delivery at A must advertise exactly one null message on c1");
        assertEquals(0, sim.logSize("c2"),
                "B must not echo A's advertisement — its only news (c4@0) is custody for B, "
                        + "which folds into the stamp but never obliges a relay (the relay trigger scope)");
    }

    /**
     * The previously storming three-node cycle quiesces immediately: A's advertisement on c1
     * carries only c4@0, custody for B (blind to c4), so B folds it and stays silent; C and A
     * hear nothing further. Under the previous relay-on-any-news rule this exact spec stormed
     * forever (every member blind to one cycle channel, custody one lap stale, each relay's own
     * offset the next member's news) and was pinned as such until the trigger was restricted to
     * consumed channels.
     */
    @Test
    void threeNodeCycleGossipQuiescesWithoutRelays() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("c4"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c3", "c4"), List.of("c1"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1"), List.of("c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("c2"), List.of("c3"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("c4");
        sim.drain();
        assertEquals(1, sim.logSize("c1"),
                "A's single business delivery must advertise exactly one null message on c1");
        assertEquals(0, sim.logSize("c2"),
                "B must not relay: c4@0 is custody for B (blind channel), and relaying custody is "
                        + "the mechanism that made this cycle storm forever");
        assertEquals(0, sim.logSize("c3"),
                "C receives nothing, so c3 must stay empty — the cycle is causally quiet");
    }

    /**
     * The chorded three-node cycle — every member consumes TWO cycle channels, so multi-hop
     * paths exist on which a claim about a consumed channel can race its physical record (the
     * one shape where quiescence is fair-scheduling-conditional rather than unconditional; see
     * the fix's design notes). Under the sim's fair drain every such race resolves when the
     * record itself is delivered, and the single seeded business record must leave the cycle
     * channels almost silent.
     */
    @Test
    void chordedThreeNodeCycleQuiescesUnderFairDrain() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("c4"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c2", "c3", "c4"), List.of("c1"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1", "c3"), List.of("c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("c1", "c2"), List.of("c3"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("c4");
        sim.drain();
        assertEquals(1, sim.logSize("c1"),
                "A's single business delivery must advertise exactly one null message on c1");
        assertEquals(0, sim.logSize("c2"),
                "B consumes c1 and c3, but A's advertisement carries no news on either — c4@0 is "
                        + "custody for B and must not oblige a relay");
        assertEquals(0, sim.logSize("c3"),
                "C consumes c1 and c2, but A's advertisement carries no news on either — the "
                        + "chorded cycle must quiesce under fair scheduling");
    }

    /**
     * The all-shared-sinks three-node cycle: every cycle channel has two producers, so every
     * member holds produced-but-not-consumed channels whose sibling appends it covers only
     * through custody. This is the constructed counterexample that disqualified the
     * consumed-OR-produced trigger variant (sibling appends registered as produced-channel news
     * one lap late, sustaining ~6 null messages per lap forever under fair scheduling); under
     * the consumed-only trigger those claims are custody and the topology is silent after A's
     * two advertisements.
     */
    @Test
    void sharedSinkThreeNodeCycleQuiescesWithoutSiblingEcho() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("c4"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("A", List.of("c4", "c3"), List.of("c1", "c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("B", List.of("c1"), List.of("c2", "c3"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("C", List.of("c2"), List.of("c3", "c1"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("c4");
        sim.drain();
        assertEquals(1, sim.logSize("c1"),
                "A's advertisement reaches c1 exactly once — C must not append a sibling echo");
        assertEquals(1, sim.logSize("c2"),
                "A's advertisement reaches c2 exactly once — B must not append a sibling echo");
        assertEquals(0, sim.logSize("c3"),
                "no one relays: every claim B and C receive is custody (c4 blind; sibling shared-"
                        + "sink appends covered only by hearsay), which must never oblige a relay");
    }

    /**
     * The issue #32 shape, with the null-message relay isolated. Six nodes form a chorded cycle on
     * two shared sinks with <em>three</em> producers each — c2 from p1, p2, p3 and c3 from p1, p5,
     * p6 — and every cycle member (p2, p4, p5, p6) consumes both, one more producer per sink than
     * {@link #sharedSinkThreeNodeCycleQuiescesWithoutSiblingEcho}. The random-topology sweep
     * flagged this shape as non-quiescing, and the suspicion was a 3-producer relay the
     * consumed-channel trigger did not cover. It is not: with the delegates silent
     * ({@code p = 0.0}) so nothing but null messages ever reaches c2 and c3, a single external
     * record on c1 settles after a handful of advertisements. Each carried claim on a chord
     * (c3 news arriving on c2, or c2 news on c3) obliges at most one bounded relay before the
     * consumed channel's own frontier catches up — the chorded-cycle bound of the relay rule,
     * holding here with three siblings per sink exactly as with one. The sweep's storm was a
     * separate concern, business feedback amplification when the delegates <em>do</em> forward
     * (loop gain &gt; 1); that is pinned as a supercritical-topology classification in
     * {@link ParsleySupercriticalTopologyClassificationTest}, not a relay defect.
     */
    @Test
    void threeProducerSharedSinkChordedCycleQuiescesWithSilentDelegates() {
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(List.of("c1"), List.of(
                new ParsleySimTrace.SimSpec.NodeSpec("p1", List.of("c1"), List.of("c2", "c3"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("p2", List.of("c1", "c2", "c3"), List.of("c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("p3", List.of("c3"), List.of("c2"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("p4", List.of("c2", "c3"), List.of(), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("p5", List.of("c1", "c2", "c3"), List.of("c3"), 0.0),
                new ParsleySimTrace.SimSpec.NodeSpec("p6", List.of("c1", "c2", "c3"), List.of("c3"), 0.0)));
        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 1);
        sim.produceExternal("c1");
        sim.drain();
        assertEquals(2, sim.logSize("c2"),
                "the 3-producer shared sink c2 must quiesce after a bounded burst of null messages — "
                        + "the relay covers the chorded 3-producer shape, not just the 2-producer one");
        assertEquals(3, sim.logSize("c3"),
                "the 3-producer shared sink c3 must quiesce after a bounded burst of null messages, "
                        + "no sustained relay");
    }
}
