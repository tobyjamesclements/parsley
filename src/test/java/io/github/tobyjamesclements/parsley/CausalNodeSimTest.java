package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The main verification suite: {@link CausalNode} under the oracle across seeded random
 * interleavings. Every scenario runs many seeds; the oracle checks causal order, FIFO, and
 * duplicates on every delivery, and completeness plus drain on every run (obligations V1-V7
 * of docs/design/verification.md).
 *
 * <p>Each scenario also asserts anti-vacuity: the machinery under test must actually have
 * fired (records held at the gate, crashes injected, position advances taken). A suite that
 * passes because nothing interesting happened proves nothing.
 */
class CausalNodeSimTest {

    private static final int SEEDS = 100;
    private static final long BUDGET = 200_000;

    private static final class Stats {
        long deliveries, holds, crashes, positionAdvances;

        void add(SimWorld w) {
            deliveries += w.totalDeliveries;
            holds += w.totalHolds;
            crashes += w.totalCrashes;
            positionAdvances += w.totalPositionAdvances;
        }
    }

    private static BiFunction<NodeConfig, SimNode, DeliveryProtocol> real() {
        return (config, host) -> new CausalNode(config, host.store, host.offsets);
    }

    /** V1: the core race — a stage's output must never overtake its input at a shared consumer. */
    @Test
    void sharedInputDiamond() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1);
            w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("C", 0, List.of("t1:0", "t2:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 8; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record: the race never occurred");
    }

    /**
     * V1 transitively: claims must survive a hop through a node that does not consume t1 —
     * including across that hop's crashes, which exercise the restore of the advertised
     * clocks (custody lost at restore would silently under-claim every later stamp).
     */
    @Test
    void transitiveClaimsThroughBlindHop() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1)
                    .crashBudget(3);
            w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("B", 0, List.of("t2:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            w.node("C", 0, List.of("t3:0", "t1:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 8; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record: transitive claims untested");
        assertTrue(stats.crashes > 0, "no crash was ever injected on the blind hop");
    }

    /** V1 at the edge: a sequential plain producer's cross-topic sends stay ordered. */
    @Test
    void edgeProducerCrossTopicOrder() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1a", 1).topic("t1b", 1);
            w.node("C", 0, List.of("t1a:0", "t1b:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 8; i++) {
                p.produce(i % 2 == 0 ? "t1a" : "t1b", 0, "k" + i, "v" + i, 1000 + i);
            }
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record: edge stamps untested");
    }

    /** V1 via observation: an edge producer that observed a stage's output before producing. */
    @Test
    void edgeObserverCreatesCrossTopicCausality() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
            w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("D", 0, List.of("t3:0", "t2:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer feed = w.producer("feed");
            for (int i = 0; i < 5; i++) feed.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            EdgeProducer observer = w.producer("observer");
            for (int i = 0; i < 5; i++) {
                observer.pollOne("t2", 0);
                observer.produce("t3", 0, "o" + i, "w" + i, 2000 + i);
            }
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record: observed causality untested");
    }

    /** V3: crashes with EOS restore — aborted records, re-inits, end-offset seeds. */
    @Test
    void crashRecoveryUnderChain() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1)
                    .crashBudget(4);
            w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            w.node("C", 0, List.of("t3:0", "t2:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 10; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.crashes > 0, "no crash was ever injected");
        assertTrue(stats.holds > 0, "the gate never held a record under crashes");
    }

    /** V4/V6: a filter stage leaves quiet sinks; trailing markers must not wedge anything. */
    @Test
    void filterStageAndTrailingMarkers() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1)
                    .crashBudget(2);
            w.node("F", 0, List.of("t1:0"), List.of("t2"), SimBehavior.filter(3, "t2"), real());
            w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            w.node("C", 0, List.of("t3:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 12; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.positionAdvances > 0, "position advances never fired: markers untested");
    }

    /** Multi-partition sinks: claims across partitions of one topic (the crossing wait). */
    @Test
    void multiPartitionSinkOrdering() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 2);
            w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("C", 0, List.of("t2:0", "t2:1"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 10; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record: cross-partition order untested");
    }

    /** Cycles: a feedback loop with gain below one drains and stays causal. */
    @Test
    void cycleWithDampedFeedback() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
            w.node("A", 0, List.of("t1:0", "t3:0"), List.of("t2"), SimBehavior.ttlForward("t2"), real());
            w.node("B", 0, List.of("t2:0"), List.of("t3"), SimBehavior.ttlForward("t3"), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1", 0, "k" + i, String.valueOf(4 + i % 3), 1000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
    }

    /** V7: truncation below a true stability bound must not disturb later traffic. */
    @Test
    void truncationBelowStabilityIsSound() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
            SimNode a = w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            SimNode b = w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            SimNode c = w.node("C", 0, List.of("t3:0", "t2:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);

            // Drained: every appended offset is delivered everywhere it is consumed, so the
            // current end offsets are a true global stability bound.
            VectorClock stability = new VectorClock();
            for (var ch : w.broker.allChannels()) {
                long end = w.broker.endOffset(ch);
                if (end > 0) stability.advanceTo(ch, end - 1);
            }
            for (SimNode n : List.of(a, b, c)) n.protocol.truncate(stability);

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) p2.produce("t1", 0, "k2" + i, "v2" + i, 3000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record after truncation");
    }

    /**
     * Soak: a wider topology — multi-partition topics, a filter, a damped feedback loop, a
     * blind hop, and crash injection — under many seeds. The kitchen sink obligation.
     */
    @Test
    void soakWideTopologyWithCrashes() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed)
                    .topic("t1", 2).topic("t2", 2).topic("t3", 1).topic("t4", 1).topic("t5", 1)
                    .crashBudget(6);
            w.node("A0", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("A1", 1, List.of("t1:1"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            w.node("B", 0, List.of("t2:0", "t2:1"), List.of("t3"), SimBehavior.filter(2, "t3"), real());
            w.node("D", 0, List.of("t3:0", "t4:0"), List.of("t5"), SimBehavior.ttlForward("t5"), real());
            w.node("E", 0, List.of("t5:0", "t1:0"), List.of("t4"), SimBehavior.filter(3, "t4"), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 12; i++) {
                p.produce("t1", i % 2, "k" + i, String.valueOf(3 + i % 4), 1000 + i);
            }
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the gate never held a record in the soak");
        assertTrue(stats.crashes > 0, "no crash was ever injected in the soak");
        assertTrue(stats.positionAdvances > 0, "position advances never fired in the soak");
    }

    /**
     * The documented caveat of sequence claims, demonstrated both ways: a sequence-form claim
     * frozen in a non-consumer's custody clock wedges a late joiner whose baseline sits above
     * the claimed record (the sender never acknowledged, so the claim never normalised, and
     * the sender never writes that partition again). The same topology with a from-the-start
     * joiner resolves and drains. An offset-claims design has no such window; late joiners
     * under sequence claims must either baseline at the stable offset (LSO) before any live
     * claim, or accept this wedge class.
     */
    @Test
    void lateJoinerStaleSequenceClaimWedges() {
        assertTrue(runStaleClaimTopology(7L, true), "expected the late joiner to wedge");
        assertTrue(!runStaleClaimTopology(7L, false), "the from-the-start joiner must not wedge");
    }

    /** @return true when the world failed to satisfy completeness or drain (the wedge). */
    private boolean runStaleClaimTopology(long seed, boolean joinAtLatest) {
        SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 2).topic("t3", 1);
        w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
        w.node("B", 0, List.of("t2:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
        if (!joinAtLatest) {
            w.node("L", 0, List.of("t3:0", "t2:1"), List.of(), SimBehavior.consumeOnly(), real());
        }

        // Phase 1: first a record A forwards to t2:1 — minting a sequence claim that never
        // normalises sender-side (there is no acknowledgement feed) — then t2:0 traffic whose
        // stamps carry that claim into B's custody.
        EdgeProducer p = w.producer("edge");
        p.produce("t1", 0, keyForPartition(w, 1, "a"), "v0", 1000);
        for (int i = 0, sent = 1; sent < 6; i++) {
            String k = "b" + i;
            if (w.routeByKey("t2", k.getBytes()).partition() != 0) continue;
            p.produce("t1", 0, k, "v" + sent, 1000 + sent);
            sent++;
        }
        w.run(BUDGET);

        if (joinAtLatest) {
            w.nodeAtLatest("L", 0, List.of("t3:0", "t2:1"), List.of(),
                    SimBehavior.consumeOnly(), real());
        }

        // Phase 2: traffic that reaches L through t3 but never writes t2:1 again.
        EdgeProducer p2 = w.producer("edge2");
        for (int i = 0, sent = 0; sent < 6; i++) {
            String k = "m" + i;
            if (w.routeByKey("t2", k.getBytes()).partition() != 0) continue;
            p2.produce("t1", 0, k, "w" + sent, 3000 + sent);
            sent++;
        }
        try {
            w.run(BUDGET);
            return false;
        } catch (AssertionError wedge) {
            String msg = String.valueOf(wedge.getMessage());
            assertTrue(msg.contains("never delivered") || msg.contains("did not drain"),
                    "unexpected failure mode: " + wedge);
            return true;
        }
    }

    /** The first key with the given suffix family routing to the wanted t2 partition. */
    private static String keyForPartition(SimWorld w, int partition, String family) {
        for (int i = 0; i < 1000; i++) {
            String k = family + i;
            if (w.routeByKey("t2", k.getBytes()).partition() == partition) return k;
        }
        throw new IllegalStateException("no key found for partition " + partition);
    }

    /**
     * The log-start stability protocol: after retention deletes the consumed history, the
     * log-start bound truncates every stamp-side clock to empty with zero coordination, and
     * later traffic stays causal. The width assertion is the point of the whole mechanism.
     */
    @Test
    void logStartStabilityTruncatesToEmpty() {
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
            SimNode a = w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            SimNode b = w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            SimNode c = w.node("C", 0, List.of("t3:0", "t2:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);

            int widthBefore = 0;
            for (SimNode n : List.of(a, b, c)) widthBefore += n.protocol.stampChannels().size();
            assertTrue(widthBefore > 0, "no stamp-side entries accumulated: nothing to truncate");

            // Retention consumes the whole drained history; log starts are the stability bound.
            for (String t : List.of("t1", "t2", "t3")) {
                w.advanceLogStart(t, 0, w.broker.endOffset(w.broker.channel(t, 0)));
            }
            for (SimNode n : List.of(a, b, c)) truncateFromLogStarts(n);
            for (SimNode n : List.of(a, b, c)) {
                assertTrue(n.protocol.stampChannels().isEmpty(),
                        n.name + " still carries stamp-side entries after full-retention truncation");
            }

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) p2.produce("t1", 0, "k2" + i, "v2" + i, 3000 + i);
            w.run(BUDGET);
        }
    }

    /**
     * The soundness-critical joiner case a membership-based stability protocol gets wrong: a
     * consumer joining from earliest after truncation. Its baseline is the log start
     * (retention already deleted everything below), so the truncated claims are out of its
     * scope by the same rule that exempts seeds — the oracle confirms causal order for
     * everything it can see.
     */
    @Test
    void fromEarliestJoinerAfterTruncationStaysCausal() {
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
            SimNode a = w.node("A", 0, List.of("t1:0"), List.of("t2"), SimBehavior.forwardTo("t2"), real());
            SimNode b = w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"), SimBehavior.forwardTo("t3"), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);

            for (String t : List.of("t1", "t2", "t3")) {
                w.advanceLogStart(t, 0, w.broker.endOffset(w.broker.channel(t, 0)));
            }
            for (SimNode n : List.of(a, b)) truncateFromLogStarts(n);

            // Joins from earliest: position resets to the log start, which is its baseline.
            w.node("L", 0, List.of("t3:0", "t1:0"), List.of(), SimBehavior.consumeOnly(), real());

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) p2.produce("t1", 0, "k2" + i, "v2" + i, 3000 + i);
            w.run(BUDGET);
        }
    }

    /** Runs the coordination-free truncation driver: log starts in, truncation out. */
    private static void truncateFromLogStarts(SimNode n) {
        var node = (CausalNode) n.protocol;
        var earliest = n.offsets.earliestOffsets(node.stampChannels());
        node.truncateToLogStarts(earliest.logStarts(), earliest.confirmedAbsent());
        n.store.commit();
    }

    /**
     * Scope shrink: a dropped input's causal past must survive in later stamps (carried
     * ancestry). The consumer that makes the carried claims load-bearing joins fresh after
     * the shrink: it must order A's post-shrink outputs after their t1b causes, which only
     * the carried entries still claim. Afterwards, retention truncates the carried ancestry
     * itself (the one stamp-side clock the plain truncation scenarios leave empty).
     */
    @Test
    void rescopeShrinkCarriesAncestry() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1a", 1).topic("t1b", 1).topic("t2", 1);
            SimNode a = w.node("A", 0, List.of("t1a:0", "t1b:0"), List.of("t2"),
                    SimBehavior.forwardTo("t2"), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) {
                p.produce(i % 2 == 0 ? "t1a" : "t1b", 0, "k" + i, "v" + i, 1000 + i);
            }
            w.run(BUDGET);

            // Drop t1b from A's inputs across a restart; its delivered past must be carried.
            a.crashIdle();
            a.reconfigure(w.config("A", 0, List.of("t1a:0"), List.of("t2")),
                    SimBehavior.forwardTo("t2"));
            a.start();

            // A fresh consumer of t2 and t1b: A's post-shrink outputs reach it claiming the
            // t1b past through carried ancestry alone.
            w.node("C2", 0, List.of("t2:0", "t1b:0"), List.of(), SimBehavior.consumeOnly(), real());

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) p2.produce("t1a", 0, "k2" + i, "v2" + i, 3000 + i);
            w.run(BUDGET);
            stats.add(w);

            assertTrue(!a.protocol.stampChannels().isEmpty(),
                    "carried ancestry never populated: the shrink carried nothing");
            for (String t : List.of("t1a", "t1b", "t2")) {
                w.advanceLogStart(t, 0, w.broker.endOffset(w.broker.channel(t, 0)));
            }
            truncateFromLogStarts(a);
            assertTrue(a.protocol.stampChannels().isEmpty(),
                    "carried ancestry survived a full-retention truncation");
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
        assertTrue(stats.holds > 0, "the carried claims never actually gated anything");
    }

    /**
     * Scope growth seeded at carried knowledge: a former sink becomes an input. The heal
     * folds the sink's end offsets into carried ancestry, the growth seed starts the frontier
     * there, and the resume position skips the node's own outputs — it must not re-deliver
     * what its stamps already claimed.
     */
    @Test
    void rescopeGrowthSeedsAtCarriedKnowledge() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1a", 1).topic("t1b", 1).topic("t2", 1);
            SimNode a = w.node("A", 0, List.of("t1a:0"), List.of("t1b"),
                    SimBehavior.forwardTo("t1b"), real());
            w.node("C", 0, List.of("t2:0", "t1b:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1a", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);

            // t1b flips from sink to input across a restart.
            a.crashIdle();
            a.reconfigure(w.config("A", 0, List.of("t1a:0", "t1b:0"), List.of("t2")),
                    SimBehavior.forwardTo("t2"));
            a.start();
            assertTrue(a.position(w.broker.channel("t1b", 0)) > 0,
                    "growth did not seed at carried knowledge: A would re-deliver its own outputs");

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) {
                p2.produce(i % 2 == 0 ? "t1a" : "t1b", 0, "k2" + i, "v2" + i, 3000 + i);
            }
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
    }
}
