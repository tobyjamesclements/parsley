package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.CausalNode;
import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.NodeConfig;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;
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
        return (config, host) -> new CausalNode(config, host.store, host.sends);
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

    /** V1 transitively: claims must survive a hop through a node that does not consume t1. */
    @Test
    void transitiveClaimsThroughBlindHop() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1);
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
            Clock stability = new Clock();
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

    /** Fail closed: an observed send failure kills the stamp and its transaction, then heals. */
    @Test
    void sendFailureFailsClosedAndRecovers() {
        int failuresSeen = 0;
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1)
                    .allowTaskFailures();
            SimNode a = w.node("A", 0, List.of("t1:0"), List.of("t2", "t3"),
                    SimBehavior.forwardTo("t2", "t3"), real());
            w.node("C", 0, List.of("t2:0", "t3:0"), List.of(), SimBehavior.consumeOnly(), real());
            a.sends.failNextAwait();
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);
            failuresSeen += w.taskFailures().size();
        }
        assertTrue(failuresSeen > 0, "the armed send failure never surfaced as a task failure");
    }

    /** Scope shrink: a dropped input's causal past must survive in later stamps (carried). */
    @Test
    void rescopeShrinkCarriesAncestry() {
        Stats stats = new Stats();
        for (long seed = 0; seed < SEEDS; seed++) {
            SimWorld w = new SimWorld(seed).topic("t1a", 1).topic("t1b", 1).topic("t2", 1);
            SimNode a = w.node("A", 0, List.of("t1a:0", "t1b:0"), List.of("t2"),
                    SimBehavior.forwardTo("t2"), real());
            w.node("C", 0, List.of("t2:0", "t1b:0"), List.of(), SimBehavior.consumeOnly(), real());
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 6; i++) {
                p.produce(i % 2 == 0 ? "t1a" : "t1b", 0, "k" + i, "v" + i, 1000 + i);
            }
            w.run(BUDGET);

            // Drop t1b from A's inputs across a restart; its delivered past must be carried.
            a.crashIdle();
            a.reconfigure(w.config("A", 0, List.of("t1a:0"), List.of("t2")));
            a.start();

            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 6; i++) p2.produce("t1a", 0, "k2" + i, "v2" + i, 3000 + i);
            w.run(BUDGET);
            stats.add(w);
        }
        assertTrue(stats.deliveries > 0, "no deliveries at all");
    }
}
