package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The host-side analogue of {@link OracleSelfTest}: the conformance kit must be able to catch a
 * broken <em>host</em>, not only a broken protocol. Each test runs the real {@link CausalNode}
 * under a {@link SimNode.HostFault} that shirks one duty of the host contract
 * (docs/reference/conformance-kit.md) and requires the kit to flag some seed. Any loud failure
 * counts as a flag — which oracle or world check fires first depends on the schedule; what the
 * kit must never do is stay green.
 */
class HostContractSelfTest {

    private static final int SEEDS = 40;
    private static final long BUDGET = 200_000;

    private static BiFunction<NodeConfig, SimNode, DeliveryProtocol> real() {
        return (config, host) -> new CausalNode(config, host.store, host.offsets);
    }

    /**
     * H3, step-atomicity: a host that commits its store while the transaction aborts leaves
     * protocol state ahead of the world it claims to describe. Across the crash-chain topology
     * the kit must flag it — as a duplicate delivery, a causal violation, a completeness
     * failure, or a loud protocol failure at restore — on at least one seed.
     */
    @Test
    void storeCommittedOnCrashIsCaughtByTheKit() {
        int flagged = runFaultySeeds(SimNode.HostFault.COMMIT_STORE_ON_CRASH, 4);
        assertTrue(flagged > 0,
                "a host that breaks store/transaction atomicity was never flagged across "
                        + SEEDS + " seeds; the kit cannot detect a non-atomic host");
    }

    /**
     * H2, the liveness signal: a host that never reports consumer position advances withholds
     * the one skip no later-fetched record reveals — a trailing marker under an
     * end-offset-seed claim (docs/foundations/liveness.md#position-advance-bridging). The
     * scenario builds that wedge structurally: D declares sink t2 but never writes it, so
     * restarting D after t2 has gone quiet under a trailing marker mints a seed claim naming
     * that marker's offset, and D's later sends carry the claim to C on t4. An honest host's
     * position advance on t2 releases C — the control run must drain clean — while the faulty
     * host strands the t4 records, and the kit must flag them at drain on at least one seed.
     */
    @Test
    void droppedPositionAdvancesAreCaughtByTheKit() {
        int flagged = 0;
        for (long seed = 0; seed < SEEDS; seed++) {
            assertTrue(!runQuietSinkTopology(seed, SimNode.HostFault.NONE),
                    "the honest host failed the quiet-sink topology on seed " + seed
                            + "; the scenario is broken, so its faulty run proves nothing");
            if (runQuietSinkTopology(seed, SimNode.HostFault.DROP_POSITION_ADVANCE)) flagged++;
        }
        assertTrue(flagged > 0,
                "a host that drops the position-advance signal was never flagged across "
                        + SEEDS + " seeds; the kit cannot detect a withheld liveness signal");
    }

    /** @return true when the kit flagged the run with a loud failure. */
    private static boolean runQuietSinkTopology(long seed, SimNode.HostFault fault) {
        try {
            SimWorld w = new SimWorld(seed)
                    .topic("t1", 1).topic("t2", 1).topic("t4", 1).topic("t5", 1);
            w.node("A", 0, List.of("t1:0"), List.of("t2"),
                    SimBehavior.forwardTo("t2"), real()).fault = fault;
            SimNode d = w.node("D", 0, List.of("t5:0"), List.of("t2", "t4"),
                    SimBehavior.forwardTo("t4"), real());
            d.fault = fault;
            w.node("C", 0, List.of("t4:0", "t2:0"), List.of(),
                    SimBehavior.consumeOnly(), real()).fault = fault;

            // Phase 1: t2 gains business records and a trailing commit marker, then quiet.
            EdgeProducer p = w.producer("edge");
            for (int i = 0; i < 4; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
            w.run(BUDGET);

            // D restarts: its end-offset seed over the never-written sink t2 now claims the
            // trailing marker's offset.
            d.crashIdle();
            d.start();

            // Phase 2: D's sends to t4 carry the claim; nothing writes t2 ever again.
            EdgeProducer p2 = w.producer("edge2");
            for (int i = 0; i < 4; i++) p2.produce("t5", 0, "m" + i, "w" + i, 3000 + i);
            w.run(BUDGET);
            return false;
        } catch (AssertionError | RuntimeException loudFailure) {
            return true;
        }
    }

    /**
     * Runs the crash-recovery chain of {@link CausalNodeSimTest} with every node's host
     * shirking the given duty, returning how many seeds the kit flagged.
     */
    private static int runFaultySeeds(SimNode.HostFault fault, int crashBudget) {
        int flagged = 0;
        for (long seed = 0; seed < SEEDS; seed++) {
            try {
                SimWorld w = new SimWorld(seed).topic("t1", 1).topic("t2", 1).topic("t3", 1)
                        .crashBudget(crashBudget);
                w.node("A", 0, List.of("t1:0"), List.of("t2"),
                        SimBehavior.forwardTo("t2"), real()).fault = fault;
                w.node("B", 0, List.of("t2:0", "t1:0"), List.of("t3"),
                        SimBehavior.forwardTo("t3"), real()).fault = fault;
                w.node("C", 0, List.of("t3:0", "t2:0"), List.of(),
                        SimBehavior.consumeOnly(), real()).fault = fault;
                EdgeProducer p = w.producer("edge");
                for (int i = 0; i < 10; i++) p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
                w.run(BUDGET);
            } catch (AssertionError | RuntimeException loudFailure) {
                flagged++;
            }
        }
        return flagged;
    }
}
