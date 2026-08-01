package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification obligation V8: the harness itself must be able to catch a broken protocol. A
 * protocol that delivers on arrival, ignoring dependencies, must trip the oracle's causal
 * check on racy schedules. If this test fails, the oracle is blind and every green simulation
 * result is meaningless.
 */
class OracleSelfTest {

    /**
     * Diamond race: an edge producer feeds t1, stage A forwards t1 into t2, and a broken stage
     * C consumes both t1 and t2 eagerly. On any schedule where C polls a t2 record before its t1
     * cause, the oracle must flag the delivery. Across 40 seeds at least one such schedule must
     * occur, and in practice most do.
     */
    @Test
    void eagerDeliveryIsCaughtByTheOracle() {
        int violations = 0;
        for (long seed = 0; seed < 40; seed++) {
            try {
                SimWorld world = new SimWorld(seed)
                        .topic("t1", 1)
                        .topic("t2", 1);
                world.node("A", 0, List.of("t1:0"), List.of("t2"),
                        SimBehavior.forwardTo("t2"), (config, host) -> new EagerProtocol());
                world.node("C", 0, List.of("t1:0", "t2:0"), List.of(),
                        SimBehavior.consumeOnly(), (config, host) -> new EagerProtocol());
                EdgeProducer p = world.producer("edge");
                for (int i = 0; i < 6; i++) {
                    p.produce("t1", 0, "k" + i, "v" + i, 1000 + i);
                }
                world.run(10_000);
            } catch (AssertionError e) {
                assertTrue(String.valueOf(e.getMessage()).contains("CAUSAL VIOLATION"),
                        "expected a causal violation, got: " + e);
                violations++;
            }
        }
        assertTrue(violations > 0,
                "the oracle never flagged an eager-delivery protocol across 40 seeds; "
                        + "the harness cannot detect causal violations");
    }
}
