package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.LongStream;

import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sweeps seeded random scenarios against the oracle.
 *
 * <p>Every run is deterministic in its seed, so a failure reproduces exactly.
 */
class CausalOrderPropertyTest {
    static LongStream seeds() {
        return LongStream.rangeClosed(1, 300);
    }

    /** Every seeded scenario satisfies the oracle's causal order checks. */
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void randomScenarioIsCausallyConsistent(long seed) {
        Scenario.Result result = Scenario.run(seed, SabotageMode.NONE);
        assertTrue(result.clean(), () -> "seed " + seed + " violations:\n  "
                + String.join("\n  ", result.violations())
                + (result.failure() == null ? "" : "\nfailure: " + result.failure()));
    }
}
