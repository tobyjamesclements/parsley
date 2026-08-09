package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.LongStream;

import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomised, seeded scenarios over the real engine under a host honouring the Host obligations. Every committed
 * history must satisfy causal delivery (SPEC Safety 1), at-most-once (Safety 2), per-channel order (Safety 3),
 * complete cause expression (Structural 12/14/15), and quiescent liveness (Liveness 1/3/5). A failure prints its
 * seed; re-running that seed reproduces the run exactly.
 */
class CausalOrderPropertyTest {

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 300);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void randomScenarioIsCausallyConsistent(long seed) {
        Scenario.Result result = Scenario.run(seed, SabotageMode.NONE);
        assertTrue(result.clean(), () -> "seed " + seed + " violations:\n  "
                + String.join("\n  ", result.violations())
                + (result.failure() == null ? "" : "\nfailure: " + result.failure()));
    }
}
