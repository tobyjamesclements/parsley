package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the alignment the {@link EngineTestFactory} bridge silently assumes. The production
 * {@link Sabotage.Mode} stays package-private (the public API offers no way to enable a
 * fault), so the factory mirrors it as {@link EngineTestFactory.SabotageMode} and crosses
 * with {@code Sabotage.Mode.valueOf(mode.name())} — which fails only at runtime, and only
 * on the first test that uses the misaligned constant.
 */
class SabotageModeAlignmentTest {

    /**
     * A constant added, removed or renamed on either side must fail here, by name, not in
     * an unrelated sabotage test's stack trace. Order is asserted too: the enums are
     * mirrors, and a mirror that reorders is halfway to diverging.
     */
    @Test
    void factoryModesMirrorProductionModesExactly() {
        List<String> factoryModes = Arrays.stream(EngineTestFactory.SabotageMode.values())
                .map(Enum::name)
                .filter(name -> !name.equals("NONE"))
                .toList();
        List<String> productionModes = Arrays.stream(Sabotage.Mode.values())
                .map(Enum::name)
                .toList();
        assertEquals(productionModes, factoryModes,
                "EngineTestFactory.SabotageMode minus NONE must mirror Sabotage.Mode exactly and in order;"
                        + " EngineTestFactory.create's valueOf bridge assumes it");
    }
}
