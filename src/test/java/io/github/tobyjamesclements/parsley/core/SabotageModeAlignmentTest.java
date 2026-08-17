package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the alignment the {@link EngineTestFactory} bridge silently assumes. The production
 * {@link Sabotage.Mode} stays package-private (the public API offers no way to enable a
 * fault, SPEC Structural 9), so the factory mirrors it as
 * {@link EngineTestFactory.SabotageMode} — nameable from {@code parsley.sim} — and crosses
 * with {@code Sabotage.Mode.valueOf(mode.name())}, which fails only at runtime, and only
 * on the first test that uses the misaligned constant.
 */
class SabotageModeAlignmentTest {

    /**
     * A constant added, removed or renamed on either side must fail here, by name, not in
     * an unrelated sabotage test's stack trace. The comparison is a set: the valueOf
     * bridge is name-based and order-free, so a readability reorder must not redden the
     * build. The {@code NONE} sentinel is excluded by identity, before the name mapping,
     * so an IDE rename of the constant cannot make the filter silently inert.
     */
    @Test
    void factoryModesMirrorProductionModesExactly() {
        Set<String> factoryModes = Arrays.stream(EngineTestFactory.SabotageMode.values())
                .filter(mode -> mode != EngineTestFactory.SabotageMode.NONE)
                .map(Enum::name)
                .collect(Collectors.toSet());
        Set<String> productionModes = Arrays.stream(Sabotage.Mode.values())
                .map(Enum::name)
                .collect(Collectors.toSet());
        assertEquals(productionModes, factoryModes,
                "EngineTestFactory.SabotageMode minus NONE must mirror Sabotage.Mode name-for-name;"
                        + " EngineTestFactory.create's valueOf bridge assumes it");
    }
}
