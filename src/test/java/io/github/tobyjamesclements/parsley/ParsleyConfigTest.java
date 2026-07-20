package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyConfig}'s own configuration loading: building from explicit properties via
 * {@link ParsleyConfig#from(Properties)}, the {@code parsley.topology.validation} value's validation,
 * and {@link ParsleyConfig#load()}'s classpath-resource fallback to defaults. Causal delivery is
 * strictly fail-closed, so there are no failure-policy settings to configure — the only key is
 * {@code parsley.topology.validation}.
 */
class ParsleyConfigTest {

    private static final String TOPOLOGY_VALIDATION = "parsley.topology.validation";

    /**
     * With no {@code parsley.topology.validation} set, {@link ParsleyConfig#load()} reads the
     * {@code parsley.properties} classpath resource (absent in this project's test classpath) and
     * falls back to the default {@code warn} mode rather than throwing — detectable
     * misconfigurations are logged but do not fail the task, so an existing deployment is never
     * broken by the check.
     *
     * Asserts the default validation mode is {@code WARN}.
     */
    @Test
    void topologyValidationDefaultsToWarn() {
        assertEquals(ParsleyConfig.ValidationMode.WARN, ParsleyConfig.load().topologyValidation(),
                "with no parsley.topology.validation set, the default mode must be 'warn'");
    }

    /**
     * Setting {@code parsley.topology.validation} to {@code strict} overrides the default so a
     * detectable topology misconfiguration fails the task fast; {@code off} disables the check.
     *
     * Asserts both explicit modes parse to their enum values.
     */
    @Test
    void fromAppliesExplicitTopologyValidationModes() {
        Properties strict = new Properties();
        strict.setProperty(TOPOLOGY_VALIDATION, "strict");
        assertEquals(ParsleyConfig.ValidationMode.STRICT, ParsleyConfig.from(strict).topologyValidation(),
                "an explicit 'strict' topology-validation mode must parse to STRICT");

        Properties off = new Properties();
        off.setProperty(TOPOLOGY_VALIDATION, "off");
        assertEquals(ParsleyConfig.ValidationMode.OFF, ParsleyConfig.from(off).topologyValidation(),
                "an explicit 'off' topology-validation mode must parse to OFF");
    }

    /**
     * An unrecognised {@code parsley.topology.validation} value (none of {@code off}/{@code warn}/
     * {@code strict}) is rejected rather than silently defaulting.
     *
     * Asserts that {@code IllegalStateException} is thrown for a garbage validation-mode value.
     */
    @Test
    void fromRejectsAnInvalidTopologyValidationValue() {
        Properties props = new Properties();
        props.setProperty(TOPOLOGY_VALIDATION, "loud");

        assertThrows(IllegalStateException.class, () -> ParsleyConfig.from(props),
                "an unrecognised topology-validation value must be rejected");
    }

    /**
     * Every removed {@code parsley.coordination.*} key fails loudly at parse: the coordination
     * subsystem is deleted (joins need zero coordination), and a key that wires nothing must not be
     * accepted quietly — an operator who believes their deployment is coordinated should learn about
     * the removal at startup, from a message naming the offending key.
     *
     * Asserts each removed key alone throws {@code IllegalStateException} naming both the key and
     * the removal.
     */
    @Test
    void fromRejectsEveryRemovedCoordinationKey() {
        for (String key : Set.of(
                "parsley.coordination.epoch-events-topic",
                "parsley.coordination.domain-topics",
                "parsley.coordination.member-apps")) {
            Properties props = new Properties();
            props.setProperty(key, "anything");
            IllegalStateException thrown = assertThrows(IllegalStateException.class,
                    () -> ParsleyConfig.from(props),
                    "the removed key " + key + " must fail configuration parsing loudly");
            assertTrue(thrown.getMessage().contains(key),
                    "the failure must name the offending key: " + thrown.getMessage());
            assertTrue(thrown.getMessage().contains("removed"),
                    "the failure must name the removal, not read as an unknown-key typo: "
                            + thrown.getMessage());
        }
    }
}
