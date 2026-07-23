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
     * falls back to the default {@code strict} mode — a detectable topology misconfiguration is a
     * deferred failure anyway (a parity mismatch crash-loops the marker produce at runtime; a
     * compacted sink silently loses null messages), so the default fails once, clearly, at init.
     *
     * Asserts the default validation mode is {@code STRICT}.
     */
    @Test
    void topologyValidationDefaultsToStrict() {
        assertEquals(ParsleyConfig.ValidationMode.STRICT, ParsleyConfig.load().topologyValidation(),
                "with no parsley.topology.validation set, the default mode must be 'strict'");
    }

    /**
     * Setting {@code parsley.topology.validation} to {@code warn} opts down from the strict
     * default so a detectable topology misconfiguration is logged but does not fail the task;
     * {@code off} disables the check entirely.
     *
     * Asserts both explicit modes parse to their enum values.
     */
    @Test
    void fromAppliesExplicitTopologyValidationModes() {
        Properties warn = new Properties();
        warn.setProperty(TOPOLOGY_VALIDATION, "warn");
        assertEquals(ParsleyConfig.ValidationMode.WARN, ParsleyConfig.from(warn).topologyValidation(),
                "an explicit 'warn' topology-validation mode must parse to WARN");

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
