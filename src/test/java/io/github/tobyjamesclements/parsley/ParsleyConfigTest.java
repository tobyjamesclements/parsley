package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyConfig}'s own configuration loading: building from explicit properties via
 * {@link ParsleyConfig#from(Properties)}, the {@code parsley.buffer.deserialization.failure.policy},
 * {@code parsley.buffer.eviction.failure.policy}, and {@code parsley.clock.resolution.failure.policy}
 * values' validation, and {@link ParsleyConfig#load()}'s classpath-resource fallback to defaults.
 */
class ParsleyConfigTest {

    private static final String FAILURE_POLICY = "parsley.buffer.deserialization.failure.policy";
    private static final String EVICTION_POLICY = "parsley.buffer.eviction.failure.policy";
    private static final String CLOCK_POLICY = "parsley.clock.resolution.failure.policy";
    private static final String TOPOLOGY_VALIDATION = "parsley.topology.validation";

    /**
     * An unrecognised {@code parsley.buffer.deserialization.failure.policy} value (neither {@code fail}
     * nor {@code continue}) is rejected rather than silently defaulting.
     *
     * Asserts that {@code IllegalStateException} is thrown for a garbage policy value.
     */
    @Test
    void fromRejectsAnInvalidFailurePolicyValue() {
        Properties props = new Properties();
        props.setProperty(FAILURE_POLICY, "explode");

        assertThrows(IllegalStateException.class, () -> ParsleyConfig.from(props),
                "an unrecognised failure-policy value must be rejected");
    }

    /**
     * An unrecognised {@code parsley.buffer.eviction.failure.policy} value (neither {@code fail} nor
     * {@code continue}) is rejected rather than silently defaulting.
     *
     * Asserts that {@code IllegalStateException} is thrown for a garbage policy value.
     */
    @Test
    void fromRejectsAnInvalidEvictionPolicyValue() {
        Properties props = new Properties();
        props.setProperty(EVICTION_POLICY, "explode");

        assertThrows(IllegalStateException.class, () -> ParsleyConfig.from(props),
                "an unrecognised eviction-policy value must be rejected");
    }

    /**
     * An unrecognised {@code parsley.clock.resolution.failure.policy} value (neither {@code fail} nor
     * {@code continue}) is rejected rather than silently defaulting.
     *
     * Asserts that {@code IllegalStateException} is thrown for a garbage policy value.
     */
    @Test
    void fromRejectsAnInvalidClockResolutionPolicyValue() {
        Properties props = new Properties();
        props.setProperty(CLOCK_POLICY, "explode");

        assertThrows(IllegalStateException.class, () -> ParsleyConfig.from(props),
                "an unrecognised clock-resolution-policy value must be rejected");
    }

    /**
     * {@code ParsleyConfig.load()} reads the {@code parsley.properties} classpath resource (absent in
     * this project's test classpath) and falls back to defaults rather than throwing — the default
     * deserialization-failure policy is {@code fail} (do not skip), the default eviction-failure
     * policy is also {@code fail} (fail fast rather than evict and forward), and the default
     * clock-resolution-failure policy is {@code fail} (fail fast rather than forward empty).
     *
     * Asserts that the loaded config does not skip on decode failure and does fail fast on both
     * eviction and an unresolvable clock.
     */
    @Test
    void loadFallsBackToDefaultsWhenNoResourceIsPresentOnTheClasspath() {
        ParsleyConfig config = ParsleyConfig.load();

        assertFalse(config.skipOnDecodeFailure(),
                "with no parsley.properties on the classpath, the default policy 'fail' must apply");
        assertTrue(config.failOnEvictionLimit(),
                "with no parsley.properties on the classpath, the default eviction policy 'fail' must apply");
        assertTrue(config.failOnUnresolvableClock(),
                "with no parsley.properties on the classpath, the default clock-resolution policy 'fail' must apply");
    }

    /**
     * Setting {@code parsley.clock.resolution.failure.policy} to {@code continue} overrides the
     * default, so an undecodable causal-dependencies header forwards the record with empty
     * dependencies instead of failing the task fast.
     *
     * Asserts that the effective config no longer fails fast on an unresolvable clock.
     */
    @Test
    void fromAppliesAnExplicitContinueClockResolutionPolicy() {
        Properties props = new Properties();
        props.setProperty(CLOCK_POLICY, "continue");

        ParsleyConfig config = ParsleyConfig.from(props);

        assertFalse(config.failOnUnresolvableClock(),
                "an explicit 'continue' clock-resolution policy must not fail fast");
    }

    /**
     * Setting {@code parsley.buffer.eviction.failure.policy} to {@code continue} overrides the
     * default, so a buffer-limit eviction evicts and forwards instead of failing the task fast.
     *
     * Asserts that the effective config no longer fails fast on eviction.
     */
    @Test
    void fromAppliesAnExplicitContinueEvictionPolicy() {
        Properties props = new Properties();
        props.setProperty(EVICTION_POLICY, "continue");

        ParsleyConfig config = ParsleyConfig.from(props);

        assertFalse(config.failOnEvictionLimit(),
                "an explicit 'continue' eviction policy must not fail fast on eviction");
    }

    /**
     * With no {@code parsley.topology.validation} set, startup topology validation defaults to
     * {@code warn} — detectable misconfigurations are logged but do not fail the task, so an existing
     * deployment is never broken by the check.
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
}
