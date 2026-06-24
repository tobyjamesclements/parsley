package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyConfig}'s own configuration loading: building from explicit properties via
 * {@link ParsleyConfig#from(Properties)}, the {@code parsley.buffer.deserialization.failure.policy}
 * and {@code parsley.buffer.eviction.failure.policy} values' validation, and
 * {@link ParsleyConfig#load()}'s classpath-resource fallback to defaults.
 */
class ParsleyConfigTest {

    private static final String FAILURE_POLICY = "parsley.buffer.deserialization.failure.policy";
    private static final String EVICTION_POLICY = "parsley.buffer.eviction.failure.policy";

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
     * {@code ParsleyConfig.load()} reads the {@code parsley.properties} classpath resource (absent in
     * this project's test classpath) and falls back to defaults rather than throwing — the default
     * deserialization-failure policy is {@code fail} (do not skip), and the default eviction-failure
     * policy is also {@code fail} (fail fast rather than evict and forward).
     *
     * Asserts that the loaded config does not skip on decode failure and does fail fast on eviction.
     */
    @Test
    void loadFallsBackToDefaultsWhenNoResourceIsPresentOnTheClasspath() {
        ParsleyConfig config = ParsleyConfig.load();

        assertFalse(config.skipOnDecodeFailure(),
                "with no parsley.properties on the classpath, the default policy 'fail' must apply");
        assertTrue(config.failOnEvictionLimit(),
                "with no parsley.properties on the classpath, the default eviction policy 'fail' must apply");
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
}
