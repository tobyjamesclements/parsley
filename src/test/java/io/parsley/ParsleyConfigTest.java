package io.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link ParsleyConfig}'s own configuration loading: building from explicit properties via
 * {@link ParsleyConfig#from(Properties)}, the {@code parsley.buffer.deserialization.failure.policy}
 * value's validation, and {@link ParsleyConfig#load()}'s classpath-resource fallback to defaults.
 */
class ParsleyConfigTest {

    private static final String FAILURE_POLICY = "parsley.buffer.deserialization.failure.policy";

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
     * {@code ParsleyConfig.load()} reads the {@code parsley.properties} classpath resource (absent in
     * this project's test classpath) and falls back to defaults rather than throwing — the default
     * deserialization-failure policy is {@code fail} (do not skip).
     *
     * Asserts that the loaded config does not skip on decode failure.
     */
    @Test
    void loadFallsBackToDefaultsWhenNoResourceIsPresentOnTheClasspath() {
        ParsleyConfig config = ParsleyConfig.load();

        assertFalse(config.skipOnDecodeFailure(),
                "with no parsley.properties on the classpath, the default policy 'fail' must apply");
    }
}
