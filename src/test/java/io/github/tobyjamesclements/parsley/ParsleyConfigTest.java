package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyConfig}'s enforcement of the empty configuration surface: Parsley has no
 * {@code parsley.*} keys at all (causal delivery is strictly fail-closed and the startup topology
 * checks are always on, so there is nothing to configure), and
 * {@link ParsleyConfig#requireNoParsleyKeys(Properties)} fails startup loudly when a stale
 * deployment still carries one.
 */
class ParsleyConfigTest {

    /**
     * A configuration with no {@code parsley.*} key passes: ordinary Kafka Streams keys are none of
     * Parsley's business, and an absent {@code parsley.properties} classpath resource (this
     * project's test classpath has none) is the expected state.
     *
     * Asserts a parsley-free configuration is accepted.
     */
    @Test
    void acceptsAConfigurationWithNoParsleyKeys() {
        Properties props = new Properties();
        props.setProperty("application.id", "app");
        props.setProperty("bootstrap.servers", "localhost:9092");

        assertDoesNotThrow(() -> ParsleyConfig.requireNoParsleyKeys(props),
                "a configuration carrying no parsley.* key must be accepted");
    }

    /**
     * Every removed {@code parsley.*} key fails loudly at startup: the removed
     * {@code parsley.topology.validation} modes (the checks are now always on), the removed
     * {@code parsley.coordination.*} subsystem (joins need zero coordination), and any unknown
     * {@code parsley.*} key alike wire nothing — and a key that wires nothing must not be accepted
     * quietly, or an operator who believes it took effect would never learn otherwise.
     *
     * Asserts each key alone throws {@code IllegalStateException} naming both the key and the
     * removal.
     */
    @Test
    void rejectsEveryParsleyKey() {
        for (String key : Set.of(
                "parsley.topology.validation",
                "parsley.coordination.epoch-events-topic",
                "parsley.coordination.domain-topics",
                "parsley.coordination.member-apps",
                "parsley.some.future.key")) {
            Properties props = new Properties();
            props.setProperty(key, "anything");
            IllegalStateException thrown = assertThrows(IllegalStateException.class,
                    () -> ParsleyConfig.requireNoParsleyKeys(props),
                    "the key " + key + " must fail startup loudly");
            assertTrue(thrown.getMessage().contains(key),
                    "the failure must name the offending key: " + thrown.getMessage());
            assertTrue(thrown.getMessage().contains("removed"),
                    "the failure must present the empty surface as a removal, not an unknown-key "
                            + "typo: " + thrown.getMessage());
        }
    }

    /**
     * A {@code parsley.*} key supplied only through the {@link Properties} defaults layer is
     * rejected too: Kafka Streams honours defaults-layer keys, so Parsley must not let one slip
     * past just because it is not in the top layer.
     *
     * Asserts a defaults-layer key throws {@code IllegalStateException}.
     */
    @Test
    void rejectsAParsleyKeyInThePropertiesDefaultsLayer() {
        Properties defaults = new Properties();
        defaults.setProperty("parsley.topology.validation", "warn");
        Properties props = new Properties(defaults);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyConfig.requireNoParsleyKeys(props),
                "a parsley.* key in the defaults layer must fail startup loudly");
        assertTrue(thrown.getMessage().contains("parsley.topology.validation"),
                "the failure must name the offending key: " + thrown.getMessage());
    }

    /**
     * Several offending keys are reported together, so an operator cleans up the whole stale
     * configuration in one pass instead of replaying startup once per key.
     *
     * Asserts the failure message names every offending key.
     */
    @Test
    void namesEveryOffendingKeyInOneFailure() {
        Properties props = new Properties();
        props.setProperty("parsley.topology.validation", "off");
        props.setProperty("parsley.coordination.member-apps", "a,b");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyConfig.requireNoParsleyKeys(props),
                "a configuration with several parsley.* keys must fail startup loudly");
        assertTrue(thrown.getMessage().contains("parsley.topology.validation")
                        && thrown.getMessage().contains("parsley.coordination.member-apps"),
                "the failure must name every offending key: " + thrown.getMessage());
    }
}
