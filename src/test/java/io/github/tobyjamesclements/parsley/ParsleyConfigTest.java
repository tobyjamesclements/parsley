package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
     * {@code parsley.coordination.domain-topics} parses as a comma-separated set, trimming whitespace
     * around each entry and dropping empty entries, when {@code parsley.coordination.epoch-events-topic}
     * is also set.
     *
     * Asserts the parsed set matches exactly, order and duplicates aside.
     */
    @Test
    void fromParsesDomainTopicsAsATrimmedCommaSeparatedSet() {
        Properties props = new Properties();
        props.setProperty("parsley.coordination.epoch-events-topic", "epoch-events");
        props.setProperty("parsley.coordination.domain-topics", " t1, t2 ,t3,, t2");

        assertEquals(Set.of("t1", "t2", "t3"), ParsleyConfig.from(props).coordinationDomainTopics(),
                "domain-topics must parse as a trimmed, deduplicated comma-separated set");
    }

    /**
     * With neither coordination key set, {@link ParsleyConfig#coordinationDomainTopics()} is empty —
     * the common, uncoordinated case, unaffected by this new key's existence.
     *
     * Asserts the default is an empty set.
     */
    @Test
    void coordinationDomainTopicsDefaultsToEmpty() {
        assertEquals(Set.of(), ParsleyConfig.load().coordinationDomainTopics(),
                "domain-topics must default to empty when coordination is not configured");
    }

    /**
     * {@code parsley.coordination.domain-topics} is only meaningful alongside {@code parsley.coordination.
     * epoch-events-topic} — setting it without the epoch-events-topic key is rejected at startup rather
     * than silently ignored, since a passthrough-wiring config with no coordination log to validate
     * against would create dead, never-exercised topology nodes with no way to detect the
     * misconfiguration later.
     *
     * Asserts {@code IllegalStateException} is thrown when domain-topics is set alone.
     */
    @Test
    void fromRejectsDomainTopicsWithoutEpochEventsTopic() {
        Properties props = new Properties();
        props.setProperty("parsley.coordination.domain-topics", "t1,t2");

        assertThrows(IllegalStateException.class, () -> ParsleyConfig.from(props),
                "domain-topics without epoch-events-topic must be rejected");
    }

    /**
     * The reverse pairing is not required: {@code parsley.coordination.epoch-events-topic} alone (with no
     * domain-topics) is exactly today's existing, already-supported coordination configuration — full-mesh
     * validation derives its own domain from the shared log's live declarations, not from this static
     * config, so there is nothing for an existing coordinated deployment to migrate.
     *
     * Asserts no exception is thrown and domain-topics is empty.
     */
    @Test
    void fromAllowsEpochEventsTopicWithoutDomainTopics() {
        Properties props = new Properties();
        props.setProperty("parsley.coordination.epoch-events-topic", "epoch-events");

        ParsleyConfig config = ParsleyConfig.from(props);
        assertEquals("epoch-events", config.coordinationEpochEventsTopic());
        assertEquals(Set.of(), config.coordinationDomainTopics(),
                "epoch-events-topic alone must not require domain-topics");
    }
}
