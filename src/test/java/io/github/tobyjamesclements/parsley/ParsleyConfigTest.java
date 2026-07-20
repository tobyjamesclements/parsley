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
     * The inert {@code parsley.coordination.*} keys still parse — {@code CausalStreams} reads them
     * only to warn that they wire nothing — and no combination of them fails startup: an inert key
     * cannot be misconfigured, so the old set-without-epoch-events-topic cross-checks are gone with
     * the subsystem's protocol role.
     *
     * Asserts each key parses alone without an exception, and domain-topics still parses as a
     * trimmed, deduplicated comma-separated set.
     */
    @Test
    void inertCoordinationKeysParseWithoutCrossChecks() {
        Properties domainAlone = new Properties();
        domainAlone.setProperty("parsley.coordination.domain-topics", " t1, t2 ,t3,, t2");
        assertEquals(Set.of("t1", "t2", "t3"), ParsleyConfig.from(domainAlone).coordinationDomainTopics(),
                "domain-topics must parse as a trimmed, deduplicated comma-separated set, with no "
                        + "epoch-events-topic cross-check (the key is inert)");

        Properties memberAppsAlone = new Properties();
        memberAppsAlone.setProperty("parsley.coordination.member-apps", "app-a,app-b");
        assertEquals(Set.of("app-a", "app-b"), ParsleyConfig.from(memberAppsAlone).coordinationMemberApps(),
                "member-apps must parse alone without an epoch-events-topic cross-check (the key is inert)");

        Properties topicAlone = new Properties();
        topicAlone.setProperty("parsley.coordination.epoch-events-topic", "epoch-events");
        assertEquals("epoch-events", ParsleyConfig.from(topicAlone).coordinationEpochEventsTopic(),
                "epoch-events-topic must still parse (CausalStreams reads it to warn)");
    }

    /**
     * With no coordination key set, {@link ParsleyConfig#coordinationDomainTopics()} is empty — the
     * common case, unaffected by the inert keys' continued existence.
     *
     * Asserts the default is an empty set.
     */
    @Test
    void coordinationDomainTopicsDefaultsToEmpty() {
        assertEquals(Set.of(), ParsleyConfig.load().coordinationDomainTopics(),
                "domain-topics must default to empty when the key is not set");
    }
}
