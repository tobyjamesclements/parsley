package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * Parsley's own configuration, loaded from a {@code parsley.properties} classpath resource. This is
 * deliberately separate from Kafka Streams configuration: Parsley behaviours have causal-frontier
 * consequences with no Streams equivalent, so they are not mapped from Streams' exception handlers.
 *
 * <p>Causal delivery has exactly two dispositions, never a third: a record is <strong>forwarded</strong>
 * once its dependencies are satisfied; while unsatisfied it stays <strong>buffered</strong>, unbounded,
 * changelog-backed. A record whose dependencies are proven impossible (an undecodable payload or
 * dependency header, or a dependency naming a coordinate this node has no channel for) unconditionally
 * fails the task. There is no configuration that trades causal safety for liveness, and no diversion —
 * proven impossibility always fails fast, never on pressure or time.
 *
 * <p>An absent file, or absent keys, fall back to defaults. {@link #from(Properties)} builds one from
 * explicit properties (programmatic override / tests).
 */
final class ParsleyConfig {

    private static final Logger log = LoggerFactory.getLogger(ParsleyConfig.class);

    /** The classpath resource Parsley reads its configuration from. */
    static final String RESOURCE = "parsley.properties";

    /**
     * {@code parsley.topology.validation} — how the processor reacts to a topology misconfiguration it
     * can detect at startup: the causal topics not sharing a partition count (co-partitioning is
     * impossible — each task cannot own the complete partition set for a causally-related group), and,
     * for a {@link CausalStreamsBuilder} stage's sink topics, a {@code cleanup.policy} that includes
     * {@code compact} (a protocol watermark is a null-value record wire-indistinguishable from a
     * compaction tombstone and can be compacted away before a slow consumer reads it):
     * <ul>
     *   <li>{@code off}: no check.</li>
     *   <li>{@code warn} (default): log a prominent warning and continue — visible without breaking an
     *       existing deployment that silently relied on the misconfiguration.</li>
     *   <li>{@code strict}: fail the task fast at {@code init()}.</li>
     * </ul>
     *
     * <p>Only the constraints the processor can observe are checked here — it knows its registered
     * input buffers, and, when built through the topology-owning {@code CausalStreamsBuilder} API, that
     * stage's sink topics too. A sink not yet created is skipped for both checks rather than failing the
     * task.
     */
    static final String TOPOLOGY_VALIDATION = "parsley.topology.validation";

    /**
     * {@code parsley.coordination.epoch-events-topic} — <strong>inert</strong>: topology-epoch
     * coordination has been removed from the causal protocol (the two-branch delivery gate needs no
     * membership, no epochs, and no join barrier — joins are coordination-free). The key is still
     * accepted so a configured deployment starts, but it wires nothing; {@link CausalStreams} logs a
     * warning when it is present. Deleted outright — startup will then fail loudly on it — in the
     * next release.
     */
    static final String COORDINATION_EPOCH_EVENTS_TOPIC = "parsley.coordination.epoch-events-topic";

    /**
     * {@code parsley.coordination.domain-topics} — <strong>inert</strong>, like
     * {@link #COORDINATION_EPOCH_EVENTS_TOPIC}: the passthrough auto-wiring this key used to drive
     * existed so a member's subscriptions could cover a coordinated domain, a requirement the
     * two-branch gate dissolved (a dependency on an unconsumed topic is soundly ignored — D1).
     * Accepted, ignored, warned about; deleted in the next release.
     */
    static final String COORDINATION_DOMAIN_TOPICS = "parsley.coordination.domain-topics";

    /**
     * {@code parsley.coordination.member-apps} — <strong>inert</strong>, like
     * {@link #COORDINATION_EPOCH_EVENTS_TOPIC}: there is no membership roster because there is no
     * membership. Accepted, ignored, warned about; deleted in the next release.
     */
    static final String COORDINATION_MEMBER_APPS = "parsley.coordination.member-apps";

    /** How {@link #TOPOLOGY_VALIDATION} reacts to a detectable topology misconfiguration. */
    enum ValidationMode { OFF, WARN, STRICT }

    private final ValidationMode topologyValidation;
    private final @Nullable String coordinationEpochEventsTopic;
    private final Set<String> coordinationDomainTopics;
    private final Set<String> coordinationMemberApps;

    private ParsleyConfig(ValidationMode topologyValidation, @Nullable String coordinationEpochEventsTopic,
                          Set<String> coordinationDomainTopics, Set<String> coordinationMemberApps) {
        this.topologyValidation = topologyValidation;
        this.coordinationEpochEventsTopic = coordinationEpochEventsTopic;
        this.coordinationDomainTopics = coordinationDomainTopics;
        this.coordinationMemberApps = coordinationMemberApps;
    }

    /** How to react to a detectable topology misconfiguration at startup. */
    ValidationMode topologyValidation() {
        return topologyValidation;
    }

    /** The shared epoch-events log topic, or {@code null} if topology-epoch coordination is not configured. */
    @Nullable String coordinationEpochEventsTopic() {
        return coordinationEpochEventsTopic;
    }

    /** The full coordinated domain's topic set, or empty if topology-epoch coordination is not configured. */
    Set<String> coordinationDomainTopics() {
        return coordinationDomainTopics;
    }

    /** The authoritative member-app roster (every {@code application.id} in the domain), or empty when
     * {@link #COORDINATION_MEMBER_APPS} is not configured — the caller then defaults to a single-app
     * roster of the app's own id. */
    Set<String> coordinationMemberApps() {
        return coordinationMemberApps;
    }

    /** Loads from the {@code parsley.properties} classpath resource, or defaults if it is absent. */
    static ParsleyConfig load() {
        return from(loadProperties());
    }

    /**
     * Reads the {@code parsley.properties} classpath resource into a {@link Properties}, or an empty
     * one if the resource is absent. Exposed so callers can use the classpath file as a base layer and
     * overlay explicit keys on top before calling {@link #from(Properties)}.
     */
    static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream in = ParsleyConfig.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (in != null) {
                props.load(in);
                log.debug("Loaded {} from the classpath", RESOURCE);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + RESOURCE + " from the classpath", e);
        }
        return props;
    }

    /**
     * Builds from explicit properties (programmatic override / tests). The {@code
     * parsley.coordination.*} keys are inert but still parsed — {@link CausalStreams} reads them
     * only to warn that they wire nothing; the old set-without-epoch-events-topic cross-checks are
     * gone with the subsystem's protocol role (an inert key cannot be misconfigured).
     */
    static ParsleyConfig from(Properties props) {
        String epochEventsTopic = props.getProperty(COORDINATION_EPOCH_EVENTS_TOPIC);
        String resolvedEpochEventsTopic =
                (epochEventsTopic == null || epochEventsTopic.isBlank()) ? null : epochEventsTopic.trim();
        Set<String> domainTopics = domainTopics(props);
        Set<String> memberApps = commaSeparated(props, COORDINATION_MEMBER_APPS);
        return new ParsleyConfig(validationMode(props), resolvedEpochEventsTopic, domainTopics, memberApps);
    }

    private static Set<String> domainTopics(Properties props) {
        return commaSeparated(props, COORDINATION_DOMAIN_TOPICS);
    }

    private static Set<String> commaSeparated(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        Set<String> items = new LinkedHashSet<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                items.add(trimmed);
            }
        }
        return Set.copyOf(items);
    }

    private static ValidationMode validationMode(Properties props) {
        String value = props.getProperty(TOPOLOGY_VALIDATION, "warn").trim();
        try {
            return ValidationMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    TOPOLOGY_VALIDATION + " must be 'off', 'warn' or 'strict', got '" + value + "'");
        }
    }
}
