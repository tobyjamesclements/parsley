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
     * {@code parsley.coordination.epoch-events-topic} — the shared, single-partition epoch-events log
     * topic name. Set it to turn on topology-epoch coordination for a {@link CausalStreams} runtime;
     * absent (the default), a topology runs in epoch 0 exactly as without coordination — no epoch-events
     * log, no coordination thread, every coordination path inert.
     */
    static final String COORDINATION_EPOCH_EVENTS_TOPIC = "parsley.coordination.epoch-events-topic";

    /**
     * {@code parsley.coordination.domain-topics} — the comma-separated set of every topic in the
     * coordinated domain (every member's inputs and sinks, external sources included). Only meaningful
     * alongside {@link #COORDINATION_EPOCH_EVENTS_TOPIC} — {@link #from} fails startup if this is set
     * without it. The reverse is not required: coordination works without this key exactly as it always
     * has (full-mesh validation derives its own domain from the shared log's live declarations, not from
     * this static config) — set it only when a stage needs {@link CausalTopology#assemble} to auto-wire
     * extra raw-bytes passthrough sources into that stage's own processor node
     * for any domain topic a stage does not otherwise consume or produce, so that stage's declared
     * subscriptions can cover the full domain ({@link ParsleyProcessor#validateFullMeshCoverage}) without
     * hand-wiring an "independent input" pass-through stage for every such topic. Kafka Streams has no
     * public API to add a source topic to an already-running task, so the full domain must be known
     * statically, ahead of runtime, for every member's source nodes to be wired correctly at all — the
     * coordination log's own full-mesh validation is then a runtime cross-check that this configuration
     * was applied correctly, never a mechanism that discovers or wires topics itself.
     */
    static final String COORDINATION_DOMAIN_TOPICS = "parsley.coordination.domain-topics";

    /** How {@link #TOPOLOGY_VALIDATION} reacts to a detectable topology misconfiguration. */
    enum ValidationMode { OFF, WARN, STRICT }

    private final ValidationMode topologyValidation;
    private final @Nullable String coordinationEpochEventsTopic;
    private final Set<String> coordinationDomainTopics;

    private ParsleyConfig(ValidationMode topologyValidation, @Nullable String coordinationEpochEventsTopic,
                          Set<String> coordinationDomainTopics) {
        this.topologyValidation = topologyValidation;
        this.coordinationEpochEventsTopic = coordinationEpochEventsTopic;
        this.coordinationDomainTopics = coordinationDomainTopics;
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

    /** Builds from explicit properties (programmatic override / tests). */
    static ParsleyConfig from(Properties props) {
        String epochEventsTopic = props.getProperty(COORDINATION_EPOCH_EVENTS_TOPIC);
        String resolvedEpochEventsTopic =
                (epochEventsTopic == null || epochEventsTopic.isBlank()) ? null : epochEventsTopic.trim();
        Set<String> domainTopics = domainTopics(props);
        if (resolvedEpochEventsTopic == null && !domainTopics.isEmpty()) {
            throw new IllegalStateException(COORDINATION_DOMAIN_TOPICS + " is set but "
                    + COORDINATION_EPOCH_EVENTS_TOPIC + " is not; domain-topics only has meaning "
                    + "under topology-epoch coordination");
        }
        return new ParsleyConfig(validationMode(props), resolvedEpochEventsTopic, domainTopics);
    }

    private static Set<String> domainTopics(Properties props) {
        String value = props.getProperty(COORDINATION_DOMAIN_TOPICS);
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        Set<String> topics = new LinkedHashSet<>();
        for (String topic : value.split(",")) {
            String trimmed = topic.trim();
            if (!trimmed.isEmpty()) {
                topics.add(trimmed);
            }
        }
        return Set.copyOf(topics);
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
