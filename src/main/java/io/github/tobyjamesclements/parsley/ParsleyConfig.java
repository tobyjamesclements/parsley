package io.github.tobyjamesclements.parsley;

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
     * The removed {@code parsley.coordination.*} key prefix. Topology-epoch coordination is no longer
     * part of the causal protocol (the two-branch delivery gate needs no membership, no epochs, and no
     * join barrier — joins are coordination-free), and its configuration keys are deleted outright:
     * {@link #from(Properties)} fails loudly when any key under this prefix is present, so a stale
     * deployment learns about the removal at startup instead of silently running with dead keys.
     */
    static final String REMOVED_COORDINATION_PREFIX = "parsley.coordination.";

    /** How {@link #TOPOLOGY_VALIDATION} reacts to a detectable topology misconfiguration. */
    enum ValidationMode { OFF, WARN, STRICT }

    private final ValidationMode topologyValidation;

    private ParsleyConfig(ValidationMode topologyValidation) {
        this.topologyValidation = topologyValidation;
    }

    /** How to react to a detectable topology misconfiguration at startup. */
    ValidationMode topologyValidation() {
        return topologyValidation;
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
     * Builds from explicit properties (programmatic override / tests).
     *
     * @throws IllegalStateException if any removed {@code parsley.coordination.*} key is present
     *         (see {@link #rejectRemovedCoordinationKeys})
     */
    static ParsleyConfig from(Properties props) {
        rejectRemovedCoordinationKeys(props);
        return new ParsleyConfig(validationMode(props));
    }

    /**
     * Fails loudly when any removed {@code parsley.coordination.*} key is present. Topology-epoch
     * coordination has been removed from the causal protocol, so the keys wire nothing — and a key
     * that wires nothing must not parse quietly, or an operator who believes their deployment is
     * coordinated would never learn otherwise. There is no migration path: an existing coordinated
     * deployment upgrades by deleting its coordination configuration (behaviour becomes strictly
     * more available — joins need zero coordination).
     */
    private static void rejectRemovedCoordinationKeys(Properties props) {
        Set<String> removed = new LinkedHashSet<>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(REMOVED_COORDINATION_PREFIX)) {
                removed.add(name);
            }
        }
        if (!removed.isEmpty()) {
            throw new IllegalStateException("removed configuration " + removed + ": topology-epoch "
                    + "coordination has been removed from the causal protocol — joins need zero "
                    + "coordination, so there is no epoch-events log, no domain, and no member roster. "
                    + "Delete every parsley.coordination.* key; no replacement configuration is needed.");
        }
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
