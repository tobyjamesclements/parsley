package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

/**
 * Parsley's own configuration, loaded from a {@code parsley.properties} classpath resource. This is
 * deliberately separate from Kafka Streams configuration: Parsley behaviours have causal-frontier
 * consequences with no Streams equivalent, so they are not mapped from Streams' exception handlers.
 *
 * <p>Causal delivery has exactly three dispositions, never a fourth: a record is <strong>forwarded</strong>
 * once its dependencies are satisfied; while unsatisfied it stays <strong>buffered</strong>, unbounded,
 * changelog-backed; and a record whose dependencies are proven impossible (an undecodable payload or
 * dependency header, or — cascading from either — a dependent that can never itself be satisfied) is
 * <strong>dead-lettered</strong>: removed from the causal execution path onto the dead-letter topic, never
 * forwarded downstream as if it were causally valid. There is no configuration that trades causal safety
 * for liveness; dead-lettering is the only escape from an unsatisfiable buffer, and it fires solely on
 * proven impossibility, never on pressure or time.
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
     * {@code parsley.deadletter.topic} — the dead-letter topic name every stage's dead-letter sink writes
     * to. Absent (the default), {@link CausalTopology#assemble} computes {@code {application.id}-deadletter}.
     */
    static final String DEADLETTER_TOPIC = "parsley.deadletter.topic";

    /**
     * {@code parsley.deadletter.partitions} — the partition count {@link CausalStreams#start()} provisions
     * the dead-letter topic with if it does not already exist. Default {@code 1}: the dead-letter topic is
     * a diagnostic, operator-facing destination, not part of the causal delivery path, so its partition
     * count has no correctness consequence.
     */
    static final String DEADLETTER_PARTITIONS = "parsley.deadletter.partitions";

    /** How {@link #TOPOLOGY_VALIDATION} reacts to a detectable topology misconfiguration. */
    enum ValidationMode { OFF, WARN, STRICT }

    private final ValidationMode topologyValidation;
    private final @Nullable String coordinationEpochEventsTopic;
    private final @Nullable String deadLetterTopic;
    private final int deadLetterPartitions;

    private ParsleyConfig(ValidationMode topologyValidation, @Nullable String coordinationEpochEventsTopic,
                          @Nullable String deadLetterTopic, int deadLetterPartitions) {
        this.topologyValidation = topologyValidation;
        this.coordinationEpochEventsTopic = coordinationEpochEventsTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.deadLetterPartitions = deadLetterPartitions;
    }

    /** How to react to a detectable topology misconfiguration at startup. */
    ValidationMode topologyValidation() {
        return topologyValidation;
    }

    /** The shared epoch-events log topic, or {@code null} if topology-epoch coordination is not configured. */
    @Nullable String coordinationEpochEventsTopic() {
        return coordinationEpochEventsTopic;
    }

    /** The configured dead-letter topic name, or {@code null} to fall back to the applicationId-derived default. */
    @Nullable String deadLetterTopic() {
        return deadLetterTopic;
    }

    /** The partition count to provision the dead-letter topic with if it does not already exist. */
    int deadLetterPartitions() {
        return deadLetterPartitions;
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
        String deadLetterTopic = props.getProperty(DEADLETTER_TOPIC);
        return new ParsleyConfig(validationMode(props),
                (epochEventsTopic == null || epochEventsTopic.isBlank()) ? null : epochEventsTopic.trim(),
                (deadLetterTopic == null || deadLetterTopic.isBlank()) ? null : deadLetterTopic.trim(),
                deadLetterPartitions(props));
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

    private static int deadLetterPartitions(Properties props) {
        String value = props.getProperty(DEADLETTER_PARTITIONS, "1").trim();
        try {
            int partitions = Integer.parseInt(value);
            if (partitions < 1) {
                throw new NumberFormatException(value);
            }
            return partitions;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    DEADLETTER_PARTITIONS + " must be a positive integer, got '" + value + "'");
        }
    }
}
