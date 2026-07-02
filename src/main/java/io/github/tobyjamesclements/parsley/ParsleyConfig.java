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
 * deliberately separate from Kafka Streams configuration: Parsley behaviours — such as how a held
 * record that can no longer be deserialised is handled — have causal-frontier consequences with no
 * Streams equivalent, so they are not mapped from Streams' exception handlers.
 *
 * <p>An absent file, or absent keys, fall back to defaults. {@link #from(Properties)} builds one from
 * explicit properties (programmatic override / tests).
 */
final class ParsleyConfig {

    private static final Logger log = LoggerFactory.getLogger(ParsleyConfig.class);

    /** The classpath resource Parsley reads its configuration from. */
    static final String RESOURCE = "parsley.properties";

    /**
     * {@code parsley.buffer.deserialization.failure.policy} — how a held record that can no longer be
     * deserialised on the forward path (e.g. an incompatible Schema Registry change while buffered) is
     * handled:
     * <ul>
     *   <li>{@code fail} (default): fail fast, leaving the record in the buffer changelog for recovery
     *       once the schema is fixed or rolled back.</li>
     *   <li>{@code continue}: drop the record (logged, counted as a violation) and keep processing.</li>
     * </ul>
     *
     * <p><strong>v1 caveat:</strong> {@code continue} is best-effort liveness and <em>lossy</em> — the
     * dropped record is gone, and because the frontier is a high-water mark, its dependents may be
     * delivered on a missing premise. A durable quarantine store plus operator-triggered redelivery is
     * planned to supersede it.
     */
    static final String DESERIALIZATION_FAILURE_POLICY = "parsley.buffer.deserialization.failure.policy";

    /**
     * {@code parsley.buffer.eviction.failure.policy} — how a {@link CausalBufferLimit} firing
     * (the buffer would otherwise force a held record's delivery out of causal order) is handled:
     * <ul>
     *   <li>{@code fail} (default): fail fast, leaving the record buffered rather than violate
     *       causal order — trades availability for consistency.</li>
     *   <li>{@code continue}: evict and forward the record anyway, out of causal order (logged,
     *       counted as a violation) — Parsley's original always-forward behaviour.</li>
     * </ul>
     */
    static final String EVICTION_FAILURE_POLICY = "parsley.buffer.eviction.failure.policy";

    /**
     * {@code parsley.clock.resolution.failure.policy} — how an inbound record whose
     * {@code parsley-causal-dependencies} header cannot be decoded into a clock (a corrupt or
     * truncated header, or one written in an unsupported wire version) is handled at ingest:
     * <ul>
     *   <li>{@code fail} (default): fail fast, rather than forward the record on an unknown premise.
     *       The record was never buffered and its source offset is not committed past it, so it is
     *       reprocessed on restart.</li>
     *   <li>{@code continue}: treat the unresolvable header as empty (vacuously satisfied) and forward
     *       the record immediately (logged, counted as a violation) — Parsley's original best-effort
     *       behaviour.</li>
     * </ul>
     *
     * <p><strong>v1 caveat:</strong> {@code continue} is lossy of causal premises — because the
     * frontier is a high-water mark, forwarding on empty dependencies can let the record, and its
     * dependents, be delivered ahead of a premise the corrupt header actually carried.
     */
    static final String CLOCK_RESOLUTION_FAILURE_POLICY = "parsley.clock.resolution.failure.policy";

    /**
     * {@code parsley.topology.validation} — how the low-level processor reacts to a topology
     * misconfiguration it can detect at startup: the causal topics not sharing a partition count
     * (co-partitioning is impossible — each task cannot own the complete partition set for a
     * causally-related group), and, for a {@code CausalStreams} stage's sink topics, a
     * {@code cleanup.policy} that includes {@code compact} (a protocol watermark is a null-value
     * record wire-indistinguishable from a compaction tombstone and can be compacted away before a
     * slow consumer reads it):
     * <ul>
     *   <li>{@code off}: no check.</li>
     *   <li>{@code warn} (default): log a prominent warning and continue — visible without breaking an
     *       existing deployment that silently relied on the misconfiguration.</li>
     *   <li>{@code strict}: fail the task fast at {@code init()}.</li>
     * </ul>
     *
     * <p>Only the constraints the decorator can observe are checked here — it knows its registered
     * input buffers, and, when built through the topology-owning {@code CausalStreams} API, that
     * stage's sink topics too (folded into the same partition-count check, plus the cleanup.policy
     * check). A sink not yet created is skipped for both checks rather than failing the task.
     */
    static final String TOPOLOGY_VALIDATION = "parsley.topology.validation";

    /**
     * {@code parsley.topology.epoch.topic} — the name of the compacted internal topic carrying the
     * topology epoch's {@code startsAt} bounds (see {@link ParsleyEpoch}). When set, a
     * {@link CausalStreams} stage wires a Kafka Streams global store over it, and the delivery gate
     * strips any dependency below its coordinate's {@code startsAt} bound. When absent (the default),
     * epoch bounding is fully disabled: no global store is wired and the gate behaves exactly as it
     * does without this feature. Only honoured by the topology-owning {@link CausalStreams} API; the
     * low-level {@code CausalProcessors} path does not wire the global store.
     */
    static final String EPOCH_TOPIC = "parsley.topology.epoch.topic";

    enum FailurePolicy { FAIL, CONTINUE }

    /** How {@link #TOPOLOGY_VALIDATION} reacts to a detectable topology misconfiguration. */
    enum ValidationMode { OFF, WARN, STRICT }

    private final FailurePolicy deserializationFailurePolicy;
    private final FailurePolicy evictionFailurePolicy;
    private final FailurePolicy clockResolutionFailurePolicy;
    private final ValidationMode topologyValidation;
    private final @Nullable String epochTopic;

    private ParsleyConfig(FailurePolicy deserializationFailurePolicy, FailurePolicy evictionFailurePolicy,
                          FailurePolicy clockResolutionFailurePolicy, ValidationMode topologyValidation,
                          @Nullable String epochTopic) {
        this.deserializationFailurePolicy = deserializationFailurePolicy;
        this.evictionFailurePolicy = evictionFailurePolicy;
        this.clockResolutionFailurePolicy = clockResolutionFailurePolicy;
        this.topologyValidation = topologyValidation;
        this.epochTopic = epochTopic;
    }

    /** Whether a buffer-decode failure should be skipped ({@code continue}) rather than fail fast. */
    boolean skipOnDecodeFailure() {
        return deserializationFailurePolicy == FailurePolicy.CONTINUE;
    }

    /** Whether a buffer-limit eviction should fail the task fast rather than evict and forward. */
    boolean failOnEvictionLimit() {
        return evictionFailurePolicy == FailurePolicy.FAIL;
    }

    /** Whether an undecodable causal-dependencies header should fail the task fast rather than forward empty. */
    boolean failOnUnresolvableClock() {
        return clockResolutionFailurePolicy == FailurePolicy.FAIL;
    }

    /** How to react to a detectable topology misconfiguration at startup. */
    ValidationMode topologyValidation() {
        return topologyValidation;
    }

    /**
     * The configured epoch topic name, or {@code null} if {@link #EPOCH_TOPIC} is unset — in which
     * case epoch bounding is disabled and the gate never strips.
     */
    @Nullable String epochTopic() {
        return epochTopic;
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
        return new ParsleyConfig(
                failurePolicy(props, DESERIALIZATION_FAILURE_POLICY),
                failurePolicy(props, EVICTION_FAILURE_POLICY),
                failurePolicy(props, CLOCK_RESOLUTION_FAILURE_POLICY),
                validationMode(props),
                epochTopic(props));
    }

    private static @Nullable String epochTopic(Properties props) {
        String value = props.getProperty(EPOCH_TOPIC);
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static FailurePolicy failurePolicy(Properties props, String key) {
        String value = props.getProperty(key, "fail").trim();
        try {
            return FailurePolicy.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(key + " must be 'fail' or 'continue', got '" + value + "'");
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
