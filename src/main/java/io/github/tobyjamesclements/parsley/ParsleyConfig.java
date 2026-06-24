package io.github.tobyjamesclements.parsley;

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

    enum FailurePolicy { FAIL, CONTINUE }

    private final FailurePolicy deserializationFailurePolicy;
    private final FailurePolicy evictionFailurePolicy;

    private ParsleyConfig(FailurePolicy deserializationFailurePolicy, FailurePolicy evictionFailurePolicy) {
        this.deserializationFailurePolicy = deserializationFailurePolicy;
        this.evictionFailurePolicy = evictionFailurePolicy;
    }

    /** Whether a buffer-decode failure should be skipped ({@code continue}) rather than fail fast. */
    boolean skipOnDecodeFailure() {
        return deserializationFailurePolicy == FailurePolicy.CONTINUE;
    }

    /** Whether a buffer-limit eviction should fail the task fast rather than evict and forward. */
    boolean failOnEvictionLimit() {
        return evictionFailurePolicy == FailurePolicy.FAIL;
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
                failurePolicy(props, EVICTION_FAILURE_POLICY));
    }

    private static FailurePolicy failurePolicy(Properties props, String key) {
        String value = props.getProperty(key, "fail").trim();
        try {
            return FailurePolicy.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(key + " must be 'fail' or 'continue', got '" + value + "'");
        }
    }
}
