package io.parsley;

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

    enum FailurePolicy { FAIL, CONTINUE }

    private final FailurePolicy deserializationFailurePolicy;

    private ParsleyConfig(FailurePolicy deserializationFailurePolicy) {
        this.deserializationFailurePolicy = deserializationFailurePolicy;
    }

    /** Whether a buffer-decode failure should be skipped ({@code continue}) rather than fail fast. */
    boolean skipOnDecodeFailure() {
        return deserializationFailurePolicy == FailurePolicy.CONTINUE;
    }

    /** Loads from the {@code parsley.properties} classpath resource, or defaults if it is absent. */
    static ParsleyConfig load() {
        Properties props = new Properties();
        try (InputStream in = ParsleyConfig.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (in != null) {
                props.load(in);
                log.debug("Loaded {} from the classpath", RESOURCE);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + RESOURCE + " from the classpath", e);
        }
        return from(props);
    }

    /** Builds from explicit properties (programmatic override / tests). */
    static ParsleyConfig from(Properties props) {
        return new ParsleyConfig(failurePolicy(props));
    }

    private static FailurePolicy failurePolicy(Properties props) {
        String value = props.getProperty(DESERIALIZATION_FAILURE_POLICY, "fail").trim();
        try {
            return FailurePolicy.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    DESERIALIZATION_FAILURE_POLICY + " must be 'fail' or 'continue', got '" + value + "'");
        }
    }
}
