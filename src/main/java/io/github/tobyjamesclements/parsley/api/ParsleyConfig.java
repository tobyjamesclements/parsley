package io.github.tobyjamesclements.parsley.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Broker connection, application identity and the tuning Parsley leaves open.
 *
 * <p>Configuration that carries the delivery guarantee is fixed by the runtime and cannot be
 * set here. {@link Builder#streamsProperty} rejects those keys rather than silently ignoring
 * them, so a configuration that would weaken the guarantee fails at construction.
 *
 * @see Parsley#start(ParsleyConfig, Process...)
 */
public final class ParsleyConfig {

    private static final Set<String> FORBIDDEN_KEYS = Set.of(
            "application.id",
            "group.id",
            "processing.guarantee",
            "enable.auto.commit",
            "transactional.id");

    private static final Set<String> FORBIDDEN_SUFFIXES = Set.of(
            "isolation.level",
            "auto.offset.reset",
            "processing.guarantee",
            "enable.auto.commit",
            "group.id",
            "transactional.id",
            "processing.exception.handler",
            "deserialization.exception.handler",
            "production.exception.handler",
            "interceptor.classes",
            "default.timestamp.extractor",
            // The group membership protocol selects the fencing semantics the initial-position
            // bootstrap's safety argument is built on; swapping it is a guarantee-bearing change.
            "group.protocol",
            "group.remote.assignor",
            // Streams pins the plain spelling from its own config but applies prefixed
            // consumer overrides on top without re-pinning (unlike its producer path), so a
            // main.consumer./restore.consumer./global.consumer. spelling would point a
            // consumer at a different cluster than the one start() resolved identities
            // against (D87).
            "bootstrap.servers");

    private final String bootstrapServers;
    private final String applicationIdPrefix;
    private final String stateDir;
    private final int metadataBudgetBytes;
    private final Map<String, Object> streamsProperties;

    private ParsleyConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.applicationIdPrefix = builder.applicationIdPrefix;
        this.stateDir = builder.stateDir;
        this.metadataBudgetBytes = builder.metadataBudgetBytes;
        this.streamsProperties = Map.copyOf(builder.streamsProperties);
    }

    /**
     * Begins a configuration.
     *
     * @param bootstrapServers    Kafka bootstrap servers
     * @param applicationIdPrefix prefix for each process's Kafka application id, which
     *                            identifies its committed state across restarts
     * @return a builder
     * @throws IllegalArgumentException if {@code bootstrapServers} or
     *         {@code applicationIdPrefix} is null or blank, or the prefix contains the
     *         reserved {@link Parsley#RESERVED_PREFIX} namespace, since it prefixes
     *         application ids and changelog topic names
     */
    public static Builder builder(String bootstrapServers, String applicationIdPrefix) {
        return new Builder(bootstrapServers, applicationIdPrefix);
    }

    /**
     * Returns the Kafka bootstrap servers.
     *
     * @return the Kafka bootstrap servers
     */
    public String bootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Returns the prefix for each process's application id.
     *
     * @return the prefix for each process's application id
     */
    public String applicationIdPrefix() {
        return applicationIdPrefix;
    }

    /**
     * Returns the local state directory, or {@code null} for the Kafka Streams default.
     *
     * @return the local state directory, or {@code null} for the Kafka Streams default
     */
    public String stateDir() {
        return stateDir;
    }

    /**
     * Returns the largest causal metadata a message may carry, in bytes.
     *
     * @return the largest causal metadata a message may carry, in bytes
     */
    public int metadataBudgetBytes() {
        return metadataBudgetBytes;
    }

    /**
     * Returns additional Kafka Streams properties, none of them safety-bearing.
     *
     * @return additional Kafka Streams properties, none of them safety-bearing
     */
    public Map<String, Object> streamsProperties() {
        return streamsProperties;
    }

    /** Accumulates configuration, rejecting anything that would weaken the guarantee. */
    public static final class Builder {
        private final String bootstrapServers;
        private final String applicationIdPrefix;
        private String stateDir;
        private int metadataBudgetBytes = io.github.tobyjamesclements.parsley.core.ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES;
        private final Map<String, Object> streamsProperties = new LinkedHashMap<>();

        private Builder(String bootstrapServers, String applicationIdPrefix) {
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new IllegalArgumentException("bootstrapServers must be non-blank");
            }
            if (applicationIdPrefix == null || applicationIdPrefix.isBlank()) {
                throw new IllegalArgumentException("applicationIdPrefix must be non-blank");
            }
            if (applicationIdPrefix.contains(Parsley.RESERVED_PREFIX)) {
                throw new IllegalArgumentException("applicationIdPrefix may not contain the"
                        + " reserved namespace " + Parsley.RESERVED_PREFIX + ": it becomes part of"
                        + " application ids and changelog topic names, which would then sit inside"
                        + " parsley's own namespace: " + applicationIdPrefix);
            }
            this.bootstrapServers = bootstrapServers;
            this.applicationIdPrefix = applicationIdPrefix;
        }

        /**
         * Sets the local directory holding process state.
         *
         * @param stateDir a directory path, or {@code null} for the Kafka Streams default
         * @return this builder
         */
        public Builder stateDir(String stateDir) {
            this.stateDir = stateDir;
            return this;
        }

        /**
         * Sets the largest causal metadata a message may carry.
         *
         * <p>A process refuses metadata beyond this size on receipt and refuses to send
         * beyond it, both with Parsley's own diagnosis, in place of reaching the broker's
         * record-size limit.
         *
         * @param metadataBudgetBytes a positive byte count
         * @return this builder
         * @throws IllegalArgumentException if {@code metadataBudgetBytes} is not positive
         */
        public Builder metadataBudgetBytes(int metadataBudgetBytes) {
            if (metadataBudgetBytes <= 0) {
                throw new IllegalArgumentException("metadataBudgetBytes must be positive");
            }
            this.metadataBudgetBytes = metadataBudgetBytes;
            return this;
        }

        /**
         * Sets an additional Kafka Streams property.
         *
         * @param key   the property name
         * @param value the property value
         * @return this builder
         * @throws IllegalArgumentException if {@code key} or {@code value} is null, or
         *         {@code key} is one Parsley owns, such as {@code processing.guarantee}
         *         or {@code isolation.level}
         */
        public Builder streamsProperty(String key, Object value) {
            if (key == null) {
                throw new IllegalArgumentException("property key must be non-null");
            }
            if (value == null) {
                throw new IllegalArgumentException("property " + key + " must have a non-null"
                        + " value; a null would only surface as an unattributed NPE at build()");
            }
            if (FORBIDDEN_KEYS.contains(key) || FORBIDDEN_SUFFIXES.stream().anyMatch(key::endsWith)) {
                throw new IllegalArgumentException("property " + key + " is owned by parsley and cannot be overridden");
            }
            streamsProperties.put(key, value);
            return this;
        }

        /**
         * Returns the configuration.
         *
         * @return the configuration
         */
        public ParsleyConfig build() {
            return new ParsleyConfig(this);
        }
    }
}
