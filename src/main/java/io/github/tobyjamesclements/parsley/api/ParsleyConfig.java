package io.github.tobyjamesclements.parsley.api;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Runtime configuration. Parsley owns the Kafka Streams lifecycle and its critical configuration: the processing
 * guarantee is exactly-once and the isolation level read_committed, neither overridable, and the consumer never
 * auto-resets past discarded positions (SPEC Substrate 3, Safety 8). Attempting to override any of these fails fast:
 * no documented use of this API can weaken a safety criterion (SPEC Structural 9).
 */
public final class ParsleyConfig {

    private static final Set<String> FORBIDDEN_KEYS = Set.of(
            "application.id",
            "group.id",
            "processing.guarantee",
            "enable.auto.commit",
            "transactional.id");
    // Suffix-matched so prefixed variants (consumer., main.consumer., producer., ...) are equally refused. The
    // exception handlers are guarantee-bearing: a continue-style handler converts failing closed into dropping a
    // message or an emission and committing anyway (SPEC Safety 3/7, Structural 19). Client interceptors and the
    // timestamp extractor are equally owned (D51): a producer interceptor's documented purpose is mutating records
    // in onSend — one that strips or replaces the causes header makes every emission read cause-free downstream,
    // below every criterion that rides on the header — and a log-and-skip timestamp extractor converts a received
    // message into a documented silent drop whose read position still advances.
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
            "default.timestamp.extractor");

    private final String bootstrapServers;
    private final String applicationIdPrefix;
    private final String stateDir;
    private final Duration factsInterval;
    private final int metadataBudgetBytes;
    private final Map<String, Object> extraProperties;

    private ParsleyConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.applicationIdPrefix = builder.applicationIdPrefix;
        this.stateDir = builder.stateDir;
        this.factsInterval = builder.factsInterval;
        this.metadataBudgetBytes = builder.metadataBudgetBytes;
        this.extraProperties = Map.copyOf(builder.extraProperties);
    }

    public static Builder builder(String bootstrapServers, String applicationIdPrefix) {
        return new Builder(bootstrapServers, applicationIdPrefix);
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    /** Each process runs as its own Streams application named {@code <prefix>-<processName>}. */
    public String applicationIdPrefix() {
        return applicationIdPrefix;
    }

    public String stateDir() {
        return stateDir;
    }

    /** How often position facts (read positions, log starts, topic existence) are refreshed. Liveness pacing only:
     * never an input to the deliverability decision (SPEC Structural 7, Liveness 3). */
    public Duration factsInterval() {
        return factsInterval;
    }

    /** The bound on encoded causal metadata accepted on receipt or expressed on send; reaching it fails closed
     * with parsley's own diagnosis instead of the substrate's record-size wall (SPEC Operational 4). */
    public int metadataBudgetBytes() {
        return metadataBudgetBytes;
    }

    public Map<String, Object> extraProperties() {
        return extraProperties;
    }

    public static final class Builder {
        private final String bootstrapServers;
        private final String applicationIdPrefix;
        private String stateDir;
        private Duration factsInterval = Duration.ofSeconds(1);
        private int metadataBudgetBytes = io.github.tobyjamesclements.parsley.core.ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES;
        private final Map<String, Object> extraProperties = new LinkedHashMap<>();

        private Builder(String bootstrapServers, String applicationIdPrefix) {
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new IllegalArgumentException("bootstrapServers must be non-blank");
            }
            if (applicationIdPrefix == null || applicationIdPrefix.isBlank()) {
                throw new IllegalArgumentException("applicationIdPrefix must be non-blank");
            }
            this.bootstrapServers = bootstrapServers;
            this.applicationIdPrefix = applicationIdPrefix;
        }

        public Builder stateDir(String stateDir) {
            this.stateDir = stateDir;
            return this;
        }

        public Builder factsInterval(Duration factsInterval) {
            if (factsInterval.isNegative() || factsInterval.isZero()) {
                throw new IllegalArgumentException("factsInterval must be positive");
            }
            this.factsInterval = factsInterval;
            return this;
        }

        /** Bound on encoded causal metadata per message, on receipt and on send. The frontier's size follows the
         * causal graph, not this process's declaration (docs/DESIGN.md §2), so the ceiling is a deployment
         * decision; reaching it fails closed with a parsley diagnosis (SPEC Operational 4). */
        public Builder metadataBudgetBytes(int metadataBudgetBytes) {
            if (metadataBudgetBytes <= 0) {
                throw new IllegalArgumentException("metadataBudgetBytes must be positive");
            }
            this.metadataBudgetBytes = metadataBudgetBytes;
            return this;
        }

        /**
         * An additional Kafka Streams property (tuning, security, and so on). Properties that would weaken the
         * guarantees — processing guarantee, isolation level, offset reset, group identity — are refused.
         */
        public Builder streamsProperty(String key, Object value) {
            if (FORBIDDEN_KEYS.contains(key) || FORBIDDEN_SUFFIXES.stream().anyMatch(key::endsWith)) {
                throw new IllegalArgumentException("property " + key + " is owned by parsley and cannot be overridden");
            }
            extraProperties.put(key, value);
            return this;
        }

        public ParsleyConfig build() {
            return new ParsleyConfig(this);
        }
    }
}
