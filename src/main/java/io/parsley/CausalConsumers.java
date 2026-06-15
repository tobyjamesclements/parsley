package io.parsley;

import java.util.Collection;
import java.util.Map;

/**
 * Factory for {@link CausalConsumer}. Obtain a {@link Builder} with
 * {@link #builder(Collection, CausalBufferPolicy, Map, Map)}, set any optional fields, and call
 * {@link Builder#build()}:
 *
 * <pre>{@code
 * CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
 *         List.of("prices", "orders"),
 *         CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         Map.of(), streamsConfig).build();
 * }</pre>
 *
 * <p>{@link CausalBufferPolicy#deadLetter Dead-letter} policies are not supported by this facade —
 * there is no dead-letter sink — and are rejected by {@link Builder#build()}. To dead-letter evicted
 * records, build a custom topology with {@link CausalProcessors} instead.
 */
public final class CausalConsumers {

    private CausalConsumers() {}

    /**
     * Starts building a running {@link CausalConsumer} subscribed to {@code topics}.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy; must not be a
     *                       {@link CausalBufferPolicy#deadLetter dead-letter} policy
     * @param consumerConfig additional consumer configuration (overrides defaults derived from
     *                       {@code streamsConfig})
     * @param streamsConfig  Kafka Streams configuration; must include at minimum
     *                       {@code application.id} and {@code bootstrap.servers}
     * @return a {@link Builder} for a {@code CausalConsumer}
     */
    public static <K, V> Builder<K, V> builder(
            Collection<String> topics,
            CausalBufferPolicy policy,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return new Builder<>(topics, policy, consumerConfig, streamsConfig);
    }

    /**
     * Builder for a {@link CausalConsumer}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    public static final class Builder<K, V> {

        private final Collection<String> topics;
        private final CausalBufferPolicy policy;
        private final Map<String, Object> consumerConfig;
        private final Map<String, Object> streamsConfig;
        private CausalViolationHandler onViolation = violation -> {};
        private String storeName = "parsley";

        private Builder(Collection<String> topics, CausalBufferPolicy policy,
                        Map<String, Object> consumerConfig, Map<String, Object> streamsConfig) {
            this.topics = topics;
            this.policy = policy;
            this.consumerConfig = consumerConfig;
            this.streamsConfig = streamsConfig;
        }

        /**
         * Sets the callback invoked when a record cannot be delivered in causal order (default: ignore).
         *
         * @param onViolation the violation handler
         * @return this builder
         */
        public Builder<K, V> onViolation(CausalViolationHandler onViolation) {
            this.onViolation = onViolation;
            return this;
        }

        /**
         * Sets the state-store namespace (default {@code "parsley"}). The frontier store is
         * {@code storeName + "-frontier"} and the buffer store {@code storeName + "-buffer"}; these
         * name the backing changelog topics, so keep {@code storeName} stable across restarts.
         *
         * @param storeName the state-store namespace
         * @return this builder
         */
        public Builder<K, V> storeName(String storeName) {
            this.storeName = storeName;
            return this;
        }

        /**
         * Builds and starts the {@link CausalConsumer}.
         *
         * @return a new, running {@code CausalConsumer}
         * @throws IllegalArgumentException if {@code policy} is a
         *                                  {@link CausalBufferPolicy#deadLetter dead-letter} policy
         */
        public CausalConsumer<K, V> build() {
            return new ParsleyConsumer<>(topics, policy, onViolation, consumerConfig, streamsConfig, storeName);
        }
    }
}
