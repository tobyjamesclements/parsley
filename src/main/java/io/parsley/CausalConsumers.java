package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Factory for {@link CausalConsumer}. Obtain a {@link Builder} with
 * {@link #builder(Collection, CausalBufferPolicy, Map, Map)}, set any optional fields, and call
 * {@link Builder#build()}:
 *
 * <pre>{@code
 * CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
 *         List.of("prices", "orders"),
 *         CausalBufferPolicy.deadLetter(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         Map.of(), streamsConfig)
 *         .deadLetterSink(deadLetterSink)
 *         .build();
 * }</pre>
 *
 * <p>Policies where any violation type uses {@link ViolationAction#DEAD_LETTER} require a
 * {@link Builder#deadLetterSink(Consumer) dead-letter sink}; {@link Builder#build()} rejects a
 * dead-letter policy with no sink, and a sink set on a non-dead-letter policy.
 */
public final class CausalConsumers {

    private CausalConsumers() {}

    /**
     * Starts building a running {@link CausalConsumer} subscribed to {@code topics}.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy; if it uses {@link ViolationAction#DEAD_LETTER}
     *                       for any violation type, {@link Builder#deadLetterSink} must be set
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
        private ParsleyTopicAdmin topicAdmin = null;
        private Consumer<ConsumerRecord<K, V>> deadLetterSink = null;

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
         * Overrides the {@link ParsleyTopicAdmin} used for outbox setup (default: a live Kafka
         * {@link org.apache.kafka.clients.admin.Admin} created from {@code bootstrap.servers}).
         * Supply a {@code MockAdminClient} in tests to avoid a real broker dependency or to force
         * deterministic name-derived UUIDs so that {@link CausalPosition#deriveUuid} produces
         * the same UUID the consumer uses for those topics, keeping test UUIDs consistent.
         *
         * @param topicAdmin the {@link ParsleyTopicAdmin} to use; closed automatically after outbox setup
         * @return this builder
         */
        public Builder<K, V> topicAdmin(ParsleyTopicAdmin topicAdmin) {
            this.topicAdmin = topicAdmin;
            return this;
        }

        /**
         * Sets the sink that receives records evicted under a
         * {@link CausalBufferPolicy#deadLetter dead-letter} policy, delivered as typed
         * {@link ConsumerRecord}s — deserialized with the same key/value serdes as
         * {@link CausalConsumer#poll}, at the record's original source topic/partition/offset, and
         * carrying the {@code parsley-dlq-*} headers described in {@link CausalViolation}. Required
         * for a dead-letter policy and illegal for any other.
         *
         * <p>Runs on the internal Kafka Streams thread, so the sink must be thread-safe — the same
         * caveat as {@link #onViolation}.
         *
         * @param deadLetterSink the dead-letter sink
         * @return this builder
         */
        public Builder<K, V> deadLetterSink(Consumer<ConsumerRecord<K, V>> deadLetterSink) {
            this.deadLetterSink = deadLetterSink;
            return this;
        }

        /**
         * Builds and starts the {@link CausalConsumer}.
         *
         * @return a new, running {@code CausalConsumer}
         * @throws IllegalArgumentException if {@code policy} is a
         *                                  {@link CausalBufferPolicy#deadLetter dead-letter} policy
         *                                  and no sink was set via {@link #deadLetterSink}, or a sink
         *                                  was set but {@code policy} is not a dead-letter policy
         */
        public CausalConsumer<K, V> build() {
            boolean needsSink = policy.requiresDeadLetterSink();
            if (needsSink && deadLetterSink == null) {
                throw new IllegalArgumentException(
                        "Policy requires a dead-letter sink — call deadLetterSink(...)");
            }
            if (!needsSink && deadLetterSink != null) {
                throw new IllegalArgumentException(
                        "A dead-letter sink is only valid when at least one violation type uses DEAD_LETTER");
            }
            return new ParsleyConsumer<>(
                    topics, policy, onViolation, consumerConfig, streamsConfig, storeName, topicAdmin, deadLetterSink);
        }
    }
}
