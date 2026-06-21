package io.parsley;

import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for {@link CausalConsumer}. Obtain a {@link Builder} with
 * {@link #builder(Collection, CausalBufferLimit, Map, Map)}, register a {@link CausalTopic} for
 * every subscribed topic, and call {@link Builder#build()}:
 *
 * <pre>{@code
 * CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
 *         List.of("prices", "orders"),
 *         CausalBufferLimit.ofDuration(Duration.ofSeconds(30)),
 *         Map.of(), streamsConfig)
 *         .addCausalTopic(new CausalTopic("prices", pricesTopicId))
 *         .addCausalTopic(new CausalTopic("orders", ordersTopicId))
 *         .build();
 * }</pre>
 */
public final class CausalConsumers {

    private CausalConsumers() {}

    /**
     * Starts building a running {@link CausalConsumer} subscribed to {@code topics}.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param limit          the buffer eviction trigger — how long to wait for a record's
     *                       dependencies before delivering it anyway (out of causal order, logged
     *                       and counted by the violation metric)
     * @param consumerConfig additional consumer configuration (overrides defaults derived from
     *                       {@code streamsConfig})
     * @param streamsConfig  Kafka Streams configuration; must include at minimum
     *                       {@code application.id} and {@code bootstrap.servers}
     * @return a {@link Builder} for a {@code CausalConsumer}
     */
    public static <K, V> Builder<K, V> builder(
            Collection<String> topics,
            CausalBufferLimit limit,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return new Builder<>(topics, limit, consumerConfig, streamsConfig);
    }

    /**
     * Builder for a {@link CausalConsumer}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    public static final class Builder<K, V> {

        private final Collection<String> topics;
        private final CausalBufferLimit limit;
        private final Map<String, Object> consumerConfig;
        private final Map<String, Object> streamsConfig;
        private final Map<String, Uuid> topicUuids = new HashMap<>();
        private String storeName = "parsley";
        private ParsleyTopicAdmin topicAdmin = null;

        private Builder(Collection<String> topics, CausalBufferLimit limit,
                        Map<String, Object> consumerConfig, Map<String, Object> streamsConfig) {
            this.topics = topics;
            this.limit = limit;
            this.consumerConfig = consumerConfig;
            this.streamsConfig = streamsConfig;
        }

        /**
         * Registers the stable causal identity for one subscribed topic. Required for every topic
         * passed to {@link CausalConsumers#builder}; {@link #build()} throws if any is missing.
         *
         * @param stream the topic name and its Kafka UUID; must not be {@code null}
         * @return this builder
         */
        public Builder<K, V> addCausalTopic(CausalTopic stream) {
            topicUuids.put(stream.topic(), stream.topicId());
            return this;
        }

        /**
         * Registers the stable causal identity for several subscribed topics at once. Equivalent to
         * calling {@link #addCausalTopic} for each element.
         *
         * @param streams the topic names and their Kafka UUIDs; must not be {@code null}
         * @return this builder
         */
        public Builder<K, V> addCausalTopics(Collection<CausalTopic> streams) {
            for (CausalTopic stream : streams) {
                addCausalTopic(stream);
            }
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
         * Overrides the {@link ParsleyTopicAdmin} used to size and create the outbox topic (default:
         * a live Kafka {@link org.apache.kafka.clients.admin.Admin} created from
         * {@code bootstrap.servers}). This governs outbox topic creation only — topic identity comes
         * from {@link #addCausalTopic}, not from this admin client.
         *
         * @param topicAdmin the {@link ParsleyTopicAdmin} to use; closed automatically after outbox setup
         * @return this builder
         */
        public Builder<K, V> topicAdmin(ParsleyTopicAdmin topicAdmin) {
            this.topicAdmin = topicAdmin;
            return this;
        }

        /**
         * Builds and starts the {@link CausalConsumer}.
         *
         * @return a new, running {@code CausalConsumer}
         * @throws IllegalStateException if any topic passed to {@link CausalConsumers#builder} has
         *                               no corresponding {@link #addCausalTopic} call
         */
        public CausalConsumer<K, V> build() {
            for (String topic : topics) {
                if (!topicUuids.containsKey(topic)) {
                    throw new IllegalStateException(
                            "no CausalTopic registered for subscribed topic '" + topic
                                    + "'; call addCausalTopic(...) for every topic passed to builder(...)");
                }
            }
            return new ParsleyConsumer<>(
                    topics, limit, consumerConfig, streamsConfig, storeName, topicAdmin, Map.copyOf(topicUuids));
        }
    }
}
