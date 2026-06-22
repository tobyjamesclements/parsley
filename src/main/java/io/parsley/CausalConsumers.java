package io.parsley;

import org.apache.kafka.common.serialization.Serde;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for {@link CausalConsumer}. Obtain a {@link Builder} with
 * {@link #builder(CausalBufferLimit, Map, Map)}, register a {@link CausalBuffer} for every
 * subscribed topic, and call {@link Builder#build()}:
 *
 * <pre>{@code
 * CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
 *         CausalBufferLimit.ofDuration(Duration.ofSeconds(30)),
 *         Map.of(), streamsConfig)
 *         .addBuffer(CausalBuffer.of("prices", Serdes.String(), orderSerde))
 *         .addBuffer(CausalBuffer.of("orders", Serdes.String(), orderSerde))
 *         .build();
 * }</pre>
 *
 * <p>The subscribed topics are the registered buffers' topics; each topic's stable UUID is resolved
 * from the broker automatically at startup.
 */
public final class CausalConsumers {

    private CausalConsumers() {}

    /**
     * Starts building a running {@link CausalConsumer}. Subscribe to topics by registering a
     * {@link CausalBuffer} for each.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
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
            CausalBufferLimit limit,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return new Builder<>(limit, consumerConfig, streamsConfig);
    }

    /**
     * Builder for a {@link CausalConsumer}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    public static final class Builder<K, V> {

        private final CausalBufferLimit limit;
        private final Map<String, Object> consumerConfig;
        private final Map<String, Object> streamsConfig;
        private final Map<String, CausalBuffer<K, V>> buffers = new LinkedHashMap<>();
        private String storeName = "parsley";
        private ParsleyTopicAdmin topicAdmin = null;

        private Builder(CausalBufferLimit limit,
                        Map<String, Object> consumerConfig, Map<String, Object> streamsConfig) {
            this.limit = limit;
            this.consumerConfig = consumerConfig;
            this.streamsConfig = streamsConfig;
        }

        /**
         * Subscribes to one topic and registers the serdes its records are deserialised with on
         * {@code poll()}. Required for every topic the consumer should read; the topic's stable UUID
         * is resolved from the broker automatically at startup.
         *
         * @param buffer the source topic and its serdes; must not be {@code null}
         * @return this builder
         */
        public Builder<K, V> addBuffer(CausalBuffer<K, V> buffer) {
            buffers.put(buffer.topic(), buffer);
            return this;
        }

        /**
         * Subscribes to several topics at once. Equivalent to calling {@link #addBuffer} for each.
         *
         * @param buffers the source buffers; must not be {@code null}
         * @return this builder
         */
        public Builder<K, V> addBuffers(Collection<CausalBuffer<K, V>> buffers) {
            for (CausalBuffer<K, V> buffer : buffers) {
                addBuffer(buffer);
            }
            return this;
        }

        /**
         * Subscribes to several topics that share one serde pair — the convenience for the
         * homogeneous case.
         *
         * @param topics the source topic names
         * @param key    the key serde shared by all {@code topics}
         * @param value  the value serde shared by all {@code topics}
         * @return this builder
         */
        public Builder<K, V> addBuffers(Collection<String> topics, Serde<K> key, Serde<V> value) {
            for (String topic : topics) {
                addBuffer(CausalBuffer.of(topic, key, value));
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
         * Overrides the {@link ParsleyTopicAdmin} used at startup to resolve the subscribed topics'
         * stable UUIDs and to size and create the outbox topic (default: a live Kafka
         * {@link org.apache.kafka.clients.admin.Admin} created from {@code bootstrap.servers}). For
         * tests without a broker.
         *
         * @param topicAdmin the {@link ParsleyTopicAdmin} to use; closed automatically after startup
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
         * @throws IllegalStateException if no {@link CausalBuffer} was registered
         */
        public CausalConsumer<K, V> build() {
            if (buffers.isEmpty()) {
                throw new IllegalStateException(
                        "at least one CausalBuffer is required; call addBuffer(...) for every subscribed topic");
            }
            return new ParsleyConsumer<>(
                    Map.copyOf(buffers), limit, consumerConfig, streamsConfig, storeName, topicAdmin);
        }
    }
}
