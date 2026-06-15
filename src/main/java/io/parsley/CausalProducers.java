package io.parsley;

import java.util.Map;

/**
 * Factory for {@link CausalProducer}. Obtain a {@link Builder} with {@link #builder(Map)} and call
 * {@link Builder#build()}:
 *
 * <pre>{@code
 * CausalProducer<String, String> producer =
 *         CausalProducers.<String, String>builder(producerConfig).build();
 * }</pre>
 */
public final class CausalProducers {

    private CausalProducers() {}

    /**
     * Starts building a {@link CausalProducer} over the given Kafka producer configuration.
     *
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @param config Kafka producer configuration ({@code ProducerConfig} properties); must include at
     *               minimum {@code bootstrap.servers}, {@code key.serializer}, and
     *               {@code value.serializer}
     * @return a {@link Builder} for a {@code CausalProducer}
     */
    public static <K, V> Builder<K, V> builder(Map<String, Object> config) {
        return new Builder<>(config);
    }

    /**
     * Builder for a {@link CausalProducer}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    public static final class Builder<K, V> {

        private final Map<String, Object> config;

        private Builder(Map<String, Object> config) {
            this.config = config;
        }

        /**
         * Builds the {@link CausalProducer}.
         *
         * @return a new {@code CausalProducer}
         */
        public CausalProducer<K, V> build() {
            return new ParsleyProducer<>(config);
        }
    }
}
