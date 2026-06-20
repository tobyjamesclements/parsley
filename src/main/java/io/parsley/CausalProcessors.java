package io.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.Map;
import java.util.function.Function;

/**
 * Factory for {@link CausalProcessorSupplier} — the decorating causal processor you drop into a Kafka
 * Streams topology with {@code stream(...).process(...)}.
 *
 * <p>Obtain a {@link Builder} with {@link #builder(ProcessorSupplier, CausalBufferLimit)}, set the
 * required serdes and any optional fields, then call {@link Builder#build()}:
 *
 * <pre>{@code
 * builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
 *        .process(CausalProcessors.builder(userSupplier, CausalBufferLimit.ofDuration(limit))
 *                .serdes(Serdes.String(), orderSerde)
 *                .onViolation(onViolation)
 *                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <p>See {@link CausalProcessorSupplier} for the causal guarantee and its preconditions.
 */
public final class CausalProcessors {

    private CausalProcessors() {}

    /**
     * Starts building a {@link CausalProcessorSupplier} that wraps {@code userSupplier} behind the
     * causal guarantee.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param limit        the buffer eviction trigger — how long to wait for a record's
     *                     dependencies before forwarding it anyway, stamped
     *                     {@link CausalResult#EVICTED}
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for a {@code CausalProcessorSupplier}
     */
    public static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferLimit limit) {
        return new Builder<>(userSupplier, limit);
    }

    /**
     * Builder for a {@link CausalProcessorSupplier}. A serde pair is required (via {@link #serdes} or
     * {@link #serdesByTopic}); the violation handler, store namespace, and frontier listener are
     * optional.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    public static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private final CausalBufferLimit limit;
        private Function<String, Serde<KIn>> keySerdeByTopic;
        private Function<String, Serde<VIn>> valueSerdeByTopic;
        private CausalViolationHandler onViolation = violation -> {};
        private String storeName = "parsley";
        private CausalFrontierListener frontierListener = frontier -> {};
        private Map<String, Uuid> topicUuids = Map.of();

        private Builder(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier, CausalBufferLimit limit) {
            this.userSupplier = userSupplier;
            this.limit = limit;
        }

        /**
         * Sets one serde pair for all input topics (the serdes the buffer (de)serialises held records
         * with).
         *
         * @param keySerde   the key serde
         * @param valueSerde the value serde
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> serdes(Serde<KIn> keySerde, Serde<VIn> valueSerde) {
            this.keySerdeByTopic = topic -> keySerde;
            this.valueSerdeByTopic = topic -> valueSerde;
            return this;
        }

        /**
         * Sets the buffer serdes resolved by source topic, so input topics with distinct serdes (e.g.
         * Avro {@code SpecificRecord} / Schema Registry) round-trip correctly.
         *
         * @param keySerdeByTopic   resolves the key serde for a given source topic
         * @param valueSerdeByTopic resolves the value serde for a given source topic
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> serdesByTopic(
                Function<String, Serde<KIn>> keySerdeByTopic,
                Function<String, Serde<VIn>> valueSerdeByTopic) {
            this.keySerdeByTopic = keySerdeByTopic;
            this.valueSerdeByTopic = valueSerdeByTopic;
            return this;
        }

        /**
         * Sets the violation callback, invoked when a record is evicted from the buffer (default:
         * ignore).
         *
         * @param onViolation the violation handler
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> onViolation(CausalViolationHandler onViolation) {
            this.onViolation = onViolation;
            return this;
        }

        /**
         * Sets the state-store namespace (default {@code "parsley"}); the frontier store is
         * {@code storeName + "-frontier"} and the buffer store {@code storeName + "-buffer"}. These
         * are persistent, changelog-backed names — keep {@code storeName} stable across restarts, and
         * unique per causal processor sharing a topology.
         *
         * @param storeName the state-store namespace
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> storeName(String storeName) {
            this.storeName = storeName;
            return this;
        }

        /**
         * Sets a callback invoked with the new frontier after every advance, and once with the
         * restored frontier at startup (default: ignore). Must be thread-safe — see
         * {@link CausalFrontierListener}.
         *
         * @param frontierListener the frontier listener
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> frontierListener(CausalFrontierListener frontierListener) {
            this.frontierListener = frontierListener;
            return this;
        }

        /**
         * Provides the Kafka topic UUIDs for the input topics, keyed by topic name. Used as the
         * stable partition identity so that topic deletion and recreation produce different
         * identities. Topics absent from the map fall back to a deterministic name-derived UUID via
         * {@link CausalPosition#deriveUuid}.
         *
         * @param topicUuids topic-name → Kafka UUID map; typically from
         *                   {@code AdminClient.describeTopics(topics)}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> topicUuids(Map<String, Uuid> topicUuids) {
            this.topicUuids = Map.copyOf(topicUuids);
            return this;
        }

        /**
         * Builds the {@link CausalProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException if no serde pair was set
         */
        public CausalProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (keySerdeByTopic == null || valueSerdeByTopic == null) {
                throw new IllegalStateException("serdes are required; call serdes(...) or serdesByTopic(...)");
            }
            return new ParsleyProcessorSupplier<>(
                    userSupplier, limit, onViolation, keySerdeByTopic, valueSerdeByTopic,
                    storeName + "-frontier", storeName + "-buffer", storeName + "-position-index",
                    frontierListener, topicUuids);
        }
    }
}
