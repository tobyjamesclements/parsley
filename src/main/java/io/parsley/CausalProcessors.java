package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Factory for {@link CausalProcessorSupplier} — the decorating causal processor you drop into a Kafka
 * Streams topology with {@code stream(...).process(...)}.
 *
 * <p>Obtain a {@link Builder} with {@link #builder(ProcessorSupplier, CausalBufferingPolicy)}, set the
 * required serdes and any optional fields, then call {@link Builder#build()}:
 *
 * <pre>{@code
 * builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
 *        .process(CausalProcessors.builder(userSupplier, CausalBufferingPolicy.deadLetter(limit, "dlq"))
 *                .serdes(Serdes.String(), orderSerde)
 *                .onViolation(onViolation)
 *                .deadLetterSink(deadLetterSink)
 *                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <p>See {@link CausalProcessorSupplier} for the causal guarantee and its three preconditions.
 */
public final class CausalProcessors {

    private CausalProcessors() {}

    /**
     * Starts building a {@link CausalProcessorSupplier} that wraps {@code userSupplier} behind the
     * causal guarantee.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param policy       the buffering policy; if it is a
     *                     {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy, a
     *                     {@link Builder#deadLetterSink(Consumer) dead-letter sink} is required
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for a {@code CausalProcessorSupplier}
     */
    public static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy policy) {
        return new Builder<>(userSupplier, policy);
    }

    /**
     * Builder for a {@link CausalProcessorSupplier}. A serde pair is required (via {@link #serdes} or
     * {@link #serdesByTopic}); the violation handler, store namespace, and frontier listener are
     * optional; a dead-letter sink is required exactly when the policy is a
     * {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    public static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private final CausalBufferingPolicy policy;
        private Function<String, Serde<KIn>> keySerdeByTopic;
        private Function<String, Serde<VIn>> valueSerdeByTopic;
        private CausalViolationHandler onViolation = violation -> {};
        private String storeName = "parsley";
        private CausalFrontierListener frontierListener = frontier -> {};
        private Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink;

        private Builder(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier, CausalBufferingPolicy policy) {
            this.userSupplier = userSupplier;
            this.policy = policy;
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
         * Sets the violation callback (default: ignore).
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
         * Sets the sink that receives evicted records under a
         * {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy. Required for a DeadLetter policy
         * and illegal for any other.
         *
         * @param deadLetterSink the dead-letter sink
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> deadLetterSink(Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink) {
            this.deadLetterSink = deadLetterSink;
            return this;
        }

        /**
         * Builds the {@link CausalProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException    if no serde pair was set
         * @throws IllegalArgumentException if the policy is a DeadLetter policy and no sink was set,
         *                                  or a sink was set with a non-DeadLetter policy
         */
        public CausalProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (keySerdeByTopic == null || valueSerdeByTopic == null) {
                throw new IllegalStateException("serdes are required; call serdes(...) or serdesByTopic(...)");
            }
            boolean isDeadLetter = policy instanceof CausalBufferingPolicy.DeadLetter;
            if (isDeadLetter && deadLetterSink == null) {
                throw new IllegalArgumentException(
                        "DeadLetter policy requires a dead-letter sink — call deadLetterSink(...)");
            }
            if (!isDeadLetter && deadLetterSink != null) {
                throw new IllegalArgumentException(
                        "a dead-letter sink is only valid with a DeadLetter policy");
            }
            return new ParsleyProcessorSupplier<>(
                    userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic,
                    storeName + "-frontier", storeName + "-buffer", frontierListener);
        }
    }
}
