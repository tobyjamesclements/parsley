package io.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.Collection;
import java.util.LinkedHashMap;
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
 *                .addBuffer(CausalBuffer.of("prices", Serdes.String(), orderSerde))
 *                .addBuffer(CausalBuffer.of("orders", Serdes.String(), orderSerde))
 *                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <p>Each input topic's stable UUID is resolved from the broker automatically at startup, so a
 * {@link CausalBuffer} only carries the per-topic serdes the buffer round-trips held records with.
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
     *                     dependencies before forwarding it anyway (out of causal order, logged and
     *                     counted by the violation metric)
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
     * Builder for a {@link CausalProcessorSupplier}. At least one {@link CausalBuffer} is required
     * (via {@link #addBuffer}/{@link #addBuffers}); the store namespace is optional.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    public static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private final CausalBufferLimit limit;
        private String storeName = "parsley";
        private final Map<String, CausalBuffer<KIn, VIn>> buffers = new LinkedHashMap<>();
        private Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory = ParsleyTopicAdmin::ofConfigs;
        private ParsleyConfig config = null;

        private Builder(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier, CausalBufferLimit limit) {
            this.userSupplier = userSupplier;
            this.limit = limit;
        }

        /**
         * Registers a causal source: its topic name and the serdes the buffer (de)serialises held
         * records from that topic with. Required for every input topic this processor will see; the
         * topic's stable UUID is resolved from the broker automatically at startup.
         *
         * @param buffer the source topic and its serdes; must not be {@code null}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addBuffer(CausalBuffer<KIn, VIn> buffer) {
            buffers.put(buffer.topic(), buffer);
            return this;
        }

        /**
         * Registers several causal sources at once. Equivalent to calling {@link #addBuffer} for each.
         *
         * @param buffers the source buffers; must not be {@code null}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addBuffers(Collection<CausalBuffer<KIn, VIn>> buffers) {
            for (CausalBuffer<KIn, VIn> buffer : buffers) {
                addBuffer(buffer);
            }
            return this;
        }

        /**
         * Registers several topics that share one serde pair — the convenience for the homogeneous
         * case, mirroring a single {@code stream(topics, Consumed.with(key, value))}.
         *
         * @param topics the source topic names
         * @param key    the key serde shared by all {@code topics}
         * @param value  the value serde shared by all {@code topics}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addBuffers(
                Collection<String> topics, Serde<KIn> key, Serde<VIn> value) {
            for (String topic : topics) {
                addBuffer(CausalBuffer.of(topic, key, value));
            }
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
         * Overrides the {@link ParsleyTopicAdmin} used to resolve topic UUIDs at startup (default: a
         * live {@link org.apache.kafka.clients.admin.Admin} built from the task's {@code appConfigs()}).
         * For tests running under {@code TopologyTestDriver} with no broker.
         *
         * @param topicAdmin the admin to resolve UUIDs with
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> topicAdmin(ParsleyTopicAdmin topicAdmin) {
            this.adminFactory = configs -> topicAdmin;
            return this;
        }

        /**
         * Overrides the {@link ParsleyConfig} (default: loaded from the {@code parsley.properties}
         * classpath resource). For tests / embedding.
         */
        Builder<KIn, VIn, KOut, VOut> config(ParsleyConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Builds the {@link CausalProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException if no {@link CausalBuffer} was registered
         */
        public CausalProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (buffers.isEmpty()) {
                throw new IllegalStateException(
                        "at least one CausalBuffer is required; call addBuffer(...) for every input topic");
            }
            Map<String, CausalBuffer<KIn, VIn>> resolved = Map.copyOf(buffers);
            Function<String, Serde<KIn>> keySerdeByTopic = topic -> serdeFor(resolved, topic).keySerde();
            Function<String, Serde<VIn>> valueSerdeByTopic = topic -> serdeFor(resolved, topic).valueSerde();
            ParsleyConfig effectiveConfig = config != null ? config : ParsleyConfig.load();
            return new ParsleyProcessorSupplier<>(
                    userSupplier, limit, keySerdeByTopic, valueSerdeByTopic,
                    storeName + "-frontier", storeName + "-buffer", storeName + "-position-index",
                    resolved.keySet(), adminFactory, effectiveConfig);
        }

        private static <KIn, VIn> CausalBuffer<KIn, VIn> serdeFor(
                Map<String, CausalBuffer<KIn, VIn>> buffers, String topic) {
            CausalBuffer<KIn, VIn> buffer = buffers.get(topic);
            if (buffer == null) {
                throw new IllegalStateException("no CausalBuffer registered for topic '" + topic + "'");
            }
            return buffer;
        }
    }
}
