package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Factory for {@link CausalProcessorSupplier} — the decorating causal processor you drop into a Kafka
 * Streams topology with {@code stream(...).process(...)}.
 *
 * <p>Obtain a {@link Builder} with {@link #builder(ProcessorSupplier)}, declare the buffer store with
 * {@link Builder#addBufferStore(String)}, register a {@link CausalBuffer} for every input topic, then
 * call {@link Builder#build()}:
 *
 * <pre>{@code
 * builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
 *        .process(CausalProcessors.builder(userSupplier)
 *                .addBufferStore("parsley")
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
     * causal guarantee. Declare the buffer store with {@link Builder#addBufferStore(String)} before
     * {@link Builder#build()}.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for a {@code CausalProcessorSupplier}
     * @throws IllegalArgumentException if {@code userSupplier} is already a
     *         {@link CausalProcessorSupplier} — decorating an already-decorated supplier would
     *         buffer and stamp every record twice, nested, silently corrupting the frontier
     */
    public static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
        if (userSupplier instanceof CausalProcessorSupplier) {
            throw new IllegalArgumentException(
                    "userSupplier is already a CausalProcessorSupplier; decorating it again would "
                            + "buffer and stamp every record twice, nested, silently corrupting the "
                            + "frontier — pass the original, undecorated supplier instead");
        }
        return new Builder<>(userSupplier);
    }

    /**
     * Builder for a {@link CausalProcessorSupplier}. A buffer store
     * (via {@link #addBufferStore(String)}) and at least one {@link CausalBuffer}
     * (via {@link #addBuffer}/{@link #addBuffers}) are required; Parsley's own configuration is
     * optional.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    public static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private @Nullable String storeName = null;
        private final Map<String, CausalBuffer<KIn, VIn>> buffers = new LinkedHashMap<>();
        private final Properties config = new Properties();
        private Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory = ParsleyTopicAdmin::ofConfigs;
        private @Nullable ParsleyConfig configOverride = null;
        private CausalAudit audit = CausalAudit.NOOP;
        private Set<String> sinkTopics = Set.of();
        private @Nullable CausalQuiesce quiesce = null;
        private @Nullable CausalCoordination coordination = null;

        private Builder(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
            this.userSupplier = userSupplier;
        }

        /**
         * Declares the buffer state store's namespace. The buffer is unbounded: a held record waits
         * until its causal dependencies are satisfied (the buffer is changelog-backed and spills to
         * disk, so it does not grow in memory). There is no eviction or size limit.
         *
         * <p>{@code name} is the state-store namespace: the frontier store is {@code name + "-frontier"}
         * (holding the contiguous frontier clock and the per-channel clocks as one value), the
         * held-record buffer store {@code name + "-buffer"}, the candidate index
         * {@code name + "-candidate-index"}, and the forwarded-offset index
         * {@code name + "-forwarded-index"}. These name the backing changelog topics, so keep
         * {@code name} stable across restarts, and unique per causal processor sharing a topology.
         *
         * @param name the state-store namespace
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addBufferStore(String name) {
            this.storeName = name;
            return this;
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
         * Sets Parsley's own configuration from a map of key/value pairs, mirroring how Kafka Streams
         * configuration is supplied. Keys are overlaid on top of any {@code parsley.properties}
         * classpath resource. See {@link ParsleyConfig} for the recognised keys.
         *
         * @param configs the configuration entries; values are recorded as their string form
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withConfigs(Map<String, Object> configs) {
            configs.forEach((key, value) -> config.setProperty(key, String.valueOf(value)));
            return this;
        }

        /**
         * Sets Parsley's own configuration from a {@link Properties}, as you might load from a
         * properties file. Keys are overlaid on top of any {@code parsley.properties} classpath
         * resource. See {@link ParsleyConfig} for the recognised keys.
         *
         * @param props the configuration properties
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withConfig(Properties props) {
            props.forEach((key, value) -> config.setProperty(String.valueOf(key), String.valueOf(value)));
            return this;
        }

        /**
         * Sets a single Parsley configuration entry, overlaid on top of any {@code parsley.properties}
         * classpath resource. See {@link ParsleyConfig} for the recognised keys.
         *
         * @param key   the configuration key
         * @param value the configuration value; recorded as its string form
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withConfig(String key, Object value) {
            config.setProperty(key, String.valueOf(value));
            return this;
        }

        /**
         * Registers a {@link CausalAudit} to receive this processor's per-record causal events
         * (forwarded, held, released, evicted, undecodable), for routing to your own audit/compliance
         * trail. Optional — without one, these events are observable only through Parsley's logs and
         * metrics.
         *
         * <p>An exception thrown from the audit is caught and logged; it never fails a record or the
         * Streams task. See {@link CausalAudit} for the full contract.
         *
         * @param audit the audit to notify; must not be {@code null}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withAudit(CausalAudit audit) {
            this.audit = audit;
            return this;
        }

        /**
         * Registers this processor's tasks with a {@link CausalQuiesce} for coordinated graceful
         * shutdown. Optional — without one, tasks process and close exactly as they do today, with no
         * quiesce tracking.
         *
         * @param quiesce the quiesce coordinator every task instance registers with
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withQuiesce(CausalQuiesce quiesce) {
            this.quiesce = quiesce;
            return this;
        }

        /**
         * Registers this stage's tasks with a {@link CausalCoordination} to participate in topology-epoch
         * coordination. Optional — without one, the stage runs in epoch 0 (no epoch-events log, no
         * coordination thread), exactly as today.
         *
         * @param coordination the coordination handle shared across every participating stage
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withCoordination(CausalCoordination coordination) {
            this.coordination = coordination;
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
            this.configOverride = config;
            return this;
        }

        /**
         * Declares the topics this stage produces. This serves two purposes:
         * <ul>
         *   <li>Their partition counts are folded into the startup co-partitioning check
         *       ({@code parsley.topology.validation}) alongside the registered input buffers, without
         *       consuming them or resolving their UUIDs. A topic that cannot be described (e.g. a sink
         *       not yet created) is skipped rather than failing the task — unlike a registered input
         *       buffer, a sink is not required to exist before the stage starts.
         *   <li>When topology-epoch coordination is configured ({@link #withCoordination}), they form
         *       this member's declaration on the shared log, from which the DAG-wide source-topic
         *       registry is derived (an external source = a topic some member consumes but no member
         *       produces). Declare them so a downstream consumer of a sink is not mistaken for a
         *       source-layer stage.
         * </ul>
         * {@link CausalStreams} sets this automatically from its {@code addSink(...)} declarations; on
         * the low-level decorator path, declare it here.
         *
         * @param topics the stage's output topic names
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> sinkTopics(Set<String> topics) {
            this.sinkTopics = Set.copyOf(topics);
            return this;
        }

        /**
         * Builds the {@link CausalProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException if no buffer store or no {@link CausalBuffer} was declared
         */
        public CausalProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (storeName == null) {
                throw new IllegalStateException(
                        "a buffer store is required; call addBufferStore(name)");
            }
            if (buffers.isEmpty()) {
                throw new IllegalStateException(
                        "at least one CausalBuffer is required; call addBuffer(...) for every input topic");
            }
            String store = storeName;
            Map<String, CausalBuffer<KIn, VIn>> resolved = Map.copyOf(buffers);
            Function<String, Serde<KIn>> keySerdeByTopic = topic -> serdeFor(resolved, topic).keySerde();
            Function<String, Serde<VIn>> valueSerdeByTopic = topic -> serdeFor(resolved, topic).valueSerde();
            ParsleyConfig effectiveConfig = configOverride != null ? configOverride : effectiveConfig();
            return new ParsleyProcessorSupplier<>(
                    userSupplier, keySerdeByTopic, valueSerdeByTopic,
                    store + "-frontier", store + "-buffer", store + "-candidate-index", store + "-forwarded-index",
                    resolved.keySet(), sinkTopics, adminFactory, effectiveConfig,
                    ParsleyAudit.wrap(audit), quiesce, coordination);
        }

        /** Classpath {@code parsley.properties} as a base layer, overlaid with builder-supplied keys. */
        private ParsleyConfig effectiveConfig() {
            Properties props = ParsleyConfig.loadProperties();
            props.putAll(config);
            return ParsleyConfig.from(props);
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
