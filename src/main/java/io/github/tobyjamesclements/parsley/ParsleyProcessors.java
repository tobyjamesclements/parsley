package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Factory for {@link ParsleyProcessorSupplier} — the decorating causal processor you drop into a Kafka
 * Streams topology with {@code stream(...).process(...)}.
 *
 * <p>Obtain a {@link Builder} with {@link #builder(ProcessorSupplier)}, declare the buffer store with
 * {@link Builder#addBufferStore(String)}, register a {@link ParsleyBuffer} for every input topic, then
 * call {@link Builder#build()}:
 *
 * <pre>{@code
 * builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
 *        .process(ParsleyProcessors.builder(userSupplier)
 *                .addBufferStore("parsley")
 *                .addBuffer(new ParsleyBuffer<>("prices", Serdes.String(), orderSerde))
 *                .addBuffer(new ParsleyBuffer<>("orders", Serdes.String(), orderSerde))
 *                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <p>Each input topic's stable UUID is resolved from the broker automatically at startup, so a
 * {@link ParsleyBuffer} only carries the per-topic serdes the buffer round-trips held records with.
 *
 * <p>See {@link ParsleyProcessorSupplier} for the causal guarantee and its preconditions.
 */
final class ParsleyProcessors {

    private ParsleyProcessors() {}

    /**
     * Starts building a {@link ParsleyProcessorSupplier} that wraps {@code userSupplier} behind the
     * causal guarantee. Declare the buffer store with {@link Builder#addBufferStore(String)} before
     * {@link Builder#build()}.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for a {@code ParsleyProcessorSupplier}
     * @throws IllegalArgumentException if {@code userSupplier} is already a
     *         {@link ParsleyProcessorSupplier} — decorating an already-decorated supplier would
     *         buffer and stamp every record twice, nested, silently corrupting the frontier
     */
    static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
        if (userSupplier instanceof ParsleyProcessorSupplier) {
            throw new IllegalArgumentException(
                    "userSupplier is already a ParsleyProcessorSupplier; decorating it again would "
                            + "buffer and stamp every record twice, nested, silently corrupting the "
                            + "frontier — pass the original, undecorated supplier instead");
        }
        return new Builder<>(userSupplier);
    }

    /**
     * Builder for a {@link ParsleyProcessorSupplier}. A buffer store
     * (via {@link #addBufferStore(String)}) and at least one {@link ParsleyBuffer}
     * (via {@link #addBuffer}/{@link #addBuffers}) are required; Parsley's own configuration is
     * optional.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private @Nullable String storeName = null;
        private final Map<String, ParsleyBuffer<KIn, VIn>> buffers = new LinkedHashMap<>();
        private final Properties config = new Properties();
        private Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory = ParsleyTopicAdmin::ofConfigs;
        private @Nullable ParsleyConfig configOverride = null;
        private Set<String> sinkTopics = Set.of();
        private List<String> sinkNodeNames = List.of();
        private @Nullable ParsleyQuiesce quiesce = null;
        private @Nullable ParsleyCoordination coordination = null;
        private Set<String> declaredTopics = Set.of();

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
        Builder<KIn, VIn, KOut, VOut> addBufferStore(String name) {
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
        Builder<KIn, VIn, KOut, VOut> addBuffer(ParsleyBuffer<KIn, VIn> buffer) {
            buffers.put(buffer.topic(), buffer);
            return this;
        }

        /**
         * Registers several causal sources at once. Equivalent to calling {@link #addBuffer} for each.
         *
         * @param buffers the source buffers; must not be {@code null}
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> addBuffers(Collection<ParsleyBuffer<KIn, VIn>> buffers) {
            for (ParsleyBuffer<KIn, VIn> buffer : buffers) {
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
        Builder<KIn, VIn, KOut, VOut> addBuffers(
                Collection<String> topics, Serde<KIn> key, Serde<VIn> value) {
            for (String topic : topics) {
                addBuffer(new ParsleyBuffer<>(topic, key, value));
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
        Builder<KIn, VIn, KOut, VOut> withConfigs(Map<String, Object> configs) {
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
        Builder<KIn, VIn, KOut, VOut> withConfig(Properties props) {
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
        Builder<KIn, VIn, KOut, VOut> withConfig(String key, Object value) {
            config.setProperty(key, String.valueOf(value));
            return this;
        }

        /**
         * Registers this processor's tasks with a {@link ParsleyQuiesce} for coordinated graceful
         * shutdown. Optional — without one, tasks process and close exactly as they do today, with no
         * quiesce tracking.
         *
         * @param quiesce the quiesce coordinator every task instance registers with
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> withQuiesce(ParsleyQuiesce quiesce) {
            this.quiesce = quiesce;
            return this;
        }

        /**
         * Registers this stage's tasks with a {@link ParsleyCoordination} to participate in topology-epoch
         * coordination. Optional — without one, the stage runs in epoch 0 (no epoch-events log, no
         * coordination thread), exactly as today.
         *
         * @param coordination the coordination handle shared across every participating stage
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> withCoordination(ParsleyCoordination coordination) {
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
         * {@link CausalTopology#assemble} sets this automatically from a stage's
         * {@code CausalProcessedStream#to(...)} declarations.
         *
         * @param topics the stage's output topic names
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> sinkTopics(Set<String> topics) {
            this.sinkTopics = Set.copyOf(topics);
            return this;
        }

        /**
         * Declares every child node this processor forwards to under the delegate's plain,
         * unaddressed {@code context.forward(record)} — this processor's business sink(s). Optional:
         * without it (the default, {@code List.of()}), a plain forward broadcasts to every actual child
         * of the processor node, exactly as Kafka Streams itself does — correct as long as every child
         * shares a compatible type. Required the moment a sibling child node with an incompatible type
         * (e.g. a raw-bytes side topic) is also wired onto this processor node, or a plain forward would
         * also broadcast to it and throw a runtime {@code ClassCastException}.
         *
         * @param names this processor's business sink node names
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> sinkNodeNames(List<String> names) {
            this.sinkNodeNames = List.copyOf(names);
            return this;
        }

        /**
         * Declares a domain topic this stage does not otherwise consume or produce, wired by the caller
         * (see {@link CausalTopology}'s domain-topics wiring) as an <strong>extra, raw byte[]/byte[]
         * source</strong> feeding this same processor node — never through a registered {@link
         * ParsleyBuffer}, since a passthrough topic's value schema is unrelated to this stage's own
         * {@code KIn}/{@code VIn} types. Folded into this member's coordination-log declaration and
         * startup topic-metadata resolution (UUID lookup, partition-count parity) alongside the registered
         * {@link ParsleyBuffer} topics, and into {@link ParsleyProcessor}'s own passthrough-record handling
         * (see its class Javadoc) so a record arriving on it is recognised and never reaches the delegate.
         * Package-private: not part of the low-level API's documented surface.
         *
         * @param topics extra topics, unioned with the registered {@link ParsleyBuffer} topics
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> declareTopics(Set<String> topics) {
            this.declaredTopics = Set.copyOf(topics);
            return this;
        }

        /**
         * Builds the {@link ParsleyProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException if no buffer store or no {@link ParsleyBuffer} was declared
         */
        ParsleyProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (storeName == null) {
                throw new IllegalStateException(
                        "a buffer store is required; call addBufferStore(name)");
            }
            if (buffers.isEmpty()) {
                throw new IllegalStateException(
                        "at least one ParsleyBuffer is required; call addBuffer(...) for every input topic");
            }
            String store = storeName;
            Map<String, ParsleyBuffer<KIn, VIn>> resolved = Map.copyOf(buffers);
            // A declared-but-not-buffered topic is a passthrough source: this stage never registered a
            // ParsleyBuffer for it (its value schema is unrelated to KIn/VIn), so it round-trips through
            // the buffer store as raw bytes — see ParsleyProcessor's passthrough-record handling.
            Set<String> passthroughTopics = new LinkedHashSet<>(declaredTopics);
            passthroughTopics.removeAll(resolved.keySet());
            Set<String> finalPassthroughTopics = Set.copyOf(passthroughTopics);
            Function<String, Serde<KIn>> keySerdeByTopic = topic -> finalPassthroughTopics.contains(topic)
                    ? byteArraySerde() : serdeFor(resolved, topic).keySerde();
            Function<String, Serde<VIn>> valueSerdeByTopic = topic -> finalPassthroughTopics.contains(topic)
                    ? byteArraySerde() : serdeFor(resolved, topic).valueSerde();
            ParsleyConfig effectiveConfig = configOverride != null ? configOverride : effectiveConfig();
            Set<String> topics = new LinkedHashSet<>(resolved.keySet());
            topics.addAll(declaredTopics);
            return new ParsleyProcessorSupplier<>(
                    userSupplier, keySerdeByTopic, valueSerdeByTopic,
                    store + "-frontier", store + "-buffer", store + "-candidate-index", store + "-forwarded-index",
                    Set.copyOf(topics), finalPassthroughTopics, sinkTopics, sinkNodeNames,
                    adminFactory, effectiveConfig, quiesce,
                    coordination);
        }

        @SuppressWarnings("unchecked") // a passthrough topic's records are raw bytes at runtime regardless of K/V
        private static <T> Serde<T> byteArraySerde() {
            return (Serde<T>) (Serde<?>) Serdes.ByteArray();
        }

        /** Classpath {@code parsley.properties} as a base layer, overlaid with builder-supplied keys. */
        private ParsleyConfig effectiveConfig() {
            Properties props = ParsleyConfig.loadProperties();
            props.putAll(config);
            return ParsleyConfig.from(props);
        }

        private static <KIn, VIn> ParsleyBuffer<KIn, VIn> serdeFor(
                Map<String, ParsleyBuffer<KIn, VIn>> buffers, String topic) {
            ParsleyBuffer<KIn, VIn> buffer = buffers.get(topic);
            if (buffer == null) {
                throw new IllegalStateException("no ParsleyBuffer registered for topic '" + topic + "'");
            }
            return buffer;
        }
    }
}
