package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link ProcessorSupplier} the causal decorator {@link CausalTopology} wires around each stage's
 * user supplier when it assembles the real Kafka Streams topology: it wraps the user's supplier in a
 * {@link ParsleyProcessor} and {@linkplain #stores() unions} the user's declared state stores with
 * Parsley's internal frontier and buffer stores, so the DSL wires all of them to the same processor
 * node. The user never names Parsley's internal stores.
 *
 * <p>Not constructed directly: obtain one through {@link #builder(ProcessorSupplier)}. User code
 * declares stages through {@link CausalStreamsBuilder}, whose {@code assemble} pass drives that builder
 * (buffer store namespace, per-topic serdes, sink topics and node names, quiesce wiring);
 * package-private tests also drive it directly to exercise the decorator without the topology layer.
 *
 * <p><strong>Data-loss precondition (low-level use).</strong> The causal-broadcast core's skip-bridge
 * ({@link ParsleyChannels#bridge}) treats an offset the consumer never returned as a transaction marker,
 * which is only sound if the consumer can never silently jump forward over lost records. The high-level
 * path ({@link CausalStreamsBuilder} → {@link CausalTopology} → {@link CausalStreams}) guarantees this by
 * declaring every source with {@code AutoOffsetReset.none()} and pre-seeding first-start offsets (see
 * {@link ParsleyOffsetSeeder}). A topology assembled directly from this supplier — bypassing {@code
 * CausalStreams} — must apply the same {@code none()} reset and offset seeding to its causal sources, or a
 * retention/{@code deleteRecords} jump past a lagging consumer will be folded as markers and deliver
 * records before their causes. Only exception handlers are the concern that {@link CausalTopology#assemble}
 * rejects centrally; this reset/seeding requirement is not enforceable from inside the processor.
 */
final class ParsleyProcessorSupplier<KIn, VIn, KOut, VOut>
        implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    private final Function<String, Serde<KIn>> keySerdeByTopic;
    private final Function<String, Serde<VIn>> valueSerdeByTopic;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final Set<String> topics;
    private final Set<String> sinkTopics;
    private final List<String> sinkNodeNames;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final @Nullable ParsleyQuiesce quiesce;

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName,
                                      String candidateIndexStoreName,
                                      String forwardedIndexStoreName,
                                      Set<String> topics,
                                      Set<String> sinkTopics,
                                      List<String> sinkNodeNames,
                                      Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                                      ParsleyConfig config,
                                      @Nullable ParsleyQuiesce quiesce) {
        this.userSupplier = userSupplier;
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.topics = topics;
        this.sinkTopics = sinkTopics;
        this.sinkNodeNames = sinkNodeNames;
        this.adminFactory = adminFactory;
        this.config = config;
        this.quiesce = quiesce;
    }

    /**
     * Starts building a {@code ParsleyProcessorSupplier} that wraps {@code userSupplier} behind the
     * causal guarantee. Declare the buffer store with {@link Builder#addBufferStore(String)} and at
     * least one {@link ParsleySource} before {@link Builder#build()}.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for a {@code ParsleyProcessorSupplier}
     * @throws IllegalArgumentException if {@code userSupplier} is already a
     *         {@code ParsleyProcessorSupplier} — decorating an already-decorated supplier would
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

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new ParsleyProcessor<>(
                userSupplier.get(),
                new ParsleySerializer<>(new ParsleyResolver<>(keySerdeByTopic, valueSerdeByTopic)),
                frontierStoreName, bufferStoreName, candidateIndexStoreName, forwardedIndexStoreName,
                topics, sinkTopics, sinkNodeNames,
                adminFactory, config, quiesce);
    }

    /** The effective Parsley configuration this supplier was built with. Package-private for tests. */
    ParsleyConfig config() {
        return config;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        Set<StoreBuilder<?>> stores = new HashSet<>();
        Set<StoreBuilder<?>> userStores = userSupplier.stores();
        if (userStores != null) {
            stores.addAll(userStores);
        }
        stores.add(ParsleyStores.frontierStore(frontierStoreName));
        stores.add(ParsleyStores.bufferStore(bufferStoreName));
        stores.add(ParsleyStores.candidateIndexStore(candidateIndexStoreName));
        stores.add(ParsleyStores.forwardedIndexStore(forwardedIndexStoreName));
        return stores;
    }

    /**
     * Builder for a {@link ParsleyProcessorSupplier}. A buffer store
     * (via {@link #addBufferStore(String)}) and at least one {@link ParsleySource}
     * (via {@link #addSource}/{@link #addSources}) are required; Parsley's own configuration is
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
        private final Map<String, ParsleySource<KIn, VIn>> sources = new LinkedHashMap<>();
        private final Properties config = new Properties();
        private Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory = ParsleyTopicAdmin::ofConfigs;
        private @Nullable ParsleyConfig configOverride = null;
        private Set<String> sinkTopics = Set.of();
        private List<String> sinkNodeNames = List.of();
        private @Nullable ParsleyQuiesce quiesce = null;

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
         * @param source the source topic and its serdes; must not be {@code null}
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> addSource(ParsleySource<KIn, VIn> source) {
            sources.put(source.topic(), source);
            return this;
        }

        /**
         * Registers several causal sources at once. Equivalent to calling {@link #addSource} for each.
         *
         * @param sources the causal sources; must not be {@code null}
         * @return this builder
         */
        Builder<KIn, VIn, KOut, VOut> addSources(Collection<ParsleySource<KIn, VIn>> sources) {
            for (ParsleySource<KIn, VIn> source : sources) {
                addSource(source);
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
        Builder<KIn, VIn, KOut, VOut> addSources(
                Collection<String> topics, Serde<KIn> key, Serde<VIn> value) {
            for (String topic : topics) {
                addSource(new ParsleySource<>(topic, key, value));
            }
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
         * Declares the topics this stage produces. Each declared sink's UUID and end offsets are
         * resolved strictly at init — like a registered input source, a causal sink must exist
         * before the stage starts (they feed own-output stamping, load-bearing for causal order);
         * an unresolvable sink fails init. Sink partition counts and cleanup policies are also
         * folded into the startup topology lints ({@code parsley.topology.validation}), which stay
         * best-effort per topic: a describe failure there skips that lint rather than failing the
         * task, since lint strictness is the validation mode's call.
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
         * Builds the {@link ParsleyProcessorSupplier}.
         *
         * @return a decorated supplier ready for {@code stream(...).process(...)}
         * @throws IllegalStateException if no buffer store or no {@link ParsleySource} was declared
         */
        ParsleyProcessorSupplier<KIn, VIn, KOut, VOut> build() {
            if (storeName == null) {
                throw new IllegalStateException(
                        "a buffer store is required; call addBufferStore(name)");
            }
            if (sources.isEmpty()) {
                throw new IllegalStateException(
                        "at least one ParsleySource is required; call addSource(...) for every input topic");
            }
            String store = storeName;
            Map<String, ParsleySource<KIn, VIn>> resolved = Map.copyOf(sources);
            Function<String, Serde<KIn>> keySerdeByTopic = topic -> serdeFor(resolved, topic).keySerde();
            Function<String, Serde<VIn>> valueSerdeByTopic = topic -> serdeFor(resolved, topic).valueSerde();
            ParsleyConfig effectiveConfig = configOverride != null ? configOverride : effectiveConfig();
            return new ParsleyProcessorSupplier<>(
                    userSupplier, keySerdeByTopic, valueSerdeByTopic,
                    store + "-frontier", store + "-buffer", store + "-candidate-index", store + "-forwarded-index",
                    Set.copyOf(resolved.keySet()), sinkTopics, sinkNodeNames,
                    adminFactory, effectiveConfig, quiesce);
        }

        /** Classpath {@code parsley.properties} as a base layer, overlaid with builder-supplied keys. */
        private ParsleyConfig effectiveConfig() {
            Properties props = ParsleyConfig.loadProperties();
            props.putAll(config);
            return ParsleyConfig.from(props);
        }

        private static <KIn, VIn> ParsleySource<KIn, VIn> serdeFor(
                Map<String, ParsleySource<KIn, VIn>> sources, String topic) {
            ParsleySource<KIn, VIn> source = sources.get(topic);
            if (source == null) {
                throw new IllegalStateException("no ParsleySource registered for topic '" + topic + "'");
            }
            return source;
        }
    }
}
