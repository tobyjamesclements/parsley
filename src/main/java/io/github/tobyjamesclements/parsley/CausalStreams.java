package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Builds a {@link Topology} around a single causal stage: one or more causal source topics feeding
 * a {@link CausalProcessors}-decorated processor, forwarding to one or more sink topics.
 *
 * <p>This is the high-level, topology-owning entry point — it plays the same role as
 * {@link org.apache.kafka.streams.StreamsBuilder}, but for a causal stage. It composes the
 * low-level {@link CausalProcessors} decorator internally rather than reimplementing the causal
 * engine; use {@link CausalProcessors} directly when you already own a multi-stage topology and
 * only need the decorator for one {@code process(...)} call. Reach for {@code CausalStreams}
 * instead whenever a stage needs a guarantee the decorator alone cannot provide, because they
 * require owning the sinks: a uniform sink partitioner ({@link Builder#withPartitioner}),
 * co-partitioning validation that also covers sink topics (not just inputs), and a sink
 * {@code cleanup.policy} check — both described on {@code parsley.topology.validation}.
 *
 * <pre>{@code
 * CausalQuiesce quiesce = CausalQuiesce.create();
 *
 * Topology topology = CausalStreams.builder(userSupplier)
 *         .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
 *         .addSource(CausalBuffer.of("prices", Serdes.String(), priceSerde))
 *         .addSource(CausalBuffer.of("orders", Serdes.String(), orderSerde))
 *         .addSink("enriched-sink", "enriched-output", Serdes.String(), enrichedSerde)
 *         .withPartitioner(tenantPrefixPartitioner)
 *         .withQuiesce(quiesce)
 *         .build();
 *
 * KafkaStreams streams = new KafkaStreams(topology, props);
 * streams.start();
 *
 * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
 *     quiesce.requestQuiesce();
 *     while (!quiesce.isSafeToClose()) {
 *         Thread.sleep(100);
 *     }
 *     streams.close();
 * }));
 * }</pre>
 *
 * <p>See {@link CausalProcessorSupplier} for the causal guarantee and its preconditions — they
 * apply unchanged here, restated in terms of what this builder owns:
 *
 * <ul>
 *   <li><strong>Your key is your shard.</strong> Every source and sink this stage declares shares
 *       one partitioner ({@link Builder#withPartitioner}, default Kafka's own key-hash partitioner)
 *       so a shard never drifts onto different partitions across topics. That partitioner must
 *       read only the key, never the value — a protocol watermark carries no value.
 *   <li><strong>Path integrity holds by construction.</strong> A stage this builder produces is
 *       exactly sources → one causal-decorated processor → sinks; there is no method that inserts
 *       a plain, non-Parsley node in between, so a hop that would silently drop the
 *       causal-dependencies header or swallow a non-emitting invocation without a watermark cannot
 *       be constructed through this API.
 * </ul>
 */
public final class CausalStreams {

    private CausalStreams() {}

    /**
     * Starts building a {@link Topology} around a causal stage that wraps {@code userSupplier}.
     *
     * @param userSupplier the user's processor supplier (its declared state stores are unioned with
     *                     Parsley's internal frontier and buffer stores)
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a {@link Builder} for the causal stage's {@code Topology}
     */
    public static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
        return new Builder<>(userSupplier);
    }

    /**
     * Builder for a single causal stage's {@link Topology}. A buffer store, at least one
     * {@link CausalBuffer} source, and at least one sink are required.
     *
     * @param <KIn>  the input key type
     * @param <VIn>  the input value type
     * @param <KOut> the forwarded key type
     * @param <VOut> the forwarded value type
     */
    public static final class Builder<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
        private @Nullable String storeName = null;
        private @Nullable CausalBufferLimit limit = null;
        private final Map<String, CausalBuffer<KIn, VIn>> sources = new LinkedHashMap<>();
        private final List<Sink<KOut, VOut>> sinks = new ArrayList<>();
        private final Properties config = new Properties();
        private CausalAudit audit = CausalAudit.NOOP;
        private @Nullable ParsleyTopicAdmin topicAdmin = null;
        private @Nullable StreamPartitioner<? super KOut, ? super VOut> partitioner = null;
        private @Nullable CausalQuiesce quiesce = null;

        private Builder(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
            this.userSupplier = userSupplier;
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
            this.topicAdmin = topicAdmin;
            return this;
        }

        /**
         * Declares the buffer state store and its eviction limit. See
         * {@link CausalProcessors.Builder#addBufferStore} — same semantics.
         *
         * @param name  the state-store namespace; also the base for this stage's generated topology
         *              node names ({@code name + "-processor"}, {@code name + "-source-" + topic})
         * @param limit the buffer eviction trigger
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addBufferStore(String name, CausalBufferLimit limit) {
            this.storeName = name;
            this.limit = limit;
            return this;
        }

        /**
         * Registers a causal source topic and adds its {@code Topology} source node. Required for
         * every input topic this stage will see.
         *
         * @param buffer the source topic and its serdes; must not be {@code null}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addSource(CausalBuffer<KIn, VIn> buffer) {
            sources.put(buffer.topic(), buffer);
            return this;
        }

        /**
         * Registers a causal sink topic and adds its {@code Topology} sink node, connected to this
         * stage's processor node.
         *
         * @param name       the sink's topology node name — the delegate may {@code forward(record,
         *                   name)} to target this sink specifically when the stage has more than one
         * @param topic      the sink topic name
         * @param keySerde   the key serde to serialise forwarded keys with
         * @param valueSerde the value serde to serialise forwarded values with
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> addSink(
                String name, String topic, Serde<KOut> keySerde, Serde<VOut> valueSerde) {
            sinks.add(new Sink<>(name, topic, keySerde, valueSerde));
            return this;
        }

        /**
         * Sets the {@link StreamPartitioner} applied to <strong>every</strong> sink this stage
         * declares (default: Kafka's own default key-hash partitioner) — never configurable
         * per-sink, so two causal sinks in the same stage can never drift onto different
         * partitioners. A watermark carries a null value and reuses its triggering record's key, so
         * {@code partitioner} must read only the key (never the value) — a coarser-than-key shard
         * function (e.g. a composite-key prefix) is the intended use, not a value-based one, which
         * cannot route a null-value watermark.
         *
         * @param partitioner the partitioner to apply uniformly to every sink; must read only the key
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withPartitioner(
                StreamPartitioner<? super KOut, ? super VOut> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * Sets Parsley's own configuration from a map of key/value pairs. See
         * {@link CausalProcessors.Builder#withConfigs} — same semantics.
         *
         * @param configs the configuration entries; values are recorded as their string form
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withConfigs(Map<String, Object> configs) {
            configs.forEach((key, value) -> config.setProperty(key, String.valueOf(value)));
            return this;
        }

        /**
         * Sets a single Parsley configuration entry. See {@link CausalProcessors.Builder#withConfig}
         * — same semantics.
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
         * Registers a {@link CausalAudit} to receive this stage's per-record causal events. See
         * {@link CausalProcessors.Builder#withAudit} — same semantics.
         *
         * @param audit the audit to notify; must not be {@code null}
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withAudit(CausalAudit audit) {
            this.audit = audit;
            return this;
        }

        /**
         * Registers this stage's tasks with a {@link CausalQuiesce} for coordinated graceful
         * shutdown. See {@link CausalProcessors.Builder#withQuiesce} — same semantics.
         *
         * @param quiesce the quiesce coordinator every task instance registers with
         * @return this builder
         */
        public Builder<KIn, VIn, KOut, VOut> withQuiesce(CausalQuiesce quiesce) {
            this.quiesce = quiesce;
            return this;
        }

        /**
         * Builds the {@link Topology}: one source node per registered {@link CausalBuffer}, one
         * processor node running {@code userSupplier} behind the causal guarantee (composing
         * {@link CausalProcessors} internally), and one sink node per registered sink — all wired
         * together as a single causal stage.
         *
         * @return the assembled {@code Topology}, ready for {@code new KafkaStreams(topology, props)}
         * @throws IllegalStateException if no buffer store, no source, or no sink was declared
         */
        public Topology build() {
            if (storeName == null || limit == null) {
                throw new IllegalStateException(
                        "a buffer store is required; call addBufferStore(name, limit)");
            }
            if (sources.isEmpty()) {
                throw new IllegalStateException(
                        "at least one source is required; call addSource(...) for every input topic");
            }
            if (sinks.isEmpty()) {
                throw new IllegalStateException(
                        "at least one sink is required; call addSink(...) for every output topic");
            }

            Set<String> sinkTopics = new LinkedHashSet<>();
            for (Sink<KOut, VOut> sink : sinks) {
                sinkTopics.add(sink.topic());
            }
            CausalProcessors.Builder<KIn, VIn, KOut, VOut> causalBuilder = CausalProcessors.builder(userSupplier)
                    .addBufferStore(storeName, limit)
                    .addBuffers(sources.values())
                    .withConfig(config)
                    .withAudit(audit)
                    .additionalPartitionCountTopics(sinkTopics);
            if (quiesce != null) {
                causalBuilder.withQuiesce(quiesce);
            }
            if (topicAdmin != null) {
                causalBuilder.topicAdmin(topicAdmin);
            }
            CausalProcessorSupplier<KIn, VIn, KOut, VOut> causalSupplier = causalBuilder.build();

            String processorName = storeName + "-processor";
            Topology topology = new Topology();
            String[] sourceNames = new String[sources.size()];
            int i = 0;
            for (CausalBuffer<KIn, VIn> buffer : sources.values()) {
                String sourceName = storeName + "-source-" + buffer.topic();
                topology.addSource(sourceName,
                        buffer.keySerde().deserializer(), buffer.valueSerde().deserializer(), buffer.topic());
                sourceNames[i++] = sourceName;
            }
            topology.addProcessor(processorName, causalSupplier, sourceNames);
            for (Sink<KOut, VOut> sink : sinks) {
                // partitioner may be null here — Topology.addSink's partitioner-accepting overload
                // treats that identically to the no-partitioner overload (falls back to the default
                // key-hash partitioner), so one call covers both cases.
                topology.addSink(sink.name(), sink.topic(),
                        sink.keySerde().serializer(), sink.valueSerde().serializer(), partitioner, processorName);
            }
            return topology;
        }

        private record Sink<K, V>(String name, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        }
    }
}
