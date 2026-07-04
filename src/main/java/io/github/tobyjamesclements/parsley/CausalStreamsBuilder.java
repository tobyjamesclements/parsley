package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@link CausalTopology}: one or more causal stages, each a set of source topics feeding a
 * causal-decorated processor and forwarding to one or more sinks. Plays the same role
 * {@link org.apache.kafka.streams.StreamsBuilder} plays for a plain Kafka Streams topology.
 *
 * <pre>{@code
 * CausalStreamsBuilder builder = new CausalStreamsBuilder();
 *
 * builder.stream(List.of("orders", "payments"), Serdes.String(), eventSerde)
 *        .process(new EnrichmentProcessorSupplier())
 *        .to("orders.enriched", Serdes.String(), enrichedSerde);
 *
 * CausalTopology topology = builder.build();
 *
 * CausalStreams causalStreams = new CausalStreams(topology, props);
 * causalStreams.start();
 * Runtime.getRuntime().addShutdownHook(new Thread(causalStreams::close));
 * }</pre>
 *
 * <p>Multiple input topics are the norm — {@link #stream(Collection, Serde, Serde)} fans several
 * co-partitioned topics sharing one serde pair into a single stage; combine streams declared with
 * different serdes with {@link CausalStream#merge}. A stage's key/value serdes may be omitted
 * ({@link #stream(String)}, {@link CausalProcessedStream#to(String)}), falling back to the runtime's
 * {@code default.key.serde}/{@code default.value.serde} — the same convention
 * {@link org.apache.kafka.streams.kstream.KStream} uses.
 *
 * <p>Unlike the Kafka Streams DSL, sources and sinks here take plain key/value serdes rather than
 * {@code Consumed}/{@code Produced}: neither exposes its serdes for reading back, and Parsley's causal
 * buffer needs the real {@link Serde} to round-trip a held record across a restart.
 *
 * <p>See {@link CausalProcessorSupplier} for the causal guarantee and its preconditions — they apply
 * unchanged here. Every source and sink a stage declares shares one {@code StreamPartitioner}
 * ({@link CausalProcessedStream#withPartitioner}, default Kafka's own key-hash partitioner), so a shard
 * never drifts onto different partitions across topics.
 */
public final class CausalStreamsBuilder {

    private final List<StageSpec<?, ?, ?, ?>> stages = new ArrayList<>();
    private @Nullable ParsleyTopicAdmin topicAdminOverride = null;

    /**
     * Registers a causal source topic, deferring its key/value serdes to the runtime's default serdes.
     *
     * @param topic the source topic name
     * @param <K>   the key type
     * @param <V>   the value type
     * @return a {@link CausalStream} over this one topic
     */
    public <K, V> CausalStream<K, V> stream(String topic) {
        return stream(List.of(topic), null, null);
    }

    /**
     * As {@link #stream(String)}, with explicit key/value serdes.
     *
     * @param topic      the source topic name
     * @param keySerde   the key serde the buffer (de)serialises held keys with
     * @param valueSerde the value serde the buffer (de)serialises held values with
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a {@link CausalStream} over this one topic
     */
    public <K, V> CausalStream<K, V> stream(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return stream(List.of(topic), keySerde, valueSerde);
    }

    /**
     * Registers several source topics that share one serde pair, fanning them into one stage — the
     * common case, since multiple input topics are the norm for a causal stage. Deferring their
     * key/value serdes to the runtime's default serdes.
     *
     * @param topics the source topic names
     * @param <K>    the key type
     * @param <V>    the value type
     * @return a {@link CausalStream} over every listed topic
     */
    public <K, V> CausalStream<K, V> stream(Collection<String> topics) {
        return stream(topics, null, null);
    }

    /**
     * As {@link #stream(Collection)}, with explicit key/value serdes shared by every listed topic.
     *
     * @param topics     the source topic names
     * @param keySerde   the key serde the buffer (de)serialises held keys with, or {@code null} to defer
     *                   to the runtime's default key serde
     * @param valueSerde the value serde the buffer (de)serialises held values with, or {@code null} to
     *                   defer to the runtime's default value serde
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a {@link CausalStream} over every listed topic
     */
    public <K, V> CausalStream<K, V> stream(
            Collection<String> topics, @Nullable Serde<K> keySerde, @Nullable Serde<V> valueSerde) {
        Map<String, StageSpec.SourceSpec<K, V>> sources = new LinkedHashMap<>();
        for (String topic : topics) {
            sources.put(topic, new StageSpec.SourceSpec<>(keySerde, valueSerde));
        }
        return new CausalStream<>(this, sources);
    }

    /**
     * Overrides the {@link ParsleyTopicAdmin} used to resolve topic UUIDs at startup (default: a live
     * {@link org.apache.kafka.clients.admin.Admin} built from the task's {@code appConfigs()}), for every
     * stage this builder declares. For tests running under {@code TopologyTestDriver} with no broker.
     *
     * @param topicAdmin the admin to resolve UUIDs with
     * @return this builder
     */
    CausalStreamsBuilder topicAdmin(ParsleyTopicAdmin topicAdmin) {
        this.topicAdminOverride = topicAdmin;
        return this;
    }

    <KIn, VIn, KOut, VOut> CausalProcessedStream<KOut, VOut> addStage(StageSpec<KIn, VIn, KOut, VOut> stage) {
        stages.add(stage);
        return new CausalProcessedStream<>(stage);
    }

    /**
     * Builds the {@link CausalTopology}: every stage this builder declared, ready to be assembled into a
     * real Kafka Streams {@code Topology} once a {@link CausalStreams} runtime supplies its {@code props}.
     *
     * @return the assembled {@code CausalTopology}
     * @throws IllegalStateException if a declared stage has no source or no sink
     */
    public CausalTopology build() {
        for (StageSpec<?, ?, ?, ?> stage : stages) {
            if (stage.sources.isEmpty()) {
                throw new IllegalStateException(
                        "at least one source is required; call stream(...) for every input topic");
            }
            if (stage.sinks.isEmpty()) {
                throw new IllegalStateException(
                        "at least one sink is required; call to(...) for every output topic");
            }
        }
        return new CausalTopology(List.copyOf(stages), topicAdminOverride);
    }
}
