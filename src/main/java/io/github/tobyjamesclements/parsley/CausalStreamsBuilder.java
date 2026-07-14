package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@link CausalTopology}: <strong>exactly one</strong> causal stage — a set of source topics
 * feeding a causal-decorated processor and forwarding to one or more sinks. Plays the same role
 * {@link org.apache.kafka.streams.StreamsBuilder} plays for a plain Kafka Streams topology.
 *
 * <pre>{@code
 * CausalTopology topology = new CausalStreamsBuilder()
 *         .stream(List.of("orders", "payments"), Serdes.String(), eventSerde)
 *         .process(new EnrichmentProcessorSupplier())
 *         .to("orders.enriched", Serdes.String(), enrichedSerde)
 *         .build();
 *
 * CausalStreams causalStreams = new CausalStreams(topology, props);
 * causalStreams.start();
 * Runtime.getRuntime().addShutdownHook(new Thread(causalStreams::close));
 * }</pre>
 *
 * <p><strong>One stage per topology.</strong> A causal application is a single causal node: it consumes
 * its declared input topics and produces its declared output topics, with no second causal-gated hop in
 * between. The public fluent chain terminates in {@link CausalProcessedStream#build()} — there is no
 * public {@code build()} on the builder and no term to open a second stage, so a topology is exactly one
 * {@code stream(...).process(...).to(...)} chain. A multi-stage causal pipeline is built as multiple
 * applications, each one stage, sharing a coordination log.
 *
 * <p>Multiple input topics are the norm — {@link #stream(Collection, Serde, Serde)} fans several
 * co-partitioned topics sharing one serde pair into a single stage; combine streams declared with
 * different serdes with {@link CausalStream#merge}. Multiple sinks are declared by chaining
 * {@link CausalProcessedStream#to(String) to(...)}. A stage's key/value serdes may be omitted
 * ({@link #stream(String)}, {@link CausalProcessedStream#to(String)}), falling back to the runtime's
 * {@code default.key.serde}/{@code default.value.serde} — the same convention
 * {@link org.apache.kafka.streams.kstream.KStream} uses.
 *
 * <p>Unlike the Kafka Streams DSL, sources and sinks here take plain key/value serdes rather than
 * {@code Consumed}/{@code Produced}: neither exposes its serdes for reading back, and Parsley's causal
 * buffer needs the real {@link Serde} to round-trip a held record across a restart.
 *
 * <p>See {@link ParsleyProcessor} for the causal guarantee and its preconditions — they apply
 * unchanged here. Every source and sink a stage declares shares one {@code StreamPartitioner}
 * ({@link CausalProcessedStream#withPartitioner}, default Kafka's own key-hash partitioner), so a shard
 * never drifts onto different partitions across topics.
 */
public final class CausalStreamsBuilder {

    private @Nullable ParsleyTopicAdmin topicAdminOverride = null;
    // The single causal stage. Set once, by CausalStream#process (via registerStage); a second stage is
    // rejected, so a topology is always exactly one stage. Held here so build() can freeze and assemble it;
    // the CausalProcessedStream returned by process() holds the same spec and mutates it via to(...).
    private @Nullable ParsleyStageSpec<?, ?, ?, ?> stage = null;

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
        Map<String, ParsleyStageSpec.SourceSpec<K, V>> sources = new LinkedHashMap<>();
        for (String topic : topics) {
            sources.put(topic, new ParsleyStageSpec.SourceSpec<>(keySerde, valueSerde));
        }
        return new CausalStream<>(this, sources);
    }

    /**
     * Overrides the {@link ParsleyTopicAdmin} used to resolve topic UUIDs at startup (default: a live
     * {@link org.apache.kafka.clients.admin.Admin} built from the task's {@code appConfigs()}), for the
     * stage this builder declares. For tests running under {@code TopologyTestDriver} with no broker. Set
     * it before {@link CausalProcessedStream#build()}, which reads it back off the builder.
     *
     * @param topicAdmin the admin to resolve UUIDs with
     * @return this builder
     */
    CausalStreamsBuilder topicAdmin(ParsleyTopicAdmin topicAdmin) {
        this.topicAdminOverride = topicAdmin;
        return this;
    }

    /**
     * Records this builder's one causal stage — called by {@link CausalStream#process}. A second stage is
     * rejected: a causal topology is exactly one stage (split extra stages into separate applications).
     */
    void registerStage(ParsleyStageSpec<?, ?, ?, ?> spec) {
        if (this.stage != null) {
            throw new IllegalStateException(
                    "a causal topology declares exactly one stage; a second process(...) is not allowed — "
                            + "split extra stages into separate applications sharing a coordination log");
        }
        this.stage = spec;
    }

    /**
     * Freezes and returns the single-stage {@link CausalTopology}. Not public: the public terminal is
     * {@link CausalProcessedStream#build()}, which delegates here — so a topology can only ever be one
     * {@code stream(...).process(...).to(...)} chain, never a builder accumulating stages.
     *
     * @throws IllegalStateException if no stage was declared, or the stage has no source or no sink
     */
    CausalTopology build() {
        ParsleyStageSpec<?, ?, ?, ?> declared = stage;
        if (declared == null) {
            throw new IllegalStateException(
                    "no causal stage declared; call stream(...).process(...).to(...) before build()");
        }
        if (declared.sources.isEmpty()) {
            throw new IllegalStateException(
                    "at least one source is required; call stream(...) for every input topic");
        }
        if (declared.sinks.isEmpty()) {
            throw new IllegalStateException(
                    "at least one sink is required; call to(...) for every output topic");
        }
        declared.freeze();
        return new CausalTopology(List.of(declared), topicAdminOverride);
    }
}
