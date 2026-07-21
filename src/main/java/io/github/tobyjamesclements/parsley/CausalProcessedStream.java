package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.jspecify.annotations.Nullable;

/**
 * A causal stage bound to its processor, ready to declare sink(s) with {@link #to} and then terminate the
 * topology with {@link #build()}. Returned by {@link CausalStream#process}.
 *
 * <p>{@code build()} lives here, not on {@link CausalStreamsBuilder}, so that a causal topology is exactly
 * one {@code stream(...).process(...).to(...)} chain: there is no term to open a second stage (see
 * {@link CausalStreamsBuilder}'s "one stage per topology" note).
 *
 * @param <K> the forwarded key type
 * @param <V> the forwarded value type
 */
public final class CausalProcessedStream<K, V> {

    private final CausalStreamsBuilder owner;
    private final ParsleyStageSpec<?, ?, K, V> stage;

    CausalProcessedStream(CausalStreamsBuilder owner, ParsleyStageSpec<?, ?, K, V> stage) {
        this.owner = owner;
        this.stage = stage;
    }

    /**
     * Declares a sink topic for this stage, deferring its key/value serdes to the runtime's default
     * serdes ({@code default.key.serde}/{@code default.value.serde}). The sink's topology node name
     * defaults to {@code topic} — use {@link #to(String, String)} to name it explicitly, required when
     * the stage forwards to more than one sink and the delegate targets one by name.
     *
     * @param topic the sink topic name
     * @return this, for declaring further sinks or {@link #withPartitioner}
     */
    public CausalProcessedStream<K, V> to(String topic) {
        return to(topic, topic, null, null);
    }

    /**
     * As {@link #to(String)}, with explicit key/value serdes.
     *
     * @param topic      the sink topic name
     * @param keySerde   the key serde to serialise forwarded keys with
     * @param valueSerde the value serde to serialise forwarded values with
     * @return this, for declaring further sinks or {@link #withPartitioner}
     */
    public CausalProcessedStream<K, V> to(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return to(topic, topic, keySerde, valueSerde);
    }

    /**
     * As {@link #to(String)}, naming the sink's topology node — the delegate may {@code forward(record,
     * name)} to target this sink specifically when the stage has more than one.
     *
     * @param name  the sink's topology node name
     * @param topic the sink topic name
     * @return this, for declaring further sinks or {@link #withPartitioner}
     */
    public CausalProcessedStream<K, V> to(String name, String topic) {
        return to(name, topic, null, null);
    }

    /**
     * As {@link #to(String, String)}, with explicit key/value serdes.
     *
     * @param name       the sink's topology node name
     * @param topic      the sink topic name
     * @param keySerde   the key serde to serialise forwarded keys with
     * @param valueSerde the value serde to serialise forwarded values with
     * @return this, for declaring further sinks or {@link #withPartitioner}
     */
    public CausalProcessedStream<K, V> to(
            String name, String topic, @Nullable Serde<K> keySerde, @Nullable Serde<V> valueSerde) {
        stage.addSink(new ParsleyStageSpec.SinkSpec<>(name, topic, keySerde, valueSerde));
        return this;
    }

    /**
     * Sets the {@link StreamPartitioner} applied to <strong>every</strong> sink this stage declares
     * (default: Kafka's own key-hash partitioner) — never configurable per-sink, so two sinks in the
     * same stage can never drift onto different partitioners. {@code partitioner} must compute the
     * partition from the key alone (never the value): the key is the only field causally related
     * records share across topics, so a coarser-than-key shard function of the key is the intended
     * use. Protocol null messages never reach this partitioner — {@link CausalTopology} wraps it in
     * a decorator that routes each null message to the forwarding task's own partition.
     *
     * @param partitioner the partitioner to apply uniformly to every sink; must read only the key
     * @return this
     */
    public CausalProcessedStream<K, V> withPartitioner(StreamPartitioner<? super K, ? super V> partitioner) {
        stage.partitioner(partitioner);
        return this;
    }

    /**
     * Freezes this stage and builds the single-stage {@link CausalTopology}, ready to be assembled into a
     * real Kafka Streams {@code Topology} once a {@link CausalStreams} runtime supplies its {@code props}.
     * The stage is frozen, so this handle can no longer add sinks or change the partitioner — the built
     * topology is genuinely immutable. This is the only <em>public</em> way to produce a
     * {@link CausalTopology}, which is why a causal topology is always exactly one stage. Delegates to the
     * builder that vended this stage.
     *
     * @return the assembled single-stage {@code CausalTopology}
     * @throws IllegalStateException if this stage has no source or no sink
     */
    public CausalTopology build() {
        return owner.build();
    }

    /**
     * As {@link #build()}, overriding the {@link ParsleyTopicAdmin} the topology resolves topic UUIDs with —
     * a test seam for {@code TopologyTestDriver} runs with no broker (see
     * {@link CausalStreamsBuilder#topicAdmin}).
     */
    CausalTopology build(ParsleyTopicAdmin admin) {
        owner.topicAdmin(admin);
        return owner.build();
    }
}
