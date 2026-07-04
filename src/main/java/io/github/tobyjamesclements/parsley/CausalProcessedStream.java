package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.jspecify.annotations.Nullable;

/**
 * A causal stage bound to its processor, ready to declare sink(s) with {@link #to}. Returned by
 * {@link CausalStream#process}.
 *
 * @param <K> the forwarded key type
 * @param <V> the forwarded value type
 */
public final class CausalProcessedStream<K, V> {

    private final StageSpec<?, ?, K, V> stage;

    CausalProcessedStream(StageSpec<?, ?, K, V> stage) {
        this.stage = stage;
    }

    /**
     * Declares a sink topic for this stage, deferring its key/value serdes to the runtime's default
     * serdes ({@code default.key.serde}/{@code default.value.serde}). The sink's topology node name
     * defaults to {@code topic} — use {@link #to(String, String)} to name it explicitly, required when
     * the stage forwards to more than one sink and the delegate targets one by name.
     *
     * @param topic the sink topic name
     * @return this, for declaring further sinks or {@link #withPartitioner}/{@link #withAudit}
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
     * @return this, for declaring further sinks or {@link #withPartitioner}/{@link #withAudit}
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
     * @return this, for declaring further sinks or {@link #withPartitioner}/{@link #withAudit}
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
     * @return this, for declaring further sinks or {@link #withPartitioner}/{@link #withAudit}
     */
    public CausalProcessedStream<K, V> to(
            String name, String topic, @Nullable Serde<K> keySerde, @Nullable Serde<V> valueSerde) {
        stage.sinks.add(new StageSpec.SinkSpec<>(name, topic, keySerde, valueSerde));
        return this;
    }

    /**
     * Sets the {@link StreamPartitioner} applied to <strong>every</strong> sink this stage declares
     * (default: Kafka's own key-hash partitioner) — never configurable per-sink, so two sinks in the
     * same stage can never drift onto different partitioners. A watermark carries a null value and
     * reuses its triggering record's key, so {@code partitioner} must read only the key (never the
     * value) — a coarser-than-key shard function is the intended use, not a value-based one, which
     * cannot route a null-value watermark.
     *
     * @param partitioner the partitioner to apply uniformly to every sink; must read only the key
     * @return this
     */
    public CausalProcessedStream<K, V> withPartitioner(StreamPartitioner<? super K, ? super V> partitioner) {
        stage.partitioner = partitioner;
        return this;
    }

    /**
     * Registers a {@link CausalAudit} to receive this stage's per-record causal events, for routing to
     * your own audit/compliance trail. Optional — without one, these events are observable only through
     * Parsley's logs and metrics.
     *
     * @param audit the audit to notify; must not be {@code null}
     * @return this
     */
    public CausalProcessedStream<K, V> withAudit(CausalAudit audit) {
        stage.audit = audit;
        return this;
    }
}
