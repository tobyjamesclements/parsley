package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One stage of a {@link CausalTopology}: the source topics feeding a causal-decorated processor, and the
 * sink(s) it forwards to. Mutable while its owning {@link CausalStreamsBuilder} is being declared;
 * {@link CausalStreamsBuilder#build()} snapshots the builder's stage list, and
 * {@link CausalTopology#assemble} reads each stage exactly once to wire the real Kafka Streams topology.
 *
 * @param <KIn>  the input key type
 * @param <VIn>  the input value type
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class ParsleyStageSpec<KIn, VIn, KOut, VOut> {

    final @Nullable String explicitName;
    final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    final Map<String, SourceSpec<KIn, VIn>> sources;
    final List<SinkSpec<KOut, VOut>> sinks = new ArrayList<>();
    @Nullable StreamPartitioner<? super KOut, ? super VOut> partitioner;
    // Set by CausalStreamsBuilder#build(): a built CausalTopology is immutable, so the
    // CausalProcessedStream handle the user still holds must not mutate this stage afterwards.
    private boolean frozen;

    ParsleyStageSpec(@Nullable String explicitName, Map<String, SourceSpec<KIn, VIn>> sources,
              ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier) {
        this.explicitName = explicitName;
        this.sources = new LinkedHashMap<>(sources);
        this.userSupplier = userSupplier;
    }

    /** Adds a sink declaration; rejected once {@link #freeze() frozen}. */
    void addSink(SinkSpec<KOut, VOut> sink) {
        requireMutable();
        sinks.add(sink);
    }

    /** Sets the stage-wide sink partitioner; rejected once {@link #freeze() frozen}. */
    void partitioner(StreamPartitioner<? super KOut, ? super VOut> partitioner) {
        requireMutable();
        this.partitioner = partitioner;
    }

    /** Marks this stage immutable — called by {@code CausalStreamsBuilder#build()} on every stage it snapshots. */
    void freeze() {
        frozen = true;
    }

    private void requireMutable() {
        if (frozen) {
            throw new IllegalStateException(
                    "this stage's CausalStreamsBuilder has already built its CausalTopology; declare every "
                            + "sink (to) and the partitioner (withPartitioner) before calling build()");
        }
    }

    /** A registered source topic's key/value serdes, or {@code null} to defer to the runtime's defaults. */
    record SourceSpec<K, V>(@Nullable Serde<K> keySerde, @Nullable Serde<V> valueSerde) {
    }

    /** A registered sink: its topology node name, topic, and key/value serdes (nullable, as above). */
    record SinkSpec<K, V>(String name, String topic, @Nullable Serde<K> keySerde, @Nullable Serde<V> valueSerde) {
    }
}
