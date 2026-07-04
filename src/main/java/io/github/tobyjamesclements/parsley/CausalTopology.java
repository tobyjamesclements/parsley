package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The immutable causal topology {@link CausalStreamsBuilder#build()} produces: every declared stage's
 * source topics, processor, sink(s), partitioner and audit. This is a specification, not yet a real Kafka
 * Streams {@link Topology} — {@link #assemble} builds that once a {@link CausalStreams} runtime supplies
 * its {@code props}, which is when a stage's default-serde-deferred sources/sinks, and the runtime's
 * quiesce/coordination wiring, become known. Construct one with {@link CausalStreamsBuilder}; hand it to
 * {@code new CausalStreams(topology, props)}.
 *
 * <p><strong>Every stage gets its own dead-letter sink node, all writing to one shared topic.</strong>
 * A single sink parented on every stage's processor node would union every stage into one Kafka Streams
 * node group (sub-topology/task assignment is computed per node group), coupling stages that are
 * otherwise independent sub-topologies chained only through real topics — so each stage's dead-letter
 * sink is its own node, parented only on that stage, and only the destination topic name is shared.
 */
public final class CausalTopology {

    private final List<StageSpec<?, ?, ?, ?>> stages;
    private final @Nullable ParsleyTopicAdmin topicAdminOverride;

    CausalTopology(List<StageSpec<?, ?, ?, ?>> stages, @Nullable ParsleyTopicAdmin topicAdminOverride) {
        this.stages = stages;
        this.topicAdminOverride = topicAdminOverride;
    }

    /**
     * Assembles the real {@link Topology}: resolves Parsley's configuration and every stage's
     * default-serde-deferred sources/sinks from {@code props}, wires {@code quiesce} and (if configured)
     * {@code coordination} into every stage, and adds each stage's source/processor/sink nodes.
     *
     * @param props        standard Kafka Streams configuration plus Parsley's {@code parsley.*} keys
     * @param quiesce      the shared quiesce coordinator every stage's tasks register with
     * @param coordination the shared topology-epoch coordination handle, or {@code null} to run in epoch 0
     * @return the assembled {@code Topology}
     */
    Topology assemble(Properties props, CausalQuiesce quiesce, @Nullable CausalCoordination coordination) {
        ParsleyConfig config = resolveConfig(props);
        DefaultSerdes defaults = new DefaultSerdes(props);
        String applicationId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        String stagePrefix = (applicationId == null || applicationId.isEmpty()) ? "stage-" : applicationId + "-stage-";
        // One dead-letter topic shared by every stage — each stage gets its own sink NODE (never one
        // node with many parents; see the class javadoc) writing to this same topic name, so the DLQ is
        // one operator-facing destination without coupling stages' Kafka Streams node groups together.
        String deadLetterTopic = config.deadLetterTopic() != null ? config.deadLetterTopic()
                : (applicationId == null || applicationId.isEmpty() ? "deadletter" : applicationId + "-deadletter");

        Topology topology = new Topology();
        int index = 0;
        for (StageSpec<?, ?, ?, ?> stage : stages) {
            index++;
            String name = stage.explicitName != null ? stage.explicitName : stagePrefix + index;
            assembleStage(topology, stage, name, config, defaults, quiesce, coordination, deadLetterTopic);
        }
        return topology;
    }

    private <KIn, VIn, KOut, VOut> void assembleStage(
            Topology topology, StageSpec<KIn, VIn, KOut, VOut> stage, String name, ParsleyConfig config,
            DefaultSerdes defaults, CausalQuiesce quiesce, @Nullable CausalCoordination coordination,
            String deadLetterTopic) {
        Map<String, CausalBuffer<KIn, VIn>> sources = new LinkedHashMap<>();
        stage.sources.forEach((topic, source) -> sources.put(topic, CausalBuffer.of(topic,
                source.keySerde() != null ? source.keySerde() : defaults.key(),
                source.valueSerde() != null ? source.valueSerde() : defaults.value())));

        Set<String> sinkTopics = new LinkedHashSet<>();
        stage.sinks.forEach(sink -> sinkTopics.add(sink.topic()));
        List<String> sinkNodeNames = stage.sinks.stream().map(StageSpec.SinkSpec::name).toList();
        String deadLetterSinkName = name + "-deadletter-sink";

        CausalProcessors.Builder<KIn, VIn, KOut, VOut> causalBuilder = CausalProcessors.builder(stage.userSupplier)
                .addBufferStore(name)
                .addBuffers(sources.values())
                .config(config)
                .withAudit(stage.audit)
                .sinkTopics(sinkTopics)
                .sinkNodeNames(sinkNodeNames)
                .deadLetterSink(deadLetterSinkName)
                .withQuiesce(quiesce);
        if (coordination != null) {
            causalBuilder.withCoordination(coordination);
        }
        if (topicAdminOverride != null) {
            causalBuilder.topicAdmin(topicAdminOverride);
        }
        CausalProcessorSupplier<KIn, VIn, KOut, VOut> supplier = causalBuilder.build();

        String processorName = name + "-processor";
        String[] sourceNames = new String[sources.size()];
        int i = 0;
        for (CausalBuffer<KIn, VIn> buffer : sources.values()) {
            String sourceName = name + "-source-" + buffer.topic();
            topology.addSource(sourceName,
                    buffer.keySerde().deserializer(), buffer.valueSerde().deserializer(), buffer.topic());
            sourceNames[i++] = sourceName;
        }
        topology.addProcessor(processorName, supplier, sourceNames);
        for (StageSpec.SinkSpec<KOut, VOut> sink : stage.sinks) {
            Serde<KOut> keySerde = sink.keySerde() != null ? sink.keySerde() : defaults.key();
            Serde<VOut> valueSerde = sink.valueSerde() != null ? sink.valueSerde() : defaults.value();
            // partitioner may be null here — Topology.addSink's partitioner-accepting overload treats
            // that identically to the no-partitioner overload (falls back to the default key-hash
            // partitioner), so one call covers both cases.
            topology.addSink(sink.name(), sink.topic(),
                    keySerde.serializer(), valueSerde.serializer(), stage.partitioner, processorName);
        }
        // This stage's own dead-letter sink — parented only on this stage's processor node (never
        // shared across stages as a multi-parent sink; see the class javadoc), raw bytes, no
        // partitioner (a diagnostic topic, not part of the causal delivery path).
        topology.addSink(deadLetterSinkName, deadLetterTopic,
                Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer(), processorName);
    }

    /** Classpath {@code parsley.properties} as a base layer, overlaid with the runtime's {@code props}. */
    private static ParsleyConfig resolveConfig(Properties props) {
        Properties merged = ParsleyConfig.loadProperties();
        merged.putAll(props);
        return ParsleyConfig.from(merged);
    }

    /** Lazily resolves {@code default.key.serde}/{@code default.value.serde} from {@code props}. */
    private static final class DefaultSerdes {

        private final Properties props;
        private @Nullable StreamsConfig streamsConfig;

        DefaultSerdes(Properties props) {
            this.props = props;
        }

        @SuppressWarnings("unchecked")
        <T> Serde<T> key() {
            return (Serde<T>) streamsConfig().defaultKeySerde();
        }

        @SuppressWarnings("unchecked")
        <T> Serde<T> value() {
            return (Serde<T>) streamsConfig().defaultValueSerde();
        }

        private StreamsConfig streamsConfig() {
            StreamsConfig config = streamsConfig;
            if (config == null) {
                config = new StreamsConfig(props);
                streamsConfig = config;
            }
            return config;
        }
    }
}
