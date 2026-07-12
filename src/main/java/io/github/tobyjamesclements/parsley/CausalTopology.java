package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The immutable causal topology {@link CausalStreamsBuilder#build()} produces: every declared stage's
 * source topics, processor, sink(s), and partitioner. This is a specification, not yet a real Kafka
 * Streams {@link Topology} — {@link #assemble} builds that once a {@link CausalStreams} runtime supplies
 * its {@code props}, which is when a stage's default-serde-deferred sources/sinks, and the runtime's
 * quiesce/coordination wiring, become known. Construct one with {@link CausalStreamsBuilder}; hand it to
 * {@code new CausalStreams(topology, props)}.
 *
 * <p><strong>{@code processing.guarantee=exactly_once_v2} is required, unconditionally.</strong> {@link
 * #assemble} throws {@link IllegalStateException} otherwise — see {@link #requireExactlyOnce} for why.
 */
public final class CausalTopology {

    private final List<ParsleyStageSpec<?, ?, ?, ?>> stages;
    private final @Nullable ParsleyTopicAdmin topicAdminOverride;

    CausalTopology(List<ParsleyStageSpec<?, ?, ?, ?>> stages, @Nullable ParsleyTopicAdmin topicAdminOverride) {
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
     * @throws IllegalStateException if {@code props} does not configure {@code exactly_once_v2}
     */
    Topology assemble(Properties props, ParsleyQuiesce quiesce, @Nullable ParsleyCoordination coordination) {
        requireExactlyOnce(props);
        ParsleyConfig config = resolveConfig(props);
        DefaultSerdes defaults = new DefaultSerdes(props);
        String applicationId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        String stagePrefix = (applicationId == null || applicationId.isEmpty()) ? "stage-" : applicationId + "-stage-";

        Topology topology = new Topology();
        int index = 0;
        for (ParsleyStageSpec<?, ?, ?, ?> stage : stages) {
            index++;
            String name = stage.explicitName != null ? stage.explicitName : stagePrefix + index;
            assembleStage(topology, stage, name, config, defaults, quiesce, coordination);
        }
        return topology;
    }

    /**
     * Requires {@code processing.guarantee=exactly_once_v2}, unconditionally — never gated by {@code
     * parsley.topology.validation}, since this is a correctness requirement, not a topology-shape lint.
     *
     * <p>Parsley's crash-safety reasoning (the frontier-before-buffer-removal write ordering throughout
     * {@code ParsleyEngine}/{@code ParsleyFrontier}) narrows an at-least-once torn-write window to a
     * benign tear direction, but two separate changelog topics still have no cross-store atomicity under
     * at-least-once: a crash during the commit-time flush can, rarely, ack one topic's batch and lose the
     * other's. Exactly-once-v2 wraps every state-store changelog write, every produced record, and the
     * consumer offset commit into one Kafka transaction, so a crash can never leave a torn write between
     * them at all — closing that residual window completely, the same way a transactional producer
     * requires {@code enable.idempotence}/a transactional id rather than treating it as optional hardening.
     *
     * @throws IllegalStateException if {@code props} does not configure {@code exactly_once_v2}
     */
    private static void requireExactlyOnce(Properties props) {
        String guarantee = props.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        if (!StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
            throw new IllegalStateException("parsley requires " + StreamsConfig.PROCESSING_GUARANTEE_CONFIG + "="
                    + StreamsConfig.EXACTLY_ONCE_V2 + " (found '" + guarantee + "'); Parsley's crash-safety "
                    + "guarantees depend on Kafka Streams' transactional multi-store commit to fully close a "
                    + "torn-write window that write ordering alone can only narrow, never eliminate, under "
                    + "at-least-once");
        }
    }

    private <KIn, VIn, KOut, VOut> void assembleStage(
            Topology topology, ParsleyStageSpec<KIn, VIn, KOut, VOut> stage, String name, ParsleyConfig config,
            DefaultSerdes defaults, ParsleyQuiesce quiesce, @Nullable ParsleyCoordination coordination) {
        Map<String, ParsleySource<KIn, VIn>> sources = new LinkedHashMap<>();
        stage.sources.forEach((topic, source) -> sources.put(topic, new ParsleySource<>(topic,
                source.keySerde() != null ? source.keySerde() : defaults.key(),
                source.valueSerde() != null ? source.valueSerde() : defaults.value())));

        Set<String> sinkTopics = new LinkedHashSet<>();
        stage.sinks.forEach(sink -> sinkTopics.add(sink.topic()));
        List<String> sinkNodeNames = stage.sinks.stream().map(ParsleyStageSpec.SinkSpec::name).toList();

        // A domain topic this stage neither consumes nor produces: wired below as an extra, raw
        // byte[]/byte[] source into this SAME processor node (see ParsleyProcessor's passthrough-record
        // handling), so this stage's declared subscriptions can cover the full coordinated domain
        // (ParsleyProcessor#validateFullMeshCoverage) without hand-wiring an "independent input" stage for
        // it. Empty whenever domain-topics is not configured — no behaviour change then.
        Set<String> passthroughTopics = new LinkedHashSet<>(config.coordinationDomainTopics());
        passthroughTopics.removeAll(sources.keySet());
        passthroughTopics.removeAll(sinkTopics);
        if (coordination == null) {
            passthroughTopics = Set.of();
        }

        ParsleyProcessorSupplier.Builder<KIn, VIn, KOut, VOut> causalBuilder = ParsleyProcessorSupplier.builder(stage.userSupplier)
                .addBufferStore(name)
                .addSources(sources.values())
                .declareTopics(passthroughTopics)
                .config(config)
                .sinkTopics(sinkTopics)
                .sinkNodeNames(sinkNodeNames)
                .withQuiesce(quiesce);
        if (coordination != null) {
            causalBuilder.withCoordination(coordination);
        }
        if (topicAdminOverride != null) {
            causalBuilder.topicAdmin(topicAdminOverride);
        }
        ParsleyProcessorSupplier<KIn, VIn, KOut, VOut> supplier = causalBuilder.build();

        String processorName = name + "-processor";
        String[] sourceNames = new String[sources.size() + passthroughTopics.size()];
        int i = 0;
        for (ParsleySource<KIn, VIn> buffer : sources.values()) {
            String sourceName = name + "-source-" + buffer.topic();
            topology.addSource(sourceName,
                    buffer.keySerde().deserializer(), buffer.valueSerde().deserializer(), buffer.topic());
            sourceNames[i++] = sourceName;
        }
        for (String passthroughTopic : passthroughTopics) {
            String sourceName = name + "-passthrough-source-" + passthroughTopic;
            // Raw byte[]/byte[] regardless of this stage's own KIn/VIn — a passthrough topic's value
            // schema is unrelated to this stage's business types. ParsleyProcessor recognises it by its
            // own source topic and never hands it to the delegate; see its class Javadoc.
            topology.addSource(sourceName,
                    Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer(), passthroughTopic);
            sourceNames[i++] = sourceName;
        }
        topology.addProcessor(processorName, supplier, sourceNames);
        // Wrap once per stage (not per sink): every sink a stage declares shares one partitioner
        // (CausalStreamsBuilder's own contract), and the wrapper's only job — routing a Parsley protocol
        // marker to the forwarding task's own owned partition, via ParsleyMarkerPartition, regardless of
        // the marker's key — is identical for every one of this stage's sinks. stage.partitioner may
        // itself be null (no custom partitioner declared); the wrapper still falls back to Kafka's
        // default key-hash partitioner for every non-marker (business) forward.
        StreamPartitioner<? super KOut, ? super VOut> markerAwarePartitioner =
                new ParsleyMarkerPartitioner<>(stage.partitioner);
        for (ParsleyStageSpec.SinkSpec<KOut, VOut> sink : stage.sinks) {
            Serde<KOut> keySerde = sink.keySerde() != null ? sink.keySerde() : defaults.key();
            Serde<VOut> valueSerde = sink.valueSerde() != null ? sink.valueSerde() : defaults.value();
            topology.addSink(sink.name(), sink.topic(),
                    keySerde.serializer(), valueSerde.serializer(), markerAwarePartitioner, processorName);
        }
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
