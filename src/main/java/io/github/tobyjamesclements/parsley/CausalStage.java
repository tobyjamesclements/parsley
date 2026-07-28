package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * One causal processing stage: typed source topics each with their own {@link SourceHandler},
 * typed sink topics, and the causal boundary in between — the protocol core gating and
 * ordering deliveries on the source side, the single stamping site on the sink side.
 *
 * <p>Serialization happens exactly once on each side, inside the stage: inbound bytes are held
 * verbatim while gated and deserialized at delivery with the source topic's serdes; emits are
 * serialized with the sink topic's serdes at the stamping site, so the clock travels with the
 * exact bytes it claims. Note that non-Parsley headers of a held record are not carried
 * through delivery.
 *
 * <p>Stages compose into a {@link CausalTopology}; a stage corresponds to one Kafka Streams
 * sub-topology, and its name keys its state store, so the name must stay stable across
 * deployments of the same application.
 */
public final class CausalStage {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStage.class);

    private record SourceDef<K, V>(CausalTopic<K, V> topic, SourceHandler<K, V> handler) {

        void dispatch(Record<byte[], byte[]> raw, StageContext ctx) {
            K key = topic.keySerde().deserializer().deserialize(topic.name(), raw.key());
            V value = topic.valueSerde().deserializer().deserialize(topic.name(), raw.value());
            handler.handle(new Record<>(key, value, raw.timestamp(), raw.headers()), ctx);
        }
    }

    private final String name;
    private final Map<String, SourceDef<?, ?>> sources;
    private final Map<String, CausalTopic<?, ?>> sinks;
    private final List<StoreBuilder<?>> userStores;
    private final Duration truncationInterval;
    private TopicIds topicIds;
    private BrokerOffsetsProvider brokerOffsets;

    /** Creates the per-task broker-offsets view; production uses an admin client. */
    interface BrokerOffsetsProvider {
        BrokerOffsets create(TopicIds topicIds, Set<String> sinkTopics);
    }

    private CausalStage(Builder b) {
        this.name = b.name;
        this.sources = b.sources;
        this.sinks = b.sinks;
        this.userStores = b.userStores;
        this.truncationInterval = b.truncationInterval;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final Map<String, SourceDef<?, ?>> sources = new LinkedHashMap<>();
        private final Map<String, CausalTopic<?, ?>> sinks = new LinkedHashMap<>();
        private final List<StoreBuilder<?>> userStores = new ArrayList<>();
        private Duration truncationInterval = Duration.ofMinutes(10);

        private Builder(String name) {
            if (!name.matches("[a-zA-Z0-9-]+")) {
                throw new IllegalArgumentException("stage name must be [a-zA-Z0-9-]+: " + name);
            }
            this.name = name;
        }

        /** Declares a source topic and the handler its causally delivered records run. */
        public <K, V> Builder source(CausalTopic<K, V> topic, SourceHandler<K, V> handler) {
            if (sources.put(topic.name(), new SourceDef<>(topic, handler)) != null) {
                throw new IllegalArgumentException("duplicate source topic: " + topic.name());
            }
            return this;
        }

        /** Declares a sink topic; handlers emit to it via {@link StageContext#emit}. */
        public Builder sink(CausalTopic<?, ?> topic) {
            sinks.put(topic.name(), topic);
            return this;
        }

        /** Declares state stores the stage's handlers use via {@link StageContext#store}. */
        public Builder stores(StoreBuilder<?>... builders) {
            userStores.addAll(List.of(builders));
            return this;
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Builder truncationInterval(Duration interval) {
            this.truncationInterval = interval;
            return this;
        }

        public CausalStage build() {
            if (sources.isEmpty()) throw new IllegalStateException("no sources declared");
            return new CausalStage(this);
        }
    }

    String name() {
        return name;
    }

    String storeName() {
        return "parsley-" + name + "-state";
    }

    private String sourceNode() {
        return "parsley-" + name + "-source";
    }

    private String processorNode() {
        return "parsley-" + name + "-processor";
    }

    private String sinkNode(String topic) {
        return "parsley-" + name + "-sink-" + topic;
    }

    Set<String> sourceTopics() {
        return sources.keySet();
    }

    Set<String> sinkTopics() {
        return sinks.keySet();
    }

    void wire(TopicIds ids, BrokerOffsetsProvider provider) {
        this.topicIds = ids;
        this.brokerOffsets = provider;
    }

    /**
     * A broker-less topology for {@code TopologyTestDriver}: topic identity is synthesized
     * deterministically, every topic has one partition (the driver's reality), and the offset
     * queries answer empty — safe precisely because no broker means no prior incarnations to
     * seed against and nothing to truncate.
     */
    public Topology testTopology() {
        wireForTest();
        return topology();
    }

    /** Applies the broker-less wiring ({@link #testTopology()} and multi-stage test paths). */
    void wireForTest() {
        wire(topic -> new TopicIds.Resolved(testChannel(topic, 0).topicId(), 1),
                (ids, sinkTopics) -> new BrokerOffsets() {
                    @Override
                    public Map<Channel, Long> endOffsets(Set<UUID> t) {
                        return Map.of();
                    }

                    @Override
                    public EarliestOffsets earliestOffsets(Set<Channel> channels) {
                        return new EarliestOffsets(Map.of(), Set.of());
                    }
                });
    }

    /** The channel {@link #testTopology()} resolves for a topic-partition. */
    static Channel testChannel(String topic, int partition) {
        return new Channel(UUID.nameUUIDFromBytes(topic.getBytes()), partition);
    }

    /** Assembles the Streams topology (single-stage form). */
    Topology topology() {
        Topology t = new Topology();
        addTo(t);
        return t;
    }

    /** Adds this stage's nodes to a shared topology. Wiring must be present. */
    void addTo(Topology t) {
        if (topicIds == null || brokerOffsets == null) {
            throw new IllegalStateException("unwired stage: start it with CausalStreams.start");
        }
        t.addSource(sourceNode(), new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                sources.keySet().toArray(String[]::new));
        t.addProcessor(processorNode(), new AdapterSupplier(), sourceNode());
        for (String sink : sinks.keySet()) {
            // The adapter partitions before stamping (sequence claims are per channel), so the
            // sink must land each record on the partition the adapter chose. The partitioner
            // recomputes the same deterministic function.
            int partitions = topicIds.resolve(sink).partitions();
            org.apache.kafka.streams.processor.StreamPartitioner<byte[], byte[]> partitioner =
                    (topic, key, value, numPartitions) ->
                            java.util.Optional.of(Set.of(partitionFor(key, partitions)));
            t.addSink(sinkNode(sink), sink, new ByteArraySerializer(), new ByteArraySerializer(),
                    partitioner, processorNode());
        }
    }

    /**
     * The deterministic partition function shared by the stamping site and the sink
     * partitioner: producer-default murmur2 for keyed records, partition zero for null keys.
     * The producer's sticky partitioning is nondeterministic, and the stamp must know the
     * destination channel before the send, so both sites recompute this function — they must
     * agree byte for byte.
     */
    static int partitionFor(byte[] key, int numPartitions) {
        if (key == null) return 0;
        return org.apache.kafka.common.utils.Utils.toPositive(
                org.apache.kafka.common.utils.Utils.murmur2(key)) % numPartitions;
    }

    private final class AdapterSupplier implements ProcessorSupplier<byte[], byte[], byte[], byte[]> {

        @Override
        public Set<StoreBuilder<?>> stores() {
            Set<StoreBuilder<?>> all = new HashSet<>(userStores);
            all.add(Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(storeName()), Serdes.Bytes(), Serdes.ByteArray())
                    .withCachingDisabled());
            return all;
        }

        @Override
        public Processor<byte[], byte[], byte[], byte[]> get() {
            return new Adapter();
        }
    }

    private final class Adapter implements Processor<byte[], byte[], byte[], byte[]> {

        private ProcessorContext<byte[], byte[]> context;
        private CausalNode node;
        private StageContextImpl stageContext;
        private final Map<String, Channel> channelByTopic = new HashMap<>();
        private final Map<Channel, String> topicByChannel = new HashMap<>();
        private int taskPartition;

        @Override
        public void init(ProcessorContext<byte[], byte[]> context) {
            this.context = context;
            this.taskPartition = context.taskId().partition();

            Set<Channel> consumed = new HashSet<>();
            for (String topic : sources.keySet()) {
                Channel c = new Channel(topicIds.resolve(topic).id(), taskPartition);
                consumed.add(c);
                channelByTopic.put(topic, c);
                topicByChannel.put(c, topic);
            }
            Set<UUID> sinkIds = new HashSet<>();
            for (String topic : sinks.keySet()) {
                sinkIds.add(topicIds.resolve(topic).id());
            }

            KeyValueStore<Bytes, byte[]> kv = context.getStateStore(storeName());
            BrokerOffsets offsets = brokerOffsets.create(topicIds, sinks.keySet());
            // Sender identity must survive topology evolution: task ids embed the sub-topology
            // index, which renumbers when stages are added, so derive from the user-stable
            // stage name and the partition instead.
            UUID senderId = UUID.nameUUIDFromBytes(
                    (context.applicationId() + "/" + name + "/" + taskPartition).getBytes());
            node = new CausalNode(
                    new NodeConfig(name + "-task-" + context.taskId(), senderId, consumed, sinkIds,
                            taskPartition),
                    new KafkaStateStore(kv), offsets);
            stageContext = new StageContextImpl();

            context.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME,
                    ts -> sweepPositions());
            // The coordination-free truncation driver: log starts are a true global stability
            // bound (retention-deleted records sit below every reachable baseline). A failed
            // sweep skips a cycle; it must never fail the task for garbage collection.
            context.schedule(truncationInterval, PunctuationType.WALL_CLOCK_TIME, ts -> {
                try {
                    var earliest = offsets.earliestOffsets(node.stampChannels());
                    node.truncateToLogStarts(earliest.logStarts(), earliest.confirmedAbsent());
                } catch (RuntimeException e) {
                    LOG.warn("truncation sweep skipped: {}", e.toString());
                }
            });
        }

        @Override
        public void process(Record<byte[], byte[]> r) {
            var meta = context.recordMetadata().orElseThrow(() -> new IllegalStateException(
                    "no record metadata: cannot establish the record's coordinate (fail closed)"));
            Channel c = channelByTopic.get(meta.topic());
            if (c == null || meta.partition() != taskPartition) {
                throw new IllegalStateException("record from unexpected source "
                        + meta.topic() + ":" + meta.partition());
            }
            VectorClock clock = CausalHeaders.read(r.headers());
            UUID sender = CausalHeaders.readSender(r.headers());
            long seq = CausalHeaders.readSeq(r.headers());
            deliverAll(node.onRecord(new InboundRecord(
                    c, meta.offset(), clock, sender, sender == null ? -1 : seq,
                    r.key(), r.value(), r.timestamp())));
            sweepPositions();
        }

        private void sweepPositions() {
            Map<TopicPartition, Long> positions = Positions.forCurrentThread();
            for (Map.Entry<String, Channel> e : channelByTopic.entrySet()) {
                Long pos = positions.get(new TopicPartition(e.getKey(), taskPartition));
                if (pos != null) {
                    deliverAll(node.positionAdvance(e.getValue(), pos));
                }
            }
        }

        private void deliverAll(List<Delivery> deliveries) {
            for (Delivery d : deliveries) {
                String topic = topicByChannel.get(d.channel());
                SourceDef<?, ?> def = sources.get(topic);
                stageContext.inFlightTimestamp = d.timestamp();
                try {
                    def.dispatch(new Record<>(d.key(), d.value(), d.timestamp(), new RecordHeaders()),
                            stageContext);
                } finally {
                    stageContext.inFlightTimestamp = null;
                }
            }
        }

        /** The narrow surface handed to handlers: emit, stores, punctuators — nothing else. */
        private final class StageContextImpl implements StageContext {

            private Long inFlightTimestamp;

            @Override
            public <K, V> void emit(CausalTopic<K, V> topic, K key, V value) {
                if (inFlightTimestamp == null) {
                    throw new IllegalStateException("no record in flight (punctuator?): use the"
                            + " timestamped emit overload");
                }
                emit(topic, key, value, inFlightTimestamp);
            }

            @Override
            public <K, V> void emit(CausalTopic<K, V> topic, K key, V value, long timestamp) {
                if (!sinks.containsKey(topic.name())) {
                    throw new IllegalStateException("emit to undeclared sink topic " + topic.name()
                            + " (declare it with Builder.sink)");
                }
                byte[] keyBytes = topic.keySerde().serializer().serialize(topic.name(), key);
                byte[] valueBytes = topic.valueSerde().serializer().serialize(topic.name(), value);
                var resolved = topicIds.resolve(topic.name());
                Channel dest = new Channel(resolved.id(), partitionFor(keyBytes, resolved.partitions()));
                var headers = new RecordHeaders();
                CausalHeaders.write(headers, node.prepareSend(dest));
                context.forward(new Record<>(keyBytes, valueBytes, timestamp, headers),
                        sinkNode(topic.name()));
            }

            @Override
            public <S extends org.apache.kafka.streams.processor.StateStore> S store(String storeName) {
                return context.getStateStore(storeName);
            }

            @Override
            public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
                return context.schedule(interval, type, callback);
            }
        }
    }
}
