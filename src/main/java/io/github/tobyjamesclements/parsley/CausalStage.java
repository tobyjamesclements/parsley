package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * One causal processing stage: typed source topics, each processed by an ordinary Kafka
 * Streams {@link Processor} supplied per source, and typed sink topics — with the causal
 * boundary in between. The protocol core gates and orders deliveries on the source side; the
 * single stamping site sits on the sink side.
 *
 * <p>User processors are full Streams citizens: supplied per task ({@code supplier.get()}),
 * with {@code init}/{@code close} lifecycle, connected stores via
 * {@code ProcessorSupplier.stores()}, and a real {@link ProcessorContext} — punctuators,
 * task metadata, metrics, stream time all behave as in any Streams application. Exactly the
 * capabilities that could violate causality are re-routed or withheld:
 *
 * <ul>
 *   <li>{@code forward} goes through the stamping door: serialized with the sink topic's
 *       serdes, deterministically partitioned before stamping, clock and sender tag attached.
 *       A named forward addresses a declared sink by topic name; an unnamed forward goes to
 *       every declared sink (Streams' fan-out semantics). Forwarding to an undeclared sink
 *       fails loudly.</li>
 *   <li>{@code getStateStore} refuses the stage's own protocol store.</li>
 *   <li>{@code recordMetadata} reports the coordinate of the record being delivered — for a
 *       record released from the hold queue, that is its own coordinate, not the coordinate
 *       of whichever later record triggered the release.</li>
 * </ul>
 *
 * <p>Serialization happens exactly once on each side, inside the stage: inbound bytes are held
 * verbatim while gated and deserialized at delivery with the source topic's serdes; forwards
 * are serialized with the sink topic's serdes at the stamping site, so the clock travels with
 * the exact bytes it claims. Note that non-Parsley headers of a held record are not carried
 * through delivery.
 *
 * <p>Stages compose into a {@link CausalTopology}; a stage corresponds to one Kafka Streams
 * sub-topology, and its name keys its state store, so the name must stay stable across
 * deployments of the same application.
 */
public final class CausalStage {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStage.class);

    private record SourceDef<K, V>(CausalTopic<K, V> topic, ProcessorSupplier<K, V, ?, ?> supplier) {}

    private final String name;
    private final Map<String, SourceDef<?, ?>> sources;
    private final Map<String, CausalTopic<?, ?>> sinks;
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
        this.truncationInterval = b.truncationInterval;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final Map<String, SourceDef<?, ?>> sources = new LinkedHashMap<>();
        private final Map<String, CausalTopic<?, ?>> sinks = new LinkedHashMap<>();
        private Duration truncationInterval = Duration.ofMinutes(10);

        private Builder(String name) {
            if (!name.matches("[a-zA-Z0-9-]+")) {
                throw new IllegalArgumentException("stage name must be [a-zA-Z0-9-]+: " + name);
            }
            this.name = name;
        }

        /**
         * Declares a source topic and the processor its causally delivered records run
         * through. The supplier is an ordinary Streams {@link ProcessorSupplier}: one
         * processor instance per task, {@code init}/{@code close} lifecycle, connected state
         * stores via {@code stores()}.
         */
        public <K, V> Builder source(CausalTopic<K, V> topic, ProcessorSupplier<K, V, ?, ?> supplier) {
            if (sources.put(topic.name(), new SourceDef<>(topic, supplier)) != null) {
                throw new IllegalArgumentException("duplicate source topic: " + topic.name());
            }
            return this;
        }

        /** Declares a sink topic; processors forward to it by topic name. */
        public Builder sink(CausalTopic<?, ?> topic) {
            sinks.put(topic.name(), topic);
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
                            Optional.of(Set.of(partitionFor(key, partitions)));
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
            Set<StoreBuilder<?>> all = new HashSet<>();
            all.add(Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(storeName()), Serdes.Bytes(), Serdes.ByteArray())
                    .withCachingDisabled());
            for (SourceDef<?, ?> def : sources.values()) {
                Set<StoreBuilder<?>> user = def.supplier().stores();
                if (user == null) continue;
                for (StoreBuilder<?> sb : user) {
                    if (sb.name().startsWith("parsley-")) {
                        throw new IllegalStateException("user store name collides with the"
                                + " protocol's namespace: " + sb.name());
                    }
                    all.add(sb);
                }
            }
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
        private CausalContext userContext;
        private final Map<String, Channel> channelByTopic = new HashMap<>();
        private final Map<Channel, String> topicByChannel = new HashMap<>();
        /** One user processor per source topic, instantiated per task (Streams lifecycle). */
        private final Map<String, Processor<Object, Object, Object, Object>> userProcessors = new HashMap<>();
        private int taskPartition;

        @Override
        @SuppressWarnings("unchecked")
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
            userContext = new CausalContext();

            for (Map.Entry<String, SourceDef<?, ?>> e : sources.entrySet()) {
                Processor<Object, Object, Object, Object> p =
                        (Processor<Object, Object, Object, Object>) e.getValue().supplier().get();
                p.init((ProcessorContext<Object, Object>) (ProcessorContext<?, ?>) userContext);
                userProcessors.put(e.getKey(), p);
            }

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

        @Override
        public void close() {
            userProcessors.values().forEach(Processor::close);
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

        @SuppressWarnings("unchecked")
        private void deliverAll(List<Delivery> deliveries) {
            for (Delivery d : deliveries) {
                String topic = topicByChannel.get(d.channel());
                SourceDef<?, ?> def = sources.get(topic);
                Object key = def.topic().keySerde().deserializer().deserialize(topic, d.key());
                Object value = def.topic().valueSerde().deserializer().deserialize(topic, d.value());
                userContext.inFlight = d;
                try {
                    userProcessors.get(topic).process(
                            new Record<>(key, value, d.timestamp(), new RecordHeaders()));
                } finally {
                    userContext.inFlight = null;
                }
            }
        }

        /**
         * The Streams context handed to user processors: everything delegates to the task's
         * real context except the three members that could misstate or violate causality —
         * forward (the stamping door), getStateStore (the protocol store is off limits), and
         * recordMetadata (the delivered record's own coordinate, not the release trigger's).
         */
        private final class CausalContext implements ProcessorContext<Object, Object> {

            private Delivery inFlight;

            @Override
            public <K1, V1> void forward(Record<K1, V1> record) {
                for (String sink : sinks.keySet()) {
                    forwardTo(sink, record);
                }
            }

            @Override
            public <K1, V1> void forward(Record<K1, V1> record, String childName) {
                if (!sinks.containsKey(childName)) {
                    throw new IllegalArgumentException("unknown sink topic: " + childName
                            + " (forward by declared sink topic name; declared: " + sinks.keySet() + ")");
                }
                forwardTo(childName, record);
            }

            @SuppressWarnings("unchecked")
            private void forwardTo(String sink, Record<?, ?> record) {
                CausalTopic<Object, Object> topic = (CausalTopic<Object, Object>) sinks.get(sink);
                byte[] key = topic.keySerde().serializer().serialize(sink, record.key());
                byte[] value = topic.valueSerde().serializer().serialize(sink, record.value());
                var resolved = topicIds.resolve(sink);
                Channel dest = new Channel(resolved.id(), partitionFor(key, resolved.partitions()));
                var headers = new RecordHeaders(record.headers());
                CausalHeaders.write(headers, node.prepareSend(dest));
                context.forward(new Record<>(key, value, record.timestamp(), headers), sinkNode(sink));
            }

            @Override
            public <S extends org.apache.kafka.streams.processor.StateStore> S getStateStore(String storeName) {
                if (storeName.equals(CausalStage.this.storeName())) {
                    throw new IllegalArgumentException(
                            "the protocol's state store is not accessible to user processors");
                }
                return context.getStateStore(storeName);
            }

            @Override
            public Optional<RecordMetadata> recordMetadata() {
                if (inFlight == null) return Optional.empty();
                String topic = topicByChannel.get(inFlight.channel());
                long offset = inFlight.offset();
                return Optional.of(new RecordMetadata() {
                    @Override
                    public String topic() {
                        return topic;
                    }

                    @Override
                    public int partition() {
                        return taskPartition;
                    }

                    @Override
                    public long offset() {
                        return offset;
                    }
                });
            }

            // ---- pure delegation below ----

            @Override
            public String applicationId() {
                return context.applicationId();
            }

            @Override
            public TaskId taskId() {
                return context.taskId();
            }

            @Override
            public Serde<?> keySerde() {
                return context.keySerde();
            }

            @Override
            public Serde<?> valueSerde() {
                return context.valueSerde();
            }

            @Override
            public File stateDir() {
                return context.stateDir();
            }

            @Override
            public org.apache.kafka.streams.StreamsMetrics metrics() {
                return context.metrics();
            }

            @Override
            public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
                return context.schedule(interval, type, callback);
            }

            @Override
            public Cancellable schedule(java.time.Instant startTime, Duration interval,
                                        PunctuationType type, Punctuator callback) {
                return context.schedule(startTime, interval, type, callback);
            }

            @Override
            public void commit() {
                context.commit();
            }

            @Override
            public Map<String, Object> appConfigs() {
                return context.appConfigs();
            }

            @Override
            public Map<String, Object> appConfigsWithPrefix(String prefix) {
                return context.appConfigsWithPrefix(prefix);
            }

            @Override
            public long currentSystemTimeMs() {
                return context.currentSystemTimeMs();
            }

            @Override
            public long currentStreamTimeMs() {
                return context.currentStreamTimeMs();
            }
        }
    }
}
