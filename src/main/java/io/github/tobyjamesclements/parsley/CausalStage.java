package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * One causal processing stage as a Kafka Streams {@link Topology}: source topics fetched as
 * raw bytes, the protocol core gating and ordering deliveries, the user's processor running on
 * typed records, and sinks written as raw bytes with the dependency-clock header stamped at
 * the single stamping site.
 *
 * <p>Serialization happens exactly once on each side, inside the adapter: inbound bytes are
 * held verbatim while gated and deserialized at delivery; outbound records are serialized at
 * forward time so the stamp travels with the exact bytes it claims. Note that non-Parsley
 * headers of a held record are not yet carried through delivery.
 *
 * <p>Production wiring ({@link CausalStreams#start}) supplies admin-backed {@link TopicIds}
 * and {@link BrokerOffsets}; tests inject fixed ids and a stub offsets view and drive the
 * topology with {@code TopologyTestDriver}.
 */
public final class CausalStage<K, V, KO, VO> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStage.class);

    static final String STORE_NAME = "parsley-causal-state";
    static final String SOURCE_NAME = "parsley-source";
    static final String PROCESSOR_NAME = "parsley-processor";
    static final String SINK_PREFIX = "parsley-sink-";

    /** Creates the per-task {@link BrokerOffsets}; production uses an admin client. */
    interface BrokerOffsetsProvider {
        BrokerOffsets create(TopicIds topicIds, Set<String> sinkTopics);
    }

    private record SourceDef<K, V>(Serde<K> keySerde, Serde<V> valueSerde) {}

    private record SinkDef<KO, VO>(Serde<KO> keySerde, Serde<VO> valueSerde) {}

    private final Map<String, SourceDef<K, V>> sources;
    private final Map<String, SinkDef<KO, VO>> sinks;
    private final ProcessorSupplier<K, V, KO, VO> userSupplier;
    private final Duration truncationInterval;
    private TopicIds topicIds;
    private BrokerOffsetsProvider brokerOffsets;

    private CausalStage(Builder<K, V, KO, VO> b) {
        this.sources = b.sources;
        this.sinks = b.sinks;
        this.userSupplier = b.userSupplier;
        this.truncationInterval = b.truncationInterval;
    }

    public static <K, V, KO, VO> Builder<K, V, KO, VO> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, V, KO, VO> {
        private final Map<String, SourceDef<K, V>> sources = new LinkedHashMap<>();
        private final Map<String, SinkDef<KO, VO>> sinks = new LinkedHashMap<>();
        private ProcessorSupplier<K, V, KO, VO> userSupplier;
        private Duration truncationInterval = Duration.ofMinutes(10);

        public Builder<K, V, KO, VO> source(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
            sources.put(topic, new SourceDef<>(keySerde, valueSerde));
            return this;
        }

        public Builder<K, V, KO, VO> processor(ProcessorSupplier<K, V, KO, VO> supplier) {
            this.userSupplier = supplier;
            return this;
        }

        public Builder<K, V, KO, VO> sink(String topic, Serde<KO> keySerde, Serde<VO> valueSerde) {
            sinks.put(topic, new SinkDef<>(keySerde, valueSerde));
            return this;
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Builder<K, V, KO, VO> truncationInterval(Duration interval) {
            this.truncationInterval = interval;
            return this;
        }

        public CausalStage<K, V, KO, VO> build() {
            if (sources.isEmpty()) throw new IllegalStateException("no sources declared");
            if (userSupplier == null) throw new IllegalStateException("no processor declared");
            return new CausalStage<>(this);
        }
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
     * deterministically ({@link #testChannel} gives a test the same channels for crafting or
     * asserting on clock headers), every topic has one partition (the driver's reality), and
     * the offset queries answer empty — safe precisely because no broker means no prior
     * incarnations to seed against and nothing to truncate.
     */
    public Topology testTopology() {
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
        return topology();
    }

    /** The channel {@link #testTopology()} resolves for a topic-partition. */
    static Channel testChannel(String topic, int partition) {
        return new Channel(UUID.nameUUIDFromBytes(topic.getBytes()), partition);
    }

    /** Assembles the Streams topology. Wiring must be present ({@code CausalStreams.start}). */
    Topology topology() {
        if (topicIds == null || brokerOffsets == null) {
            throw new IllegalStateException("unwired stage: start it with CausalStreams.start");
        }
        Topology t = new Topology();
        t.addSource(SOURCE_NAME, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                sources.keySet().toArray(String[]::new));
        t.addProcessor(PROCESSOR_NAME, new AdapterSupplier(), SOURCE_NAME);
        for (String sink : sinks.keySet()) {
            // The adapter partitions before stamping (sequence claims are per channel), so the
            // sink must land each record on the partition the adapter chose. The partitioner
            // recomputes the same deterministic function.
            int partitions = topicIds.resolve(sink).partitions();
            org.apache.kafka.streams.processor.StreamPartitioner<byte[], byte[]> partitioner =
                    (topic, key, value, numPartitions) ->
                            java.util.Optional.of(Set.of(partitionFor(key, partitions)));
            t.addSink(SINK_PREFIX + sink, sink, new ByteArraySerializer(), new ByteArraySerializer(),
                    partitioner, PROCESSOR_NAME);
        }
        return t;
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
                            Stores.persistentKeyValueStore(STORE_NAME), Serdes.Bytes(), Serdes.ByteArray())
                    .withCachingDisabled());
            Set<StoreBuilder<?>> user = userSupplier.stores();
            if (user != null) all.addAll(user);
            return all;
        }

        @Override
        public Processor<byte[], byte[], byte[], byte[]> get() {
            return new Adapter();
        }
    }

    private final class Adapter implements Processor<byte[], byte[], byte[], byte[]> {

        private ProcessorContext<byte[], byte[]> context;
        private Processor<K, V, KO, VO> userProcessor;
        private CausalNode node;
        private final Map<String, Channel> channelByTopic = new HashMap<>();
        private final Map<Channel, String> topicByChannel = new HashMap<>();
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

            KeyValueStore<Bytes, byte[]> kv = context.getStateStore(STORE_NAME);
            BrokerOffsets offsets = brokerOffsets.create(topicIds, sinks.keySet());
            // The sender identity must be stable across restarts and rebalances: derive it
            // from the application and task ids, which name this partition group durably.
            UUID senderId = UUID.nameUUIDFromBytes(
                    (context.applicationId() + "/" + context.taskId()).getBytes());
            node = new CausalNode(
                    new NodeConfig("task-" + context.taskId(), senderId, consumed, sinkIds,
                            taskPartition),
                    new KafkaStateStore(kv), offsets);

            userProcessor = userSupplier.get();
            userProcessor.init(new StampingContext(context));

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
                SourceDef<K, V> def = sources.get(topic);
                K key = def.keySerde().deserializer().deserialize(topic, d.key());
                V value = def.valueSerde().deserializer().deserialize(topic, d.value());
                userProcessor.process(new Record<>(key, value, d.timestamp(), new RecordHeaders()));
            }
        }

        @Override
        public void close() {
            if (userProcessor != null) userProcessor.close();
        }

        /**
         * The context handed to the user processor: forwards are serialized with the sink's
         * serdes, stamped, and routed to the sink node. An unnamed forward goes to every sink,
         * matching Streams' fan-out semantics.
         */
        private final class StampingContext implements ProcessorContext<KO, VO> {

            private final ProcessorContext<byte[], byte[]> inner;

            StampingContext(ProcessorContext<byte[], byte[]> inner) {
                this.inner = inner;
            }

            @Override
            public <K1 extends KO, V1 extends VO> void forward(Record<K1, V1> record) {
                for (String sink : sinks.keySet()) {
                    forwardTo(sink, record);
                }
            }

            @Override
            public <K1 extends KO, V1 extends VO> void forward(Record<K1, V1> record, String childName) {
                if (!sinks.containsKey(childName)) {
                    throw new IllegalArgumentException("unknown sink topic: " + childName
                            + " (forward by sink topic name)");
                }
                forwardTo(childName, record);
            }

            private void forwardTo(String sink, Record<? extends KO, ? extends VO> record) {
                SinkDef<KO, VO> def = sinks.get(sink);
                byte[] key = def.keySerde().serializer().serialize(sink, record.key());
                byte[] value = def.valueSerde().serializer().serialize(sink, record.value());
                var resolved = topicIds.resolve(sink);
                Channel dest = new Channel(resolved.id(), partitionFor(key, resolved.partitions()));
                var headers = new RecordHeaders(record.headers());
                CausalHeaders.write(headers, node.prepareSend(dest));
                inner.forward(new Record<>(key, value, record.timestamp(), headers), SINK_PREFIX + sink);
            }

            // ---- pure delegation below ----

            @Override
            public String applicationId() {
                return inner.applicationId();
            }

            @Override
            public org.apache.kafka.streams.processor.TaskId taskId() {
                return inner.taskId();
            }

            @Override
            public java.util.Optional<org.apache.kafka.streams.processor.api.RecordMetadata> recordMetadata() {
                return inner.recordMetadata();
            }

            @Override
            public Serde<?> keySerde() {
                return inner.keySerde();
            }

            @Override
            public Serde<?> valueSerde() {
                return inner.valueSerde();
            }

            @Override
            public java.io.File stateDir() {
                return inner.stateDir();
            }

            @Override
            public org.apache.kafka.streams.StreamsMetrics metrics() {
                return inner.metrics();
            }

            @Override
            public <S extends org.apache.kafka.streams.processor.StateStore> S getStateStore(String name) {
                return inner.getStateStore(name);
            }

            @Override
            public org.apache.kafka.streams.processor.Cancellable schedule(
                    Duration interval, PunctuationType type,
                    org.apache.kafka.streams.processor.Punctuator callback) {
                return inner.schedule(interval, type, callback);
            }

            @Override
            public org.apache.kafka.streams.processor.Cancellable schedule(
                    java.time.Instant startTime, Duration interval, PunctuationType type,
                    org.apache.kafka.streams.processor.Punctuator callback) {
                return inner.schedule(startTime, interval, type, callback);
            }

            @Override
            public void commit() {
                inner.commit();
            }

            @Override
            public Map<String, Object> appConfigs() {
                return inner.appConfigs();
            }

            @Override
            public Map<String, Object> appConfigsWithPrefix(String prefix) {
                return inner.appConfigsWithPrefix(prefix);
            }

            @Override
            public long currentSystemTimeMs() {
                return inner.currentSystemTimeMs();
            }

            @Override
            public long currentStreamTimeMs() {
                return inner.currentStreamTimeMs();
            }
        }
    }
}
