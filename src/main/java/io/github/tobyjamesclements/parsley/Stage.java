package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * One causal processing stage: typed source topics, each handled by pure user logic (a
 * {@link Handler} or, with per-key state, a {@link Fold}), and typed sink topics — with the
 * causal boundary in between. The protocol gates and orders deliveries on the source
 * side; the single stamping site applies the returned emissions on the sink side.
 *
 * <p>The functional core is the user's half: logic is a pure function from a delivered
 * {@link Message} to emissions (and, for folds, the next state), so it holds no references
 * to any runtime and is unit-testable with plain equality. The imperative edge is this
 * class's half: gating, decoding, state persistence, stamping, partitioning, and the
 * transaction, all inside the stage's adapter.
 *
 * <p>Serialization happens exactly once on each side, inside the stage: inbound bytes are
 * held verbatim while gated and decoded at delivery with the source topic's codecs;
 * emissions are encoded with their topic's codecs at the stamping site, so the clock travels
 * with the exact bytes it claims. Non-Parsley headers of a held record are not carried
 * through delivery.
 *
 * <p>Stages are immutable and compose through {@link Parsley}; a stage corresponds to one
 * Kafka Streams sub-topology, and its name keys its state stores, so the name must stay
 * stable across deployments of the same application.
 */
public final class Stage {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Stage.class);

    /** A source topic with its logic, erased to the adapter's runtime types. */
    private record Source(Topic<Object, Object> topic, Fold<Object, Object, Object> fold) {}

    /** The per-key state declaration of a stateful stage, erased. */
    private record StateSpec(Codec<Object> codec, Supplier<Object> initial) {}

    /** Creates the per-task broker-offsets view; production uses an admin client. */
    interface BrokerOffsetsProvider {
        BrokerOffsets create(TopicIds topicIds, Set<String> sinkTopics);
    }

    private final String name;
    private final Map<String, Source> sources;
    private final Map<String, Topic<?, ?>> sinks;
    private final StateSpec state;
    private final Duration truncationInterval;

    private Stage(String name, Map<String, Source> sources, Map<String, Topic<?, ?>> sinks,
                  StateSpec state, Duration truncationInterval) {
        this.name = name;
        this.sources = Map.copyOf(sources);
        this.sinks = Map.copyOf(sinks);
        this.state = state;
        this.truncationInterval = truncationInterval;
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    /** Builds a stateless stage; {@link #state} upgrades it to a stateful one. */
    public static final class Builder {
        private final String name;
        private final Map<String, Source> sources = new LinkedHashMap<>();
        private final Map<String, Topic<?, ?>> sinks = new LinkedHashMap<>();
        private Duration truncationInterval = Duration.ofMinutes(10);

        private Builder(String name) {
            if (!name.matches("[a-zA-Z0-9-]+")) {
                throw new IllegalArgumentException("stage name must be [a-zA-Z0-9-]+: " + name);
            }
            this.name = name;
        }

        /** Declares a source topic handled by stateless logic. */
        @SuppressWarnings("unchecked")
        public <K, V> Builder on(Topic<K, V> topic, Handler<K, V> handler) {
            Fold<Object, Object, Object> erased =
                    (s, m) -> new Step<>(s, ((Handler<Object, Object>) handler).handle(m));
            if (sources.put(topic.name(), new Source((Topic<Object, Object>) topic, erased)) != null) {
                throw new IllegalArgumentException("duplicate source topic: " + topic.name());
            }
            return this;
        }

        /**
         * Declares the stage's per-key state: its codec and the initial value a fold sees
         * for an unseen key. Callable once; sources declared before it stay stateless.
         */
        @SuppressWarnings("unchecked")
        public <S> Stateful<S> state(Codec<S> codec, Supplier<S> initial) {
            return new Stateful<>(this,
                    new StateSpec((Codec<Object>) codec, (Supplier<Object>) initial));
        }

        /** Declares sink topics; emissions may only name declared sinks. */
        public Builder into(Topic<?, ?>... topics) {
            for (Topic<?, ?> t : topics) {
                sinks.put(t.name(), t);
            }
            return this;
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Builder truncationInterval(Duration interval) {
            this.truncationInterval = interval;
            return this;
        }

        public Stage build() {
            if (sources.isEmpty()) throw new IllegalStateException("no sources declared");
            return new Stage(name, sources, sinks, null, truncationInterval);
        }
    }

    /** Builds a stateful stage: sources may fold over the declared per-key state. */
    public static final class Stateful<S> {
        private final Builder base;
        private final StateSpec state;

        private Stateful(Builder base, StateSpec state) {
            this.base = base;
            this.state = state;
        }

        /** Declares a source topic folded over the stage's per-key state. */
        @SuppressWarnings("unchecked")
        public <K, V> Stateful<S> on(Topic<K, V> topic, Fold<S, K, V> fold) {
            var erased = (Fold<Object, Object, Object>) fold;
            if (base.sources.put(topic.name(),
                    new Source((Topic<Object, Object>) topic, erased)) != null) {
                throw new IllegalArgumentException("duplicate source topic: " + topic.name());
            }
            return this;
        }

        /** Declares a source topic handled by stateless logic; the state is untouched. */
        public <K, V> Stateful<S> on(Topic<K, V> topic, Handler<K, V> handler) {
            base.on(topic, handler);
            return this;
        }

        /** Declares sink topics; emissions may only name declared sinks. */
        public Stateful<S> into(Topic<?, ?>... topics) {
            base.into(topics);
            return this;
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Stateful<S> truncationInterval(Duration interval) {
            base.truncationInterval(interval);
            return this;
        }

        public Stage build() {
            if (base.sources.isEmpty()) throw new IllegalStateException("no sources declared");
            return new Stage(base.name, base.sources, base.sinks, state, base.truncationInterval);
        }
    }

    String name() {
        return name;
    }

    String protocolStoreName() {
        return "parsley-" + name + "-state";
    }

    private String foldStoreName() {
        return "parsley-" + name + "-fold";
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

    /** The channel the test wiring resolves for a topic-partition. */
    static Channel testChannel(String topic, int partition) {
        return new Channel(UUID.nameUUIDFromBytes(topic.getBytes()), partition);
    }

    /**
     * Broker-less wiring for {@code TopologyTestDriver}: topic identity is synthesized
     * deterministically, every topic has one partition (the driver's reality), and the offset
     * queries answer empty — safe precisely because no broker means no prior incarnations to
     * seed against and nothing to truncate.
     */
    void addToForTest(Topology t) {
        addTo(t, topic -> new TopicIds.Resolved(testChannel(topic, 0).topicId(), 1),
                (ids, sinkTopics) -> new BrokerOffsets() {
                    @Override
                    public Map<Channel, Long> endOffsets(Set<UUID> topicIds) {
                        return Map.of();
                    }

                    @Override
                    public EarliestOffsets earliestOffsets(Set<Channel> channels) {
                        return new EarliestOffsets(Map.of(), Set.of());
                    }
                }, true);
    }

    /**
     * Adds this stage's nodes to a topology with the given wiring. The wiring is captured by
     * the node suppliers rather than stored on the stage, so a stage is immutable and every
     * assembled topology is self-contained.
     */
    void addTo(Topology t, TopicIds topicIds, BrokerOffsetsProvider brokerOffsets, boolean testWired) {
        t.addSource(sourceNode(), new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                sources.keySet().toArray(String[]::new));
        t.addProcessor(processorNode(), new AdapterSupplier(topicIds, brokerOffsets, testWired),
                sourceNode());
        for (String sink : sinks.keySet()) {
            // The adapter partitions before stamping (sequence claims are per channel), so the
            // sink must land each record on the partition the adapter chose. The partitioner
            // recomputes the same deterministic function over the same cached partition count.
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

        private final TopicIds topicIds;
        private final BrokerOffsetsProvider brokerOffsets;
        private final boolean testWired;

        private AdapterSupplier(TopicIds topicIds, BrokerOffsetsProvider brokerOffsets,
                                boolean testWired) {
            this.topicIds = topicIds;
            this.brokerOffsets = brokerOffsets;
            this.testWired = testWired;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            Set<StoreBuilder<?>> all = new HashSet<>();
            all.add(Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(protocolStoreName()),
                            Serdes.Bytes(), Serdes.ByteArray())
                    .withCachingDisabled());
            if (state != null) {
                all.add(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(foldStoreName()),
                                Serdes.Bytes(), Serdes.ByteArray())
                        .withCachingDisabled());
            }
            return all;
        }

        @Override
        public Processor<byte[], byte[], byte[], byte[]> get() {
            return new Adapter(topicIds, brokerOffsets, testWired);
        }
    }

    private final class Adapter implements Processor<byte[], byte[], byte[], byte[]> {

        private final TopicIds topicIds;
        private final BrokerOffsetsProvider brokerOffsets;
        private final boolean testWired;
        private ProcessorContext<byte[], byte[]> context;
        private CausalNode node;
        private KeyValueStore<Bytes, byte[]> foldStore;
        private final Map<String, Channel> channelByTopic = new HashMap<>();
        private final Map<Channel, String> topicByChannel = new HashMap<>();
        /** Highest offset fed through {@code onRecord} per channel, this task incarnation. */
        private final Map<Channel, Long> fedThrough = new HashMap<>();
        private int taskPartition;
        private boolean positionCaptureChecked;

        private Adapter(TopicIds topicIds, BrokerOffsetsProvider brokerOffsets, boolean testWired) {
            this.topicIds = topicIds;
            this.brokerOffsets = brokerOffsets;
            this.testWired = testWired;
        }

        @Override
        public void init(ProcessorContext<byte[], byte[]> context) {
            this.context = context;
            this.taskPartition = context.taskId().partition();

            if (!testWired) {
                Object guarantee = context.appConfigs().get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
                if (!StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
                    throw new IllegalStateException("parsley requires processing.guarantee="
                            + StreamsConfig.EXACTLY_ONCE_V2 + " but the task runs with " + guarantee
                            + "; start the application through Parsley.streams (fail closed)");
                }
            }

            Set<Channel> consumed = new HashSet<>();
            for (String topic : sources.keySet()) {
                Channel c = new Channel(topicIds.resolve(topic).id(), taskPartition);
                consumed.add(c);
                channelByTopic.put(topic, c);
                topicByChannel.put(c, topic);
                // A view captured by a previous incarnation of this task on this thread pairs
                // a position with another consumer session's returned records; only views this
                // incarnation's polls capture may feed the sweep.
                Positions.forCurrentThread().remove(new TopicPartition(topic, taskPartition));
            }
            Set<UUID> sinkIds = new HashSet<>();
            for (String topic : sinks.keySet()) {
                sinkIds.add(topicIds.resolve(topic).id());
            }

            KeyValueStore<Bytes, byte[]> kv = context.getStateStore(protocolStoreName());
            if (state != null) {
                foldStore = context.getStateStore(foldStoreName());
            }
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
            if (!testWired && !positionCaptureChecked) {
                // Liveness self-check: without the position-capturing client supplier, held
                // records are never released. The capture map fills on the task's first poll,
                // which necessarily precedes its first record.
                if (Positions.forCurrentThread().isEmpty()) {
                    throw new IllegalStateException("no captured consumer positions: this"
                            + " topology is not running under Parsley.streams (fail closed)");
                }
                positionCaptureChecked = true;
            }
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
            fedThrough.put(c, meta.offset());
            deliverAll(node.onRecord(new InboundRecord(
                    c, meta.offset(), clock, sender, sender == null ? -1 : seq,
                    r.key(), r.value(), r.timestamp())));
            sweepPositions();
        }

        private void sweepPositions() {
            Map<TopicPartition, Positions.PollView> positions = Positions.forCurrentThread();
            for (Map.Entry<String, Channel> e : channelByTopic.entrySet()) {
                Positions.PollView view = positions.get(new TopicPartition(e.getKey(), taskPartition));
                if (view == null) continue;
                // A post-poll position runs ahead of returned records still buffered between
                // poll and process; positionAdvance asserts everything below is fed or
                // consumer-skipped, so reporting early would jump the frontier past the
                // buffered records and drop them as replays. Report only once every returned
                // record at or below the position has been fed.
                if (view.lastReturned() > fedThrough.getOrDefault(e.getValue(), -1L)) continue;
                deliverAll(node.positionAdvance(e.getValue(), view.position()));
            }
        }

        private void deliverAll(List<Delivery> deliveries) {
            for (Delivery d : deliveries) {
                String topic = topicByChannel.get(d.channel());
                Source source = sources.get(topic);
                Object key = d.key() == null ? null : source.topic().keyCodec().decode(d.key());
                Object value = d.value() == null ? null : source.topic().valueCodec().decode(d.value());
                var message = new Message<>(topic, taskPartition, d.offset(), d.timestamp(), key, value);
                Step<Object> step = state == null
                        ? source.fold().apply(null, message)
                        : foldOverState(source, d.key(), message);
                for (Emission emission : step.emissions()) {
                    apply(emission, d.timestamp());
                }
            }
        }

        /**
         * Applies one fold step: resolve the key's state (initial when absent), apply, persist.
         * The state key is the inbound key's encoded bytes prefixed with a presence byte, so a
         * null key folds under its own state without colliding with an empty key.
         */
        private Step<Object> foldOverState(Source source, byte[] rawKey, Message<Object, Object> message) {
            Bytes stateKey = stateKey(rawKey);
            byte[] prior = foldStore.get(stateKey);
            Object before = prior == null ? state.initial().get() : state.codec().decode(prior);
            Step<Object> step = source.fold().apply(before, message);
            if (step.state() == null) {
                foldStore.delete(stateKey);
            } else {
                foldStore.put(stateKey, state.codec().encode(step.state()));
            }
            return step;
        }

        private Bytes stateKey(byte[] rawKey) {
            if (rawKey == null) return Bytes.wrap(new byte[] {0});
            byte[] prefixed = new byte[rawKey.length + 1];
            prefixed[0] = 1;
            System.arraycopy(rawKey, 0, prefixed, 1, rawKey.length);
            return Bytes.wrap(prefixed);
        }

        /**
         * The stamping door: encode with the emission's topic codecs, partition
         * deterministically, stamp, and forward to the declared sink node. Emissions to
         * undeclared sinks fail loudly.
         */
        @SuppressWarnings("unchecked")
        private void apply(Emission emission, long deliveredTimestamp) {
            String sink = emission.topic().name();
            if (!sinks.containsKey(sink)) {
                throw new IllegalArgumentException("emission to undeclared sink topic: " + sink
                        + " (declare it with Stage.Builder.into; declared: " + sinks.keySet() + ")");
            }
            Topic<Object, Object> topic = (Topic<Object, Object>) emission.topic();
            byte[] key = emission.key() == null ? null : topic.keyCodec().encode(emission.key());
            byte[] value = emission.value() == null ? null : topic.valueCodec().encode(emission.value());
            var resolved = topicIds.resolve(sink);
            Channel dest = new Channel(resolved.id(), partitionFor(key, resolved.partitions()));
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, node.prepareSend(dest));
            long timestamp = emission.timestamp() == Emission.INHERIT_TIMESTAMP
                    ? deliveredTimestamp
                    : emission.timestamp();
            context.forward(new Record<>(key, value, timestamp, headers), sinkNode(sink));
        }
    }
}
