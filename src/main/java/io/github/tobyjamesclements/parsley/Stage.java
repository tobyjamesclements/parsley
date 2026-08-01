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
import java.util.ArrayList;
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

    /**
     * The tick declaration: the emission interval and the logic, erased. {@code stateful} is
     * whether the logic folds over the tick state slot; a {@link TickHandler} on a stateful
     * stage leaves the slot untouched.
     */
    private record TickSpec(Duration interval, TickFold<Object> fold, boolean stateful) {}

    /** Creates the per-task broker-offsets view; production uses an admin client. */
    interface BrokerOffsetsProvider {
        BrokerOffsets create(TopicIds topicIds, Set<String> sinkTopics);
    }

    private final String name;
    private final Map<String, Source> sources;
    private final Map<String, Topic<?, ?>> sinks;
    private final StateSpec state;
    private final TickSpec ticks;
    private final Duration truncationInterval;
    private final Duration holdWarningAfter;

    private Stage(String name, Map<String, Source> sources, Map<String, Topic<?, ?>> sinks,
                  StateSpec state, TickSpec ticks, Duration truncationInterval,
                  Duration holdWarningAfter) {
        this.name = name;
        this.sources = Map.copyOf(sources);
        this.sinks = Map.copyOf(sinks);
        this.state = state;
        this.ticks = ticks;
        this.truncationInterval = truncationInterval;
        this.holdWarningAfter = holdWarningAfter;
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    /** Builds a stateless stage; {@link #state} upgrades it to a stateful one. */
    public static final class Builder {
        private final String name;
        private final Map<String, Source> sources = new LinkedHashMap<>();
        private final Map<String, Topic<?, ?>> sinks = new LinkedHashMap<>();
        private TickSpec ticks;
        private Duration truncationInterval = Duration.ofMinutes(10);
        private Duration holdWarningAfter = Duration.ofSeconds(30);

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

        /**
         * Declares ticks: the runtime emits one stamped record per interval to the stage's
         * own tick topic ({@code <application.id>-<name>-ticks}, which must exist with the
         * same partition count as the stage's widest source topic), consumes it back through
         * the gate, and hands it to {@code logic} as a {@link Tick}. Callable once.
         */
        public Builder ticks(Duration interval, TickHandler logic) {
            this.ticks = statelessTickSpec(interval, logic, ticks);
            return this;
        }

        private static TickSpec statelessTickSpec(Duration interval, TickHandler logic,
                                                  TickSpec existing) {
            if (existing != null) {
                throw new IllegalStateException("ticks already declared");
            }
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("tick interval must be positive: " + interval);
            }
            return new TickSpec(interval, (s, tick) -> new Step<>(s, logic.onTick(tick)), false);
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Builder truncationInterval(Duration interval) {
            this.truncationInterval = interval;
            return this;
        }

        /**
         * How long a record may gate its channel before the runtime logs a WARN naming the
         * missing cause. Default thirty seconds; zero or negative disables the warning,
         * leaving {@link CausalStreams#explainHolds()} and the hold metrics as the interface.
         * A hold is not an error — the warning says a wait has lasted long enough to be worth
         * a look, and what to look at.
         */
        public Builder holdWarningAfter(Duration threshold) {
            this.holdWarningAfter = threshold;
            return this;
        }

        public Stage build() {
            if (sources.isEmpty()) throw new IllegalStateException("no sources declared");
            return new Stage(name, sources, sinks, null, ticks, truncationInterval,
                    holdWarningAfter);
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

        /**
         * Declares ticks folded over the stage's tick state: one reserved per-partition slot
         * of the stage's state type, distinct from every per-key slot. The runtime emits one
         * stamped record per interval to the stage's own tick topic
         * ({@code <application.id>-<name>-ticks}, which must exist with the same partition
         * count as the stage's widest source topic), consumes it back through the gate, and
         * folds it with {@code logic}. Callable once.
         */
        @SuppressWarnings("unchecked")
        public Stateful<S> ticks(Duration interval, TickFold<S> logic) {
            if (base.ticks != null) {
                throw new IllegalStateException("ticks already declared");
            }
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("tick interval must be positive: " + interval);
            }
            base.ticks = new TickSpec(interval, (TickFold<Object>) logic, true);
            return this;
        }

        /** Declares ticks handled by stateless logic; the tick state slot is untouched. */
        public Stateful<S> ticks(Duration interval, TickHandler logic) {
            base.ticks(interval, logic);
            return this;
        }

        /** How often the log-start truncation sweep runs. Default ten minutes. */
        public Stateful<S> truncationInterval(Duration interval) {
            base.truncationInterval(interval);
            return this;
        }

        /**
         * How long a record may gate its channel before the runtime logs a WARN naming the
         * missing cause. Default thirty seconds; zero or negative disables the warning.
         */
        public Stateful<S> holdWarningAfter(Duration threshold) {
            base.holdWarningAfter(threshold);
            return this;
        }

        public Stage build() {
            if (base.sources.isEmpty()) throw new IllegalStateException("no sources declared");
            return new Stage(base.name, base.sources, base.sinks, state, base.ticks,
                    base.truncationInterval, base.holdWarningAfter);
        }
    }

    String name() {
        return name;
    }

    /** The declared source topics with their typed declarations; the tick topic is not one. */
    Map<String, Topic<?, ?>> declaredSources() {
        Map<String, Topic<?, ?>> byName = new LinkedHashMap<>();
        sources.forEach((topic, source) -> byName.put(topic, source.topic()));
        return byName;
    }

    /** The declared sink topics with their typed declarations. */
    Map<String, Topic<?, ?>> declaredSinks() {
        return sinks;
    }

    boolean hasTicks() {
        return ticks != null;
    }

    Duration tickInterval() {
        return ticks.interval();
    }

    boolean stateful() {
        return state != null;
    }

    /**
     * The protocol store's name. Kafka Streams derives its changelog topic as
     * {@code <application.id>-<store>-changelog}, so a store named for the stage alone puts
     * the changelog beside every other internal topic of the application when a cluster's
     * topics are listed in name order.
     */
    String protocolStoreName() {
        return name + "-state";
    }

    /** The fold store's name; see {@link #protocolStoreName} for why it is stage-relative. */
    String foldStoreName() {
        return name + "-fold";
    }

    private String sourceNode() {
        return "vc-" + name + "-source";
    }

    private String processorNode() {
        return "vc-" + name + "-processor";
    }

    private String sinkNode(String topic) {
        return "vc-" + name + "-sink-" + topic;
    }

    /**
     * The stage's tick topic, qualified by the application it belongs to. Kafka Streams names
     * the topics it creates {@code <application.id>-<name>-<kind>}; the tick topic is the same
     * kind of thing — internal to one application, owned by one stage — so it takes the same
     * shape and sorts into the same block of a topic listing.
     */
    String tickTopicName(String applicationId) {
        return applicationId + "-" + name + "-ticks";
    }

    /** All topics the stage's source node consumes, the tick topic included when declared. */
    Set<String> sourceTopics(String applicationId) {
        if (ticks == null) return sources.keySet();
        Set<String> all = new HashSet<>(sources.keySet());
        all.add(tickTopicName(applicationId));
        return all;
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
    void addToForTest(Topology t, String applicationId) {
        addTo(t, applicationId, topic -> new TopicIds.Resolved(testChannel(topic, 0).topicId(), 1),
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
    void addTo(Topology t, String applicationId, TopicIds topicIds,
               BrokerOffsetsProvider brokerOffsets, boolean testWired) {
        t.addSource(sourceNode(), new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                sourceTopics(applicationId).toArray(String[]::new));
        t.addProcessor(processorNode(),
                new AdapterSupplier(applicationId, topicIds, brokerOffsets, testWired),
                sourceNode());
        if (ticks != null) {
            String tickTopic = tickTopicName(applicationId);
            // A task emits ticks to its own partition, so the tick topic must cover every
            // task: fewer partitions than the widest source and high tasks cannot tick; more
            // and Streams creates ghost tasks consuming nothing but their tick partition.
            int tickPartitions = topicIds.resolve(tickTopic).partitions();
            int taskPartitions = sources.keySet().stream()
                    .mapToInt(topic -> topicIds.resolve(topic).partitions()).max().orElseThrow();
            if (tickPartitions != taskPartitions) {
                throw new IllegalStateException("tick topic " + tickTopic + " has "
                        + tickPartitions + " partitions but the stage's widest source has "
                        + taskPartitions + "; create it with exactly that count");
            }
            // The tick's destination is chosen before stamping (the stamp names the
            // channel), so the record carries it and the sink partitioner reads it back.
            org.apache.kafka.streams.processor.StreamPartitioner<byte[], byte[]> partitioner =
                    (topic, key, value, numPartitions) ->
                            Optional.of(Set.of(tickPartition(key)));
            t.addSink(sinkNode(tickTopic), tickTopic,
                    new ByteArraySerializer(), new ByteArraySerializer(),
                    partitioner, processorNode());
        }
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

    /**
     * Whether a gating head warrants its warning now: the threshold is enabled and elapsed,
     * and this exact head has not been warned about already. One hold warns once — a warning
     * per punctuator tick would bury the diagnosis it is trying to surface — and a new head on
     * the same channel is a new hold, so it warns again.
     *
     * @param threshold the stage's configured age, zero or negative to disable warnings
     * @param heldMs how long the head has gated its channel
     * @param warnedOffset the head offset already warned about on this channel, or null
     * @param headOffset the current head's offset
     */
    static boolean shouldWarn(Duration threshold, long heldMs, Long warnedOffset, long headOffset) {
        if (threshold.isZero() || threshold.isNegative()) return false;
        if (heldMs < threshold.toMillis()) return false;
        return warnedOffset == null || warnedOffset != headOffset;
    }

    /**
     * Decodes a tick record's destination partition from its key: the emitting task's own
     * partition as a big-endian int, written at the stamping site so the sink partitioner
     * lands the tick back on the emitting task's channel.
     */
    static int tickPartition(byte[] key) {
        if (key == null || key.length != 4) {
            throw new IllegalStateException("malformed tick key: expected a 4-byte partition");
        }
        return java.nio.ByteBuffer.wrap(key).getInt();
    }

    private final class AdapterSupplier implements ProcessorSupplier<byte[], byte[], byte[], byte[]> {

        private final String applicationId;
        private final TopicIds topicIds;
        private final BrokerOffsetsProvider brokerOffsets;
        private final boolean testWired;

        private AdapterSupplier(String applicationId, TopicIds topicIds,
                                BrokerOffsetsProvider brokerOffsets, boolean testWired) {
            this.applicationId = applicationId;
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
            return new Adapter(applicationId, topicIds, brokerOffsets, testWired);
        }
    }

    private final class Adapter implements Processor<byte[], byte[], byte[], byte[]> {

        private final String applicationId;
        private final TopicIds topicIds;
        private final BrokerOffsetsProvider brokerOffsets;
        private final boolean testWired;
        private ProcessorContext<byte[], byte[]> context;
        private CausalNode node;
        private StageMetrics metrics;
        private KeyValueStore<Bytes, byte[]> foldStore;
        private Channel tickChannel;
        private final Map<String, Channel> channelByTopic = new HashMap<>();
        private final Map<Channel, String> topicByChannel = new HashMap<>();
        /** Highest offset fed through {@code onRecord} per channel, this task incarnation. */
        private final Map<Channel, Long> fedThrough = new HashMap<>();
        /** Head offset already warned about per channel, so one hold warns once. */
        private final Map<Channel, Long> warnedHeads = new HashMap<>();
        private String taskKey;
        private int taskPartition;
        private boolean positionCaptureChecked;

        private Adapter(String applicationId, TopicIds topicIds,
                        BrokerOffsetsProvider brokerOffsets, boolean testWired) {
            this.applicationId = applicationId;
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
                    throw new IllegalStateException("processing.guarantee must be "
                            + StreamsConfig.EXACTLY_ONCE_V2 + " but the task runs with " + guarantee
                            + "; start the application through " + Parsley.class.getSimpleName()
                            + ".streams (fail closed)");
                }
            }

            Set<Channel> consumed = new HashSet<>();
            for (String topic : sourceTopics(applicationId)) {
                Channel c = new Channel(topicIds.resolve(topic).id(), taskPartition);
                consumed.add(c);
                channelByTopic.put(topic, c);
                topicByChannel.put(c, topic);
                // A view captured by a previous incarnation of this task on this thread pairs
                // a position with another consumer session's returned records; only views this
                // incarnation's polls capture may feed the sweep.
                Positions.forCurrentThread().remove(new TopicPartition(topic, taskPartition));
            }
            Set<String> sinkTopics = new HashSet<>(sinks.keySet());
            if (ticks != null) {
                tickChannel = channelByTopic.get(tickTopicName(applicationId));
                // The tick channel is both consumed and produced: the end-offset seed must
                // cover it so a restart's stamps still claim the prior incarnation's ticks.
                sinkTopics.add(tickTopicName(applicationId));
            }
            Set<UUID> sinkIds = new HashSet<>();
            for (String topic : sinkTopics) {
                sinkIds.add(topicIds.resolve(topic).id());
            }

            KeyValueStore<Bytes, byte[]> kv = context.getStateStore(protocolStoreName());
            if (state != null) {
                foldStore = context.getStateStore(foldStoreName());
            }
            BrokerOffsets offsets = brokerOffsets.create(topicIds, sinkTopics);
            // Sender identity must survive topology evolution: task ids embed the sub-topology
            // index, which renumbers when stages are added, so derive from the user-stable
            // stage name and the partition instead.
            UUID senderId = UUID.nameUUIDFromBytes(
                    (context.applicationId() + "/" + name + "/" + taskPartition).getBytes());
            node = new CausalNode(
                    new NodeConfig(name + "-task-" + context.taskId(), senderId, consumed, sinkIds,
                            taskPartition),
                    new KafkaStateStore(kv), offsets);
            // The channel ids in protocol log lines decode through this mapping.
            LOG.info("{}: task {} initialised: sender {}, sources {}, sinks {}",
                    name, context.taskId(), senderId, channelByTopic, sinks.keySet());
            metrics = new StageMetrics(context.metrics(), name, context.taskId().toString(),
                    topicByChannel, node);

            this.taskKey = context.taskId().toString();
            context.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME,
                    ts -> {
                        sweepPositions();
                        metrics.sample(ts);
                        // Publishing after sampling reuses the metrics' head-age marks, so the
                        // gauge, the snapshot, and the warning cannot disagree.
                        publishHolds();
                    });
            if (ticks != null) {
                context.schedule(ticks.interval(), PunctuationType.WALL_CLOCK_TIME, this::emitTick);
            }
            // The coordination-free truncation driver: log starts are a true global stability
            // bound (retention-deleted records sit below every reachable baseline). A failed
            // sweep skips a cycle; it must never fail the task for garbage collection.
            context.schedule(truncationInterval, PunctuationType.WALL_CLOCK_TIME, ts -> {
                try {
                    var earliest = offsets.earliestOffsets(node.stampChannels());
                    node.truncateToLogStarts(earliest.logStarts(), earliest.confirmedAbsent());
                } catch (RuntimeException e) {
                    metrics.recordSweepSkipped();
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
                            + " topology is not running under " + Parsley.class.getSimpleName()
                            + ".streams (fail closed)");
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

        /**
         * Snapshots why each gating head is waiting, publishes it for
         * {@link CausalStreams#explainHolds()}, and warns once per head that outlives the
         * stage's threshold. Built on the stream thread from the node's read-only observation
         * surface; the published list is immutable and payload-free.
         */
        private void publishHolds() {
            List<CausalNode.HeadHold> heads = node.explainHeads();
            List<HeldRecord> snapshot = new ArrayList<>(heads.size());
            for (CausalNode.HeadHold head : heads) {
                long heldMs = metrics.heldMs(head.channel());
                List<HeldRecord.Unmet> unmet = new ArrayList<>(head.unmet().size());
                for (CausalNode.UnmetClaim claim : head.unmet()) {
                    unmet.add(describe(claim));
                }
                HeldRecord record = new HeldRecord(name, taskKey,
                        topicByChannel.get(head.channel()), taskPartition, head.offset(),
                        head.queueDepth(), heldMs, unmet);
                snapshot.add(record);
                warnIfOverdue(head.channel(), record);
            }
            warnedHeads.keySet().retainAll(
                    heads.stream().map(CausalNode.HeadHold::channel).collect(java.util.stream.Collectors.toSet()));
            Holds.publish(applicationId, taskKey, snapshot);
        }

        /** Classifies one unmet claim and renders it with topic names instead of channel ids. */
        private HeldRecord.Unmet describe(CausalNode.UnmetClaim claim) {
            HeldRecord.Diagnosis diagnosis;
            if (claim.sender() != null) {
                diagnosis = claim.deliveredSeq() < 0
                        ? HeldRecord.Diagnosis.SENDER_UNSEEN
                        : HeldRecord.Diagnosis.SENDER_BEHIND;
            } else {
                // The position is the dividing line: everything strictly below it has been
                // fetched here or consumer-skipped, so a claim below it names a record that
                // arrived and is itself held, not one still in flight.
                diagnosis = claim.localPosition() <= claim.claimedOffset()
                        ? HeldRecord.Diagnosis.NOT_FETCHED
                        : HeldRecord.Diagnosis.HELD_UPSTREAM;
            }
            String topic = topicByChannel.get(claim.channel());
            return new HeldRecord.Unmet(diagnosis,
                    topic == null ? claim.channel().toString() : topic,
                    claim.channel().partition(), claim.claimedOffset(), claim.localFrontier(),
                    claim.localPosition(), claim.sender() == null ? null : claim.sender().toString(),
                    claim.claimedSeq(), claim.deliveredSeq());
        }

        /**
         * Warns once per head that has gated its channel for longer than the threshold. A
         * hold is not an error and the gate is not misbehaving, so this is WARN and not ERROR:
         * it says a wait has lasted long enough to be worth a look, and names what to look at.
         */
        private void warnIfOverdue(Channel channel, HeldRecord record) {
            if (!shouldWarn(holdWarningAfter, record.heldMs(), warnedHeads.get(channel),
                    record.offset())) {
                return;
            }
            warnedHeads.put(channel, record.offset());
            LOG.warn("{}: {}", record.unmet().get(0).diagnosis().code(), record.summary());
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

        /**
         * Emits one tick: stamped at the existing door with the task's current clock (a
         * consistent cut of its consumed history) and forwarded to the tick sink, which
         * routes it back to this task's own partition via the key. The record commits with
         * the current transaction and returns through the gate like any other record.
         */
        private void emitTick(long ts) {
            var headers = new RecordHeaders();
            CausalHeaders.write(headers, node.prepareSend(tickChannel));
            byte[] key = java.nio.ByteBuffer.allocate(4).putInt(taskPartition).array();
            context.forward(new Record<>(key, null, ts, headers),
                    sinkNode(tickTopicName(applicationId)));
            metrics.recordTickEmitted();
        }

        private void deliverAll(List<Delivery> deliveries) {
            if (!deliveries.isEmpty()) metrics.recordDelivered(deliveries.size());
            for (Delivery d : deliveries) {
                if (d.channel().equals(tickChannel)) {
                    deliverTick(d);
                    continue;
                }
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

        /** The reserved tick state slot, in its own namespace beside the key prefixes. */
        private static final Bytes TICK_STATE_KEY = Bytes.wrap(new byte[] {2});

        /**
         * Delivers one tick to the stage's tick logic; a stateful tick fold steps the
         * reserved per-partition slot exactly as a key fold steps its key's state.
         */
        private void deliverTick(Delivery d) {
            Tick tick = new Tick(d.timestamp());
            Step<Object> step;
            if (ticks.stateful()) {
                byte[] prior = foldStore.get(TICK_STATE_KEY);
                Object before = prior == null ? state.initial().get() : state.codec().decode(prior);
                step = ticks.fold().apply(before, tick);
                if (step.state() == null) {
                    foldStore.delete(TICK_STATE_KEY);
                } else {
                    foldStore.put(TICK_STATE_KEY, state.codec().encode(step.state()));
                }
            } else {
                step = ticks.fold().apply(null, tick);
            }
            for (Emission emission : step.emissions()) {
                apply(emission, d.timestamp());
            }
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
                        + " (declare it with " + Stage.class.getSimpleName() + "."
                        + Builder.class.getSimpleName() + ".into; declared: " + sinks.keySet() + ")");
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

        @Override
        public void close() {
            // Tasks migrate; sensors and published snapshots left behind would accumulate on
            // the instance and describe a task this instance no longer runs.
            if (metrics != null) metrics.close();
            if (applicationId != null) Holds.clear(applicationId, taskKey);
        }
    }
}
