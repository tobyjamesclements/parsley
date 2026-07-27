package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * A Decorator over a user {@link Processor}, gating delegation on the causal frontier: an incoming
 * record is held until the frontier dominates its dependencies, then delivered to
 * {@code delegate.process(...)} exactly once. Delivery is fail-closed, with no eviction, buffer
 * limit, or timeout that forwards a record ahead of its dependencies, and every forward is stamped by
 * a {@link ParsleyProcessorContext} through {@link ParsleyCausalBroadcast#broadcast}.
 *
 * <p>Held records are persisted to a changelog-backed buffer and restored on {@code init}, so they
 * survive a restart; the frontier-before-forward ordering holds on both the admit and punctuator
 * paths.
 *
 * <p>A received null message ({@link #handleNullMessage}) is relayed downstream only when its carried
 * clock advanced this node's knowledge of a channel it consumes (the relay rule on
 * {@link ParsleyGossip}), never merely because a record was delivered. A delivered business record,
 * by contrast, always causes this node to emit on its own sink (see {@link #deliver}). That is what
 * lets a topology cycle quiesce.
 *
 * @param <KIn>  the input key type
 * @param <VIn>  the input value type
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class ParsleyProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyProcessor.class);

    private static final Duration METRICS_REFRESH_INTERVAL = Duration.ofSeconds(5);

    // Kafka's default producer delivery.timeout.ms, used when the app config does not set one.
    private static final long DEFAULT_DELIVERY_TIMEOUT_MS = 120_000L;

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final Set<String> topics;
    // The topics this stage produces. Feeds the partition-count parity check.
    private final Set<String> sinkTopics;
    // Every child node this processor forwards a control-plane record (a null message) to by
    // name — a stage's processor node addresses every such forward explicitly rather than a zero-arg
    // broadcast. See ParsleyProcessorContext.forward.
    private final List<String> sinkNodeNames;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final @Nullable ParsleyQuiesce quiesce;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Source topic name -> stable UUID, resolved from the broker at init() (the topology decorator
    // has no broker config until then). Used by ingest() to stamp each record's causal identity.
    private Map<String, Uuid> topicUuids = Map.of();
    // This stage's own sink topics, name -> UUID, strictly resolved at init() (see
    // resolveSinkTopicUuids: an unresolvable sink fails init, so every declared sink is present) —
    // the UUIDs feed causalBroadcast() so ParsleyCausalBroadcast can strip a node's own
    // produced coordinates from any inbound dependency/marker clock, and the name keys translate the
    // producer-ack registry's topic names into UUID identity for the ownOutputs fold. Never used
    // to route or gate an inbound record by itself.
    private Map<String, Uuid> sinkTopicUuids = Map.of();
    // Each declared sink topic's per-partition end offsets, captured at init() alongside the UUID
    // resolution (same admin session, same strictness: an unreadable sink fails init) — the
    // ownOutputs seed claims endOffset - 1, the sink's last appended position, per partition
    // (an over-claim that names a real appended position, so it can only delay a downstream
    // delivery, and it heals the frontier value trailing the last transaction's acks).
    private Map<String, Map<Integer, Long>> sinkEndOffsets = Map.of();
    // The effective producer delivery.timeout.ms, resolved at init(). Bounds the crossing wait (a
    // send unacked past it has failed — the wait must throw) and doubles as the stall
    // threshold (past it, no in-flight upstream send can still land at the claimed position, so a
    // hold above highestReceived is a genuine stall, not latency). Leaning on the existing producer
    // config rather than a new knob — its deadline is exactly the boundary both uses care about.
    private long deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS;
    // The instance-wide topic-identity watch, resolved from config at init() like
    // the own-output registry; null when no CausalStreams instance minted one (TopologyTestDriver
    // runs, low-level supplier wirings) — every check is then a no-op. Consulted before ingesting
    // each record and before every stamped forward, so a detected mid-run topic recreation fails
    // the task (and its EOS transaction) fast instead of mislabelling coordinates.
    private @Nullable ParsleyTopicIdentityWatch identityWatch;

    private ProcessorContext<KOut, VOut> context;
    // The task's one causal-broadcast core, built once at init() over the task's state stores. Exactly
    // one Processor instance ever touches these stores within a task, so the cached ParsleyChannels's
    // in-memory copy of the persisted state cannot diverge from a concurrent writer — there is none.
    private ParsleyCausalBroadcast<KIn, VIn> causalBroadcast;
    // The task's L3 gossip module, built at init() over the same core: receives null messages
    // (handleNullMessage) and builds this node's own (advertise). Owns the relay rule and the
    // null-message destination set (every declared sink at this task's own partition).
    private ParsleyGossip<KIn, VIn> gossip;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> candidateIndexStore;
    private KeyValueStore<byte[], byte[]> forwardedIndexStore;
    private ParsleyMetrics.Wired wiredMetrics;
    // Set once delegate.init() has returned, so close() closes the delegate only if it was actually
    // initialised. init() can throw before delegate.init(), after which Streams still calls close();
    // closing an un-inited delegate
    // — or dereferencing the still-null wiredMetrics — would mask the real init failure. See close().
    private boolean delegateInitialized;
    // The stamping proxy context handed to the delegate. Held here so deliver() can check the
    // per-record forward count and emit a null message when the delegate forwarded nothing. The stamp
    // itself comes from causalBroadcast.broadcast() — the single stamping site, read live at forward
    // time — confined to the task thread (both the delegate's forwards and its punctuator fires run on
    // the StreamThread).
    private ParsleyProcessorContext<KOut, VOut> stampingContext;
    private volatile @Nullable RecordMetadata deliveryMetadata;
    private Cancellable restoredDrainSchedule;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     Set<String> topics,
                     Set<String> sinkTopics,
                     List<String> sinkNodeNames,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     @Nullable ParsleyQuiesce quiesce) {
        this.delegate = delegate;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.topics = topics;
        this.sinkTopics = sinkTopics;
        this.sinkNodeNames = sinkNodeNames;
        this.adminFactory = adminFactory;
        this.quiesce = quiesce;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.topicUuids = resolveTopicUuids(context);
        registerIdentityExpectations();
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.candidateIndexStore = context.getStateStore(candidateIndexStoreName);
        this.forwardedIndexStore = context.getStateStore(forwardedIndexStoreName);

        boolean restored = frontierStore.get(ParsleyStores.FRONTIER_KEY) != null;

        this.wiredMetrics = ParsleyMetrics.wire(context);

        // The crossing-wait bound and stall threshold, and every null-message forward's destination
        // set — both fixed for the task's lifetime, resolved before the causal-broadcast core binds
        // them. The destination set — every declared sink at this task's own partition
        // (ParsleyMarkerPartition routes every null message there) — is excluded from the null
        // message's stamp crossing wait: same-coordinate pending sends are covered by partition FIFO
        // and per-producer stamp monotonicity, and the cross-sink exemption is safe only because a
        // null message's destination set is known exactly at stamp time. Business
        // forwards never get an exclusion (their destination partition is unknowable at stamp time;
        // see ParsleyCausalBroadcast#broadcast).
        this.deliveryTimeoutMs = deliveryTimeoutMs(context.appConfigs());
        Set<TopicPartition> destinations = new HashSet<>();
        for (String sink : sinkTopics) {
            destinations.add(new TopicPartition(sink, context.taskId().partition()));
        }

        // Build the task's one causal-broadcast core: constructs the real ParsleyChannels (restoring from the store if
        // restored is true), prunes/seeds it to this task's current scope, and persists. Cached for the
        // processor's whole lifetime — see buildCausalBroadcast(). The L3 gossip module layers over
        // the same core and channel state.
        this.causalBroadcast = buildCausalBroadcast();
        ParsleyCausalBroadcast<KIn, VIn> causalBroadcast = this.causalBroadcast;
        // The consumed scope doubles as the relay trigger: only a carried-clock advance on a
        // coordinate this task consumes obliges a relay (ParsleyGossip's class Javadoc, single
        // home of the rule).
        this.gossip = new ParsleyGossip<>(causalBroadcast, destinations, consumedScope());
        if (restored) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), causalBroadcast.frontier());
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }

        this.stampingContext = new ParsleyProcessorContext<>(
                context, causalBroadcast, () -> Optional.ofNullable(deliveryMetadata), sinkNodeNames,
                this::ensureTopicIdentityIntact);
        delegate.init(stampingContext);
        this.delegateInitialized = true;

        // Drain any records that became satisfiable between the last committed frontier and the last
        // committed buffer-removal (the at-least-once window) once, against the buffer restored from a
        // changelog. Must run as a punctuation, not inline here: Streams hasn't finished wiring the
        // task's RecordCollector until every processor in the topology returns from init(), so
        // forward() during init() throws NPE.
        restoredDrainSchedule = context.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    restoredDrainSchedule.cancel();
                    ParsleyCausalBroadcast.Outcome<KIn, VIn> outcome = causalBroadcast().drainAfterRestore();
                    deliver(outcome.delivered());
                });

        // Refreshes the oldest-record gauge independent of buffer traffic, so it stays current on a
        // buffer that sits idle between admits and releases, which has no other periodic tick at all.
        // Also re-pushes this task's quiesce-drained state: without
        // this, a task whose buffer emptied before requestQuiesce() was ever called would report
        // drained=false forever — updateQuiesceState() only runs on a depth-changing event, and once the
        // buffer is idle there is no such event left to re-evaluate isQuiesceRequested() && empty after
        // the request actually arrives — hanging CausalStreams#close()'s wait indefinitely despite the
        // buffer genuinely being empty. This tick closes that gap within one refresh interval.
        context.schedule(METRICS_REFRESH_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    causalBroadcast().reportBufferState();
                    // The stalled-dependency scan (O(buffer × deps)) rides the same tick, never
                    // the hot delivery path; the threshold is the producer delivery timeout — past
                    // it, no in-flight send can still land at a claimed-but-never-received position.
                    causalBroadcast().reportHeldDependencyStalls(deliveryTimeoutMs);
                    updateQuiesceState();
                });

        // Registered last, once init() has otherwise succeeded, so a failed init never leaves a
        // phantom task permanently blocking ParsleyQuiesce#isSafeToClose.
        if (quiesce != null) {
            quiesce.register(context.taskId().toString());
        }
        // Report the restored buffer's drained state once up front, so a task that starts idle with an
        // empty buffer is known-drained to quiesce and to leave() without waiting for the first delivery.
        updateQuiesceState();
    }

    /**
     * The task's one {@link ParsleyCausalBroadcast}, built by {@link #buildCausalBroadcast()} at {@code init()} and
     * cached for the processor's whole lifetime.
     *
     * <p>Caching is sound because exactly one {@link Processor} instance ever touches this task's
     * causal state stores — there is no other processor node sharing them. Rebuilding the core at
     * the top of every operation to keep hypothetical sharers coherent — a full buffer scan,
     * candidate re-index, and frontier-blob re-persist each time — would make every operation
     * O(buffer-depth) for a sharer that cannot exist.
     */
    private ParsleyCausalBroadcast<KIn, VIn> causalBroadcast() {
        return causalBroadcast;
    }

    /**
     * The coordinates this task consumes: a registered input topic, on the partition this task
     * owns (Streams co-partitions a sub-topology's sources, so the task owns partition
     * {@code taskId().partition()} of every input). Derived, never persisted, so it is recomputed
     * identically after a rebalance. Two consumers, deliberately the same predicate: restore-time
     * pruning in {@link #buildCausalBroadcast()} (not a delivery filter — the gate waits for every
     * channel; see {@code vectorTime()}), and the relay trigger scope handed to
     * {@link ParsleyGossip} — a foreign partition of a consumed topic is a sibling task's scope,
     * never fetched here, so a carried claim on it must fold without obliging a relay.
     */
    private ParsleyVectorClock.CoordinatePredicate consumedScope() {
        Set<Uuid> consumedTopicIds = Set.copyOf(topicUuids.values());
        int taskPartition = context.taskId().partition();
        return (topicId, partition) -> partition == taskPartition && consumedTopicIds.contains(topicId);
    }

    /**
     * Builds the causal-broadcast core over this task's state stores: constructs the {@link ParsleyChannels}
     * (restoring the frontier clock and channel clocks from the {@code "frontier"} value when
     * present), prunes restored state to this task's current scope, seeds a channel entry for every
     * consumed input, and wires the buffer, candidate index, and forwarded index. Called exactly once,
     * from {@link #init}; {@link #wiredMetrics} must already be set.
     */
    private ParsleyCausalBroadcast<KIn, VIn> buildCausalBroadcast() {
        ParsleyBufferStore<KIn, VIn> buffer = new StoreBackedBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new StoreBackedCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new StoreBackedForwardedIndex(forwardedIndexStore);
        // The single owner of the persisted causal metadata: loads the frontier clock and channel
        // clocks from key "frontier" of the frontier store and rewrites that value on change. The forwarded
        // index keeps its own keyed store and is injected here.
        ParsleyChannels channels = new ParsleyChannels(frontierStore, forwardedIndex);

        int taskPartition = context.taskId().partition();
        ParsleyVectorClock.CoordinatePredicate inScope = consumedScope();

        // Reconcile restored causal state with the currently declared input set (the #21 fix: the
        // scope decision keys on "input set unchanged since the persisted blob", not blob presence
        // alone). Retired ancestry re-homes into the carried-ancestry clock the stamp keeps merging;
        // an added input's frontier seeds at the carried-ancestry value so its already-carried
        // prefix is skipped, never replayed as live; a recreated input's old UUID is destroyed.
        // Then seed an (empty) entry for every consumed input channel so rescope has a recorded
        // channel to diff against on the next scope change; the entry contributes nothing to
        // vectorTime(), which is a plain max-merge. Idempotent
        // against an already-rescoped/seeded store (the
        // common case, every call after the first), so this costs a redundant write, never a wrong one.
        Map<String, Uuid> previousInputs = channels.declaredInputs();
        channels.rescope(topicUuids, taskPartition);
        logScopeDiff(previousInputs);
        // The destroyed source incarnations: inputs recreated across the restart (same name, new
        // UUID) — the same diff logScopeDiff just reported and rescope just purged from channel
        // state. Passed to the L2 constructor, which owns the held-record disposition (purge a
        // destroyed incarnation's records; fail init on a removed-but-alive input's records).
        Set<Uuid> destroyedSources = new HashSet<>();
        for (Map.Entry<String, Uuid> previous : previousInputs.entrySet()) {
            Uuid current = topicUuids.get(previous.getKey());
            if (current != null && !current.equals(previous.getValue())) {
                destroyedSources.add(previous.getValue());
            }
        }
        for (Uuid topicId : topicUuids.values()) {
            channels.channelUpdate(topicId, taskPartition, ParsleyVectorClock.empty());
        }

        // Wire the ownOutputs clock: bind the producer-ack registry (when this task runs under a
        // CausalStreams instance — a TopologyTestDriver run has none) so every stamp's preceding
        // fold can translate acked sink names to UUID identity — the registry is also the
        // pending-send view the crossing wait blocks on before each stamp, per sink partition,
        // bounded by the producer's delivery.timeout.ms (past it the unacked send has failed, so
        // the wait throws and the transaction dies with it, never stamping and proceeding) — then
        // seed each declared sink partition (sink resolution is strict at init, so every declared
        // sink is resolved and read) at its end offset - 1, the sink's last appended position. The
        // seed is an over-claim (it covers siblings' records on a shared sink, aborted tails, and
        // markers), and safe for exactly that reason: every position it names is a real appended
        // one, so it can only delay a downstream delivery, never reorder it. It also heals the
        // restored blob trailing the last transaction's acks. Runs after rescope so a recreated
        // input-sink's destroyed UUID is already purged before its successor seeds.
        Object registryId = context.appConfigs().get("producer." + ParsleyOwnOutputRegistry.CONFIG_KEY);
        ParsleyOwnOutputRegistry registry = registryId == null
                ? null
                : ParsleyOwnOutputRegistry.lookup(registryId.toString());
        if (registry != null) {
            channels.bindOwnOutputSource(registry, registry, sinkTopicUuids, deliveryTimeoutMs);
            log.debug("Bound own-output registry '{}' for sinks {} [task: {}]",
                    registryId, sinkTopicUuids.keySet(), context.taskId());
        } else {
            // Not an error: TopologyTestDriver runs and low-level supplier wirings have no
            // CausalStreams instance to mint a registry — ownOutputs then advances only through
            // the init-time end-offset seed. Logged so a production task silently missing live
            // ack tracking is diagnosable.
            log.info("No own-output registry bound (id: {}) — ownOutputs advances only via the "
                    + "end-offset seed; crossing wait inactive [task: {}]", registryId, context.taskId());
        }
        for (Map.Entry<String, Uuid> sink : sinkTopicUuids.entrySet()) {
            for (Map.Entry<Integer, Long> end
                    : sinkEndOffsets.getOrDefault(sink.getKey(), Map.of()).entrySet()) {
                if (end.getValue() > 0) {
                    channels.acknowledge(sink.getValue(), end.getKey(), end.getValue() - 1);
                }
            }
        }
        // Heal the restored ownOutputs clock for the PREVIOUS run's sinks that are no longer sinks
        // the frontier value always trails the final transaction's own acks, and the end-offset
        // seed above covers only the currently declared sinks — without this, a redeploy that drops
        // a sink (or turns it into an input) would stamp under-claims of this node's own
        // final-transaction outputs, and a downstream consumer of that topic plus another of this
        // node's sinks could deliver an effect before its cause. Runs after rescope
        // deliberately: an added former-own-sink input's frontier seed must keep the carried
        // (pre-heal) cut, so the node's own final-transaction outputs are delivered as ordinary
        // input records above it, while the stamp is healed to cover them from the first emission.
        healFormerSinkOwnOutputs(channels);
        channels.declareSinks(sinkTopicUuids);

        // This stage's own sink topics — never a delivery-scope concern (inScope, above), but fed
        // to the core for the reflected-claim diagnostic: an inbound claim on an own-sink
        // coordinate above the ownOutputs clock is worth seeing (a stale own-output view or an
        // untruthful peer stamp), never worth failing over. The gate treats reflected claims like
        // any other dependency (consumed → gated on local delivery; unconsumed → ignored).
        Set<Uuid> ownSinkTopicIds = Set.copyOf(sinkTopicUuids.values());
        ParsleyVectorClock.CoordinatePredicate ownSinkTopics = (topicId, partition) -> ownSinkTopicIds.contains(topicId);

        return new ParsleyCausalBroadcast<>(channels, buffer, candidateIndex,
                wiredMetrics.metrics(), context::currentSystemTimeMs, inScope, ownSinkTopics,
                destroyedSources);
    }

    /**
     * Resolves this instance's {@link ParsleyTopicIdentityWatch} (minted by {@link CausalStreams};
     * absent under TopologyTestDriver and low-level wirings) and registers the name → UUID bindings
     * this task just resolved — every input and every declared sink (sink resolution is strict at
     * init, so the watch always covers the full declared set) — as its expectations. Sinks are
     * included because a mid-run <em>sink</em> recreation is as unsafe as an input's: the ack
     * registry folds by topic name through a stale UUID map, and the recreated topic's restarted
     * offsets make every fold a monotone no-op, so stamps silently stop claiming this node's own
     * new outputs, so downstream stamps under-claim this node's causal past.
     */
    private void registerIdentityExpectations() {
        Object watchId = context.appConfigs().get("producer." + ParsleyTopicIdentityWatch.CONFIG_KEY);
        this.identityWatch = watchId == null ? null : ParsleyTopicIdentityWatch.lookup(watchId.toString());
        ParsleyTopicIdentityWatch watch = this.identityWatch;
        if (watch == null) {
            log.info("No topic-identity watch bound (id: {}) — mid-run topic recreation is not "
                    + "monitored; identity is still re-resolved at every restart [task: {}]",
                    watchId, context.taskId());
            return;
        }
        topicUuids.forEach(watch::expect);
        sinkTopicUuids.forEach(watch::expect);
    }

    /**
     * Fails the task fast if the identity watch has detected a mid-run topic recreation — checked
     * before each record is ingested and (via the stamping context) before each stamped forward.
     * One volatile read when a watch is bound; a no-op otherwise.
     */
    private void ensureTopicIdentityIntact() {
        ParsleyTopicIdentityWatch watch = identityWatch;
        if (watch != null) {
            watch.ensureIntact();
        }
    }

    /**
     * Heals the restored {@code ownOutputs} clock for every topic the previous run declared as a sink
     * but this run does not ({@link ParsleyChannels#declaredSinks}), since only a declared sink's acks
     * can trail the persisted blob. Per former sink, resolved through a fresh admin session: if the
     * topic survives under its recorded UUID, acknowledge each partition's {@code endOffset - 1} (the
     * same over-claim the current-sink seed makes, safe because it names a real appended position);
     * if it was deleted or recreated under a new UUID, purge the recorded UUID's claims
     * ({@link ParsleyChannels#destroyOwnOutput}, the one removal from stamp-feeding state the
     * unconditional merge permits), since no receiver can deliver them; if
     * resolution fails otherwise, fail init loudly, because proceeding would stamp under-claims and
     * purging without proof would manufacture them. A still-declared sink is skipped, and an unchanged
     * sink set opens no admin session.
     */
    private void healFormerSinkOwnOutputs(ParsleyChannels channels) {
        Map<String, Uuid> previousSinks = channels.declaredSinks();
        Map<String, Uuid> former = new HashMap<>();
        for (Map.Entry<String, Uuid> previous : previousSinks.entrySet()) {
            Uuid current = sinkTopicUuids.get(previous.getKey());
            if (current == null) {
                former.put(previous.getKey(), previous.getValue());
            } else if (!current.equals(previous.getValue())) {
                channels.destroyOwnOutput(previous.getValue());
            }
        }
        if (former.isEmpty()) {
            return;
        }
        try (ParsleyTopicAdmin admin = adminFactory.apply(context.appConfigs())) {
            for (Map.Entry<String, Uuid> sink : former.entrySet()) {
                Uuid currentId = resolveFormerSinkId(admin, sink.getKey());
                if (!sink.getValue().equals(currentId)) {
                    log.info("Former sink topic '{}' is deleted or recreated; purging its destroyed "
                            + "own-output claims [task: {}]", sink.getKey(), context.taskId());
                    channels.destroyOwnOutput(sink.getValue());
                    continue;
                }
                Map<Integer, Long> ends;
                try {
                    ends = admin.endOffsets(sink.getKey());
                } catch (Exception e) {
                    throw new IllegalStateException("failed to read end offsets for former sink topic '"
                            + sink.getKey() + "' while healing the restored ownOutputs clock; the "
                            + "persisted clock can trail the final transaction's acks, so starting "
                            + "without the heal could stamp under-claims of this node's own outputs", e);
                }
                for (Map.Entry<Integer, Long> end : ends.entrySet()) {
                    if (end.getValue() > 0) {
                        channels.acknowledge(sink.getValue(), end.getKey(), end.getValue() - 1);
                    }
                }
                log.info("Healed own-output claims for former sink topic '{}' from its end offsets "
                        + "[task: {}]", sink.getKey(), context.taskId());
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to heal former-sink own-output claims", e);
        }
    }

    /**
     * Resolves {@code topic}'s current UUID for the former-sink heal, mapping a provably missing
     * topic ({@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException} in the failure
     * chain) to {@code null} — the destroyed case — while any other failure (an unreachable broker)
     * propagates and fails init: only positive evidence of destruction may purge claims.
     */
    private static @Nullable Uuid resolveFormerSinkId(ParsleyTopicAdmin admin, String topic) {
        try {
            return admin.topicIds(List.of(topic)).get(topic);
        } catch (Exception e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                    return null;
                }
            }
            throw new IllegalStateException("failed to resolve former sink topic '" + topic
                    + "' while healing the restored ownOutputs clock", e);
        }
    }

    /**
     * Reports the input-set diff {@link ParsleyChannels#rescope} just reconciled: which inputs were
     * added, removed, or recreated (same name, new UUID) relative to the persisted declaration. An
     * empty {@code previousInputs} (a fresh store, or a blob predating the declared-input section)
     * has nothing to compare — nothing is logged; an unchanged set logs at debug.
     */
    private void logScopeDiff(Map<String, Uuid> previousInputs) {
        if (previousInputs.isEmpty()) {
            return;
        }
        Set<String> added = new TreeSet<>();
        Set<String> recreated = new TreeSet<>();
        for (Map.Entry<String, Uuid> current : topicUuids.entrySet()) {
            Uuid previous = previousInputs.get(current.getKey());
            if (previous == null) {
                added.add(current.getKey());
            } else if (!previous.equals(current.getValue())) {
                recreated.add(current.getKey());
            }
        }
        Set<String> removed = new TreeSet<>(previousInputs.keySet());
        removed.removeAll(topicUuids.keySet());
        if (added.isEmpty() && removed.isEmpty() && recreated.isEmpty()) {
            log.debug("Input set unchanged since the persisted state [task: {}]", context.taskId());
            return;
        }
        log.info("Input set changed since the persisted state [task: {}] — added: {}, removed: {}, "
                + "recreated: {}. Removed inputs' ancestry re-homes into the carried-ancestry clock "
                + "(stamps keep dominating it); added inputs skip the prefix at or below this node's "
                + "carried ancestry (a full reset is the opt-in for processing that history).",
                context.taskId(), added, removed, recreated);
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        // Before anything ingests or advances state under a possibly stale name → UUID binding:
        // once the watch has detected a mid-run topic recreation, every record from here on could
        // be mislabelled, so the task must die now.
        ensureTopicIdentityIntact();
        switch (classify(record)) {
            case NULL_MESSAGE -> {
                handleNullMessage(record);
                return;
            }
            case BUSINESS -> { }
        }
        ParsleyMessage<KIn, VIn> ingested = ingest(record);
        ParsleyCausalBroadcast.Outcome<KIn, VIn> outcome = causalBroadcast().receive(ingested);
        deliver(outcome.delivered());
        // Advertise this node's progress so downstream channel clocks advance gap-free (L3's
        // emission rule). A delivered record advertises through its business output's stamp — or, if
        // the delegate forwarded nothing, the null message emitted in deliver(). A consumed record
        // that was buffered produces neither, so emit a heartbeat null message — but only when
        // receiving it genuinely advanced a consumed channel's clock (a coordinate's first sighting
        // seeds the frontier, a bridge crosses a marker gap), so an unrelated held record does not
        // flood downstream with no-op null messages. This is the CMB trigger rule the relay half
        // already applies to a received null message (ParsleyGossip), here applied to a record that
        // was buffered rather than delivered.
        if (outcome.delivered().isEmpty() && outcome.advancedConsumedChannel()) {
            // Key the heartbeat with the buffered record's own key so it routes to that record's
            // partition, matching where its eventual business output will land; carry the buffered
            // record's own timestamp so downstream stream time follows the data's time.
            advertise(record.key(), record.timestamp());
        }
    }

    @Override
    public void close() {
        log.info("Processor closing [task: {}]", context.taskId());
        if (quiesce != null) {
            quiesce.unregister(context.taskId().toString());
        }
        // init() may have thrown before it finished — yet Streams
        // still calls close(). Close only what init() actually set up: closing an un-inited delegate, or
        // dereferencing the still-null wiredMetrics, would throw here and mask the real init failure with
        // a spurious NPE. (The quiesce cleanup above is already safe on a partial init: the
        // quiesce sets no-op on an unregistered id.)
        if (delegateInitialized) {
            delegate.close();
        }
        if (wiredMetrics != null) {
            wiredMetrics.close(context.metrics());
        }
    }

    private void deliver(List<ParsleyMessage<KIn, VIn>> admitted) {
        for (ParsleyMessage<KIn, VIn> message : admitted) {
            // Every forward the delegate makes for this message is stamped live by
            // causalBroadcast.broadcast() — the node's vector time at forward time, sound across all
            // input branches (never a per-record frontier-snapshot merged with inbound deps, which was
            // correct only in single-layer topologies).
            deliveryMetadata = new ParsleyRecordMetadata(message.topic(), message.partition(), message.offset());
            // User headers + the producer's dependencies only; the source coordinate is surfaced via
            // context.recordMetadata(), and ParsleyProcessorContext re-stamps the frontier on forward.
            stampingContext.resetForwardCount();
            delegate.process(new Record<>(message.key(), message.value(), message.timestamp(),
                    message.headersWithDependencies()));
            // If the delegate did not forward any business record for this input, emit a null
            // message carrying the node's current vector time so that downstream nodes still
            // learn about this node's causal progress (L3's emission rule). Without this, a
            // non-emitting processor silently stalls downstream causal progress, breaking the inductive
            // correctness of multi-layer topologies.
            if (stampingContext.forwardCount() == 0) {
                // Reuse the delivered record's key so the stand-in null message routes to the same
                // partition its business output would have, and its timestamp so downstream stream
                // time advances with the data's time, not the wall clock.
                advertise(message.key(), message.timestamp());
            }
        }
        deliveryMetadata = null;
        updateQuiesceState();
    }

    /**
     * Reports this task's current buffer-drained state to {@link #quiesce} (if registered). Called
     * after every buffer-depth-changing event (every path that can
     * hold or release a record funnels through {@link #delivered}), so the signal reflects the
     * current buffer depth without polling. Never fabricates progress — it only observes the buffer
     * depth the ordinary delivery path already produced. Quiesce additionally gates its drained flag on
     * {@link ParsleyQuiesce#isQuiesceRequested()}.
     */
    private void updateQuiesceState() {
        boolean empty = causalBroadcast().bufferSize() == 0;
        if (quiesce != null) {
            quiesce.setDrained(context.taskId().toString(), quiesce.isQuiesceRequested() && empty);
        }
    }

    /** Which Parsley protocol record, if any, {@link #classify} identified a record as. */
    private enum RecordKind { NULL_MESSAGE, BUSINESS }

    /**
     * Classifies {@code record} by a single pass over its headers, identifying a Parsley null
     * message ({@link ParsleyHeader#NULL_MESSAGE}) —
     * never by a record's key, which for a null message carries the triggering record's key for
     * routing. A null message carries no business payload and must never be forwarded to the user
     * delegate or buffered — it exists only to propagate causal progress through
     * non-emitting layers ({@link ParsleyGossip}). Anything else is {@link RecordKind#BUSINESS}.
     */
    private RecordKind classify(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.NULL_MESSAGE.equals(h.key())) {
                return RecordKind.NULL_MESSAGE;
            }
        }
        return RecordKind.BUSINESS;
    }

    /**
     * Handles a received null message: decodes its carried clock, folds it through
     * {@link ParsleyGossip#receive}, and relays this node's own null message downstream iff the
     * received one advanced this node's knowledge of a consumed channel (the relay rule). The null
     * message is never delivered to the delegate and never buffered. Two guards mirror the business
     * path: a null message on an unregistered topic fails the task, as {@link #ingest} does, and a
     * present-but-undecodable {@link ParsleyHeader#CAUSAL_CLOCK} header fails it via
     * {@link #onUnresolvableClock} rather than folding nothing and dropping the peer's progress claims
     * (which would leave downstream stamps under-claiming); an absent header stays an empty clock
     * whose offset is still delivered.
     *
     * <p>The {@link ParsleyGossip#receive} call and the delivery of its released records run outside
     * the decode catch: past that point a failure is a delivery failure, not a decode failure, and
     * swallowing it would let the task commit past records the delegate never processed. Fail-closed.
     */
    private void handleNullMessage(Record<KIn, VIn> record) {
        RecordMetadata meta = requireRecordMetadata();
        String topic = meta.topic();
        int partition = meta.partition();
        long offset = meta.offset();
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no ParsleySource registered for topic '" + topic
                            + "'; call addSource(...) on the ParsleyProcessorSupplier builder for every input topic");
        }
        ParsleyVectorClock carried = ParsleyVectorClock.empty();
        Header clockHeader = record.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        if (clockHeader != null && clockHeader.value() != null) {
            try {
                carried = ParsleyVectorClock.fromBytes(clockHeader.value());
            } catch (Exception e) {
                throw onUnresolvableClock(new CausalVectorClockResolutionException(topic, topicId,
                        partition, offset, clockHeader.value(),
                        "encoded causal-clock header length " + clockHeader.value().length, e));
            }
        }
        ParsleyGossip.Reception<KIn, VIn> reception = gossip.receive(topicId, partition, offset, carried);
        deliver(reception.delivered());
        // Relay only when this null message advanced this node's knowledge of a channel it
        // consumes (the relay trigger scope on ParsleyGossip): the delivered/advertised boundary still
        // propagates through non-subscribing layers when a node's own inputs genuinely moved,
        // while custody-only news folds into the stamp without obliging a message — the relay
        // discipline that lets topology cycles quiesce. Reuse the incoming message's own key:
        // upstream keyed it to route to this partition,
        // so re-emitting under the same key keeps the relayed message on the co-partitioned
        // downstream partition. Its timestamp propagates too — a relay carries the original
        // trigger's event time onward, never re-stamping it with this node's wall clock.
        if (reception.advancedConsumedChannel()) {
            advertise(record.key(), record.timestamp());
        }
    }

    /**
     * Forwards this node's null message ({@link ParsleyGossip#advertise}): its current outbound stamp
     * in the {@link ParsleyHeader#CAUSAL_CLOCK} header, marked by {@link ParsleyHeader#NULL_MESSAGE},
     * sent to every business sink ({@link #sinkNodeNames}) and keyed with {@code triggerKey} for wire
     * content only. {@link ParsleyMarkerPartition} routes it to this task's own partition regardless
     * of the key, so a downstream task's channel clock always advances across the sink boundary. The
     * same {@link ParsleyCausalBroadcast#broadcast} that stamps business forwards attaches the clock,
     * so the two stamps cannot diverge, and it bypasses the stamping proxy's forward counter so it
     * does not itself count as a business emission.
     *
     * <p>{@code triggerTimestamp} is the triggering record's own timestamp, never the wall clock:
     * Kafka Streams advances downstream stream time from every polled record's timestamp, so a
     * wall-clock stamp during a reprocessing run would yank downstream windows and grace periods to
     * now (see {@link ParsleyGossip#advertise} for the retention consequence). The {@code KIn}-to-{@code
     * KOut} key cast is sound because a causal processor must not change the key across the node.
     */
    @SuppressWarnings("unchecked") // KIn==KOut under the co-partitioning contract
    private void advertise(@Nullable KIn triggerKey, long triggerTimestamp) {
        forwardToSinks(gossip.advertise((KOut) (Object) triggerKey, triggerTimestamp));
    }

    /**
     * Forwards a control-plane record (a null message) to every business sink by name, or
     * broadcasts (Kafka Streams' own zero-arg {@code forward}) when {@link #sinkNodeNames} is empty —
     * the common case, with no incompatibly-typed sibling sink configured. Named forwarding is required
     * the moment one is a sibling child of this processor's business sink(s): see
     * {@link ParsleyProcessorContext}.
     *
     * <p>The sole call site for every null-message forward (business forwards go through the stamping
     * proxy, never here), so this is also the sole place {@link ParsleyMarkerPartition} is set: {@link
     * CausalTopology} installs a {@link ParsleyMarkerPartitioner} on every sink this stage declares,
     * which reads this override to route the null message to this task's own owned partition — {@code
     * context.taskId().partition()} — regardless of {@code record}'s key. Set immediately before, cleared
     * immediately after (a {@code finally}, so an exception mid-forward never leaks the override onto a
     * later, unrelated business forward on this thread).
     */
    private void forwardToSinks(Record<KOut, VOut> record) {
        ParsleyMarkerPartition.set(context.taskId().partition());
        try {
            if (sinkNodeNames.isEmpty()) {
                context.forward(record);
                return;
            }
            for (String name : sinkNodeNames) {
                context.forward(record, name);
            }
        } finally {
            ParsleyMarkerPartition.clear();
        }
    }

    /**
     * Decodes {@code record} into a {@link ParsleyMessage}, ready for {@link ParsleyCausalBroadcast#receive}.
     */
    private ParsleyMessage<KIn, VIn> ingest(Record<KIn, VIn> record) {
        RecordMetadata meta = requireRecordMetadata();
        String topic = meta.topic();
        TopicPartition source = new TopicPartition(topic, meta.partition());
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no ParsleySource registered for topic '" + topic
                            + "'; call addSource(...) on the ParsleyProcessorSupplier builder for every input topic");
        }
        try {
            return ParsleyMessage.from(record, source, meta.offset(), topicId);
        } catch (CausalVectorClockResolutionException e) {
            throw onUnresolvableClock(e);
        }
    }

    /**
     * The current record's source metadata, required present: every caller runs inside
     * {@link #process}, where Kafka Streams always supplies it for a source-fed record. Absence means
     * a record reached the causal path from a context with no source coordinate (e.g. a punctuator
     * forward) — fabricating a coordinate (the old {@code partition 0, offset 0} fallback) would write
     * causal state for a coordinate nothing actually consumed, so this is an invariant violation, not
     * a recoverable input.
     */
    private RecordMetadata requireRecordMetadata() {
        return context.recordMetadata().orElseThrow(() -> new IllegalStateException(
                "record metadata is unavailable on the causal path; Parsley processes records only from "
                        + "topic sources, where Kafka Streams supplies the source coordinate"));
    }

    /**
     * Handles an inbound record whose causal-clock header could not be decoded. Its real
     * dependencies are unknown, so forwarding it on the ordinary path would deliver on an unknown
     * premise — never permitted, so this fails the task fast: the record was never buffered and its
     * source offset is not committed past it, so it is reprocessed on restart.
     */
    private CausalVectorClockResolutionException onUnresolvableClock(CausalVectorClockResolutionException e) {
        wiredMetrics.metrics().recordClockResolutionError();
        log.error("Unresolvable causal-clock header on {}-{} @{}; failing fast (fail-closed). "
                + "The record was not forwarded and is reprocessed on restart. {}",
                e.topic(), e.partition(), e.offset(), e.details(), e);
        return e;
    }

    /**
     * Resolves each registered source topic's stable UUID from the broker. The topology decorator has
     * no broker configuration until init, so this runs here (once per task), using the task's
     * {@code appConfigs()} so it inherits broker security settings.
     */
    private Map<String, Uuid> resolveTopicUuids(ProcessorContext<KOut, VOut> context) {
        List<String> topicList = new ArrayList<>(topics);
        Map<String, Uuid> resolved;
        Map<String, Integer> sourcePartitionCounts;
        Map<String, Integer> sinkPartitionCounts;
        Map<String, String> sinkCleanupPolicies;
        Map<String, String> sourceCleanupPolicies;
        try (ParsleyTopicAdmin admin = adminFactory.apply(context.appConfigs())) {
            try {
                resolved = admin.topicIds(topicList);
                sourcePartitionCounts = admin.partitionCounts(topicList);
                sinkPartitionCounts = additionalTopicInfo(admin, "partition count", ParsleyTopicAdmin::partitionCounts);
                sinkCleanupPolicies = additionalTopicInfo(admin, "cleanup.policy", ParsleyTopicAdmin::cleanupPolicies);
                // Source cleanup.policy is resolved over the input topics directly (they must already
                // exist for the resolves above to have succeeded), not via the best-effort
                // additionalTopicInfo path: a compacted source is a correctness hazard for the
                // skip-bridge, so its describe failing must fail init rather than skip the check (see
                // validateSourcesNotCompacted).
                sourceCleanupPolicies = admin.cleanupPolicies(topicList);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "failed to resolve topic metadata for causal buffers " + topics
                                + "; ensure the topics exist and the broker is reachable", e);
            }
            // Outside the wrap above, so a strict sink-resolution failure surfaces naming the sink
            // and its remedy rather than as a generic buffer-metadata error.
            this.sinkTopicUuids = resolveSinkTopicUuids(admin);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to resolve topic metadata for causal buffers " + topics
                            + "; ensure the topics exist and the broker is reachable", e);
        }
        for (String topic : topics) {
            if (resolved.get(topic) == null) {
                throw new IllegalStateException("broker did not return a UUID for topic '" + topic + "'");
            }
        }
        // Validate outside the resolve try/catch so a check failure surfaces as itself rather
        // than being wrapped as a broker-reachability error.
        validateSourcesNotCompacted(sourceCleanupPolicies);
        validateSourcePartitionParity(sourcePartitionCounts);
        validateSinkPartitionWidth(sourcePartitionCounts, sinkPartitionCounts);
        validateSinksNotCompacted(sinkCleanupPolicies);
        return resolved;
    }

    /**
     * Strict, per-topic UUID resolution over {@link #sinkTopics}, capturing each sink's per-partition
     * end offsets ({@link #sinkEndOffsets}) in the same admin session for the {@code ownOutputs}
     * init-time seed. It feeds the ack fold's name → UUID translation and the reflected-claim
     * diagnostic, so a declared sink that cannot be resolved (a missing topic, or a failed describe or
     * end-offset read) fails init loudly rather than skipping: the seed is what keeps this node's
     * stamps transitively complete, and a skipped sink would silently stamp under-claims for the
     * task's whole lifetime (the same reasoning
     * as {@link #healFormerSinkOwnOutputs}). A causal sink must therefore exist before the stage
     * starts; auto-creation on first produce is not supported.
     */
    private Map<String, Uuid> resolveSinkTopicUuids(ParsleyTopicAdmin admin) {
        Map<String, Uuid> ids = new HashMap<>();
        Map<String, Map<Integer, Long>> endOffsets = new HashMap<>();
        for (String topic : sinkTopics) {
            Uuid id;
            try {
                id = admin.topicIds(List.of(topic)).get(topic);
            } catch (Exception e) {
                throw new IllegalStateException(sinkResolutionFailure(topic), e);
            }
            if (id == null) {
                throw new IllegalStateException(sinkResolutionFailure(topic));
            }
            ids.put(topic, id);
            try {
                endOffsets.put(topic, Map.copyOf(admin.endOffsets(topic)));
            } catch (Exception e) {
                throw new IllegalStateException("failed to read end offsets for declared sink topic '"
                        + topic + "'; the ownOutputs init-time seed cannot be skipped — the persisted "
                        + "clock can trail the final transaction's acks, so starting unseeded could "
                        + "stamp under-claims of this node's own outputs. Check broker reachability.",
                        e);
            }
        }
        this.sinkEndOffsets = Map.copyOf(endOffsets);
        return ids;
    }

    /** The strict sink-resolution failure message: names the sink, the rule, and the remedy. */
    private static String sinkResolutionFailure(String topic) {
        return "failed to resolve the topic id for declared sink topic '" + topic + "'; a causal "
                + "sink must exist before the stage starts (create the topic, and check broker "
                + "reachability) — own-output stamping depends on its resolved identity, and "
                + "starting without it would stamp under-claims of this node's own outputs";
    }

    /**
     * Best-effort, per-topic resolution of {@code describe} over {@link #sinkTopics}
     * — extra topics (e.g. a {@link CausalStreams} sink) folded into the startup topology checks
     * without being consumed. Unlike {@link #resolveSinkTopicUuids} (strict: a sink must exist and
     * resolve, it feeds correctness mechanisms), these describes feed only the sink-side topology
     * checks — so a topic that cannot be described is logged and omitted from the result rather
     * than failing the task here; the checks then judge whatever was resolved.
     *
     * <p>Resolved <strong>one topic at a time</strong>, deliberately not batched: the real
     * {@link ParsleyTopicAdmin} calls this delegates to (via {@code describeTopics}/
     * {@code describeConfigs}) fail their <em>entire</em> batch if any single topic in it errors, so
     * batching all sink topics together would let one not-yet-created sink mask a genuine
     * misconfiguration on another, already-existing sink.
     */
    private <T> Map<String, T> additionalTopicInfo(
            ParsleyTopicAdmin admin, String what, TopicDescriptor<T> describe) {
        if (sinkTopics.isEmpty()) {
            return Map.of();
        }
        Map<String, T> result = new HashMap<>();
        for (String topic : sinkTopics) {
            try {
                result.putAll(describe.describe(admin, List.of(topic)));
            } catch (Exception e) {
                log.warn("Could not resolve {} for sink topic '{}'; skipping the topology check "
                        + "for it", what, topic, e);
            }
        }
        return result;
    }

    /** A {@link ParsleyTopicAdmin} lookup keyed by topic name, for {@link #additionalTopicInfo}. */
    @FunctionalInterface
    private interface TopicDescriptor<T> {
        Map<String, T> describe(ParsleyTopicAdmin admin, List<String> topics) throws Exception;
    }

    /**
     * Fails the task at {@code init()}, unconditionally, when the causal <em>source</em> topics do
     * not share a partition count. Co-partitioning requires an equal partition count across all
     * causally-related source topics so that a single task owns the complete partition set for a
     * related group; unequal counts make that impossible and let a task evaluate the causal frontier
     * against an incomplete partition set — a causal-safety hole, since the argument that makes an
     * out-of-scope coordinate safe to ignore assumes proper sharding, not a topology lint, so there
     * is no opt-down. A single source topic is always vacuously fine. Sink topics are deliberately
     * not part of this parity
     * check: a sink may legitimately be <em>wider</em> than the sources (a funnel fanning into a
     * re-keyed sink) — see {@link #validateSinkPartitionWidth} for the sink-side constraint.
     */
    private static void validateSourcePartitionParity(Map<String, Integer> partitionCounts) {
        if (partitionCounts.size() < 2 || Set.copyOf(partitionCounts.values()).size() <= 1) {
            return;
        }
        throw new IllegalStateException("causal source topics have mismatched partition counts "
                + partitionCounts + "; co-partitioning requires an equal partition count across all "
                + "causally-related source topics so each task owns the complete partition set for a "
                + "related group. Repartition the sources to a shared count.");
    }

    /**
     * Fails the task at {@code init()}, unconditionally, when a declared sink topic has fewer
     * partitions than the sources: {@link ParsleyMarkerPartitioner} routes every protocol marker to
     * the forwarding task's own partition ({@code taskId().partition()}), so a task owning a source
     * partition the sink does not have would fail the marker produce outright, crash-looping the
     * task at runtime — this check surfaces that impossibility once, clearly, at startup. A sink as
     * wide as, or wider than, every source (a funnel fanning narrow sources into a wider, re-keyed
     * sink) is protocol-safe and passes. Best-effort per sink: a sink whose describe failed is
     * absent from {@code sinkPartitionCounts} and skipped here (sink <em>existence</em> is enforced
     * strictly by {@link #resolveSinkTopicUuids}).
     */
    private static void validateSinkPartitionWidth(Map<String, Integer> sourcePartitionCounts,
                                                   Map<String, Integer> sinkPartitionCounts) {
        int widestSource = sourcePartitionCounts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        for (Map.Entry<String, Integer> sink : sinkPartitionCounts.entrySet()) {
            if (sink.getValue() < widestSource) {
                throw new IllegalStateException("declared sink topic '" + sink.getKey() + "' has "
                        + sink.getValue() + " partition(s) but a causal source topic has " + widestSource
                        + "; protocol markers route to the forwarding task's own partition, so a sink "
                        + "narrower than its sources fails the marker produce at runtime. Give the sink "
                        + "at least as many partitions as the widest source.");
            }
        }
    }

    /**
     * The effective producer {@code delivery.timeout.ms}: the {@code producer.}-prefixed override
     * wins, then the un-prefixed client config Streams also passes through, then Kafka's default.
     * See the {@link #deliveryTimeoutMs} field for the two uses (crossing-wait bound, stall
     * threshold). Package-private so the resolution branches are pinned directly.
     */
    static long deliveryTimeoutMs(Map<String, Object> appConfigs) {
        Object configured = appConfigs.get("producer." + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        if (configured == null) {
            configured = appConfigs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        }
        return switch (configured) {
            case Number number -> number.longValue();
            case String text when !text.isBlank() -> {
                try {
                    yield Long.parseLong(text.trim());
                } catch (NumberFormatException e) {
                    // Never silently defaulted: this value bounds the crossing wait and is the
                    // stall threshold — a typo that quietly became 120 s would misconfigure both.
                    throw new IllegalStateException("malformed " + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                            + " value '" + text + "'; it bounds the crossing wait and the stall "
                            + "diagnostic, so it must parse as a long (an absent key defaults to Kafka's "
                            + DEFAULT_DELIVERY_TIMEOUT_MS + " ms)", e);
                }
            }
            case null, default -> DEFAULT_DELIVERY_TIMEOUT_MS;
        };
    }

    /**
     * Fails the task, unconditionally, when a causal <em>source</em> topic has {@code cleanup.policy}
     * including {@code compact}. Unlike {@link #validateSinksNotCompacted} (best-effort per sink), a
     * source describe failure fails init too — this is a correctness guard on the skip-bridge, same
     * precedent as {@link #resolveSinkTopicUuids}. The bridge ({@link ParsleyChannels#bridge}) treats
     * an offset a {@code read_committed} consumer skipped as a transaction marker or aborted record and
     * folds it into the contiguous frontier. Log compaction breaks that premise: it removes a real,
     * committed mid-log record, leaving exactly the same consumer-visible hole a marker would — so the
     * bridge would advance the frontier past a record that was never delivered, releasing a dependent
     * before its cause (a silent causal-order violation). A compacted source therefore cannot be consumed
     * causally at all; the only safe response is to refuse at startup. {@code compact,delete} is equally
     * unsafe: compaction still runs.
     *
     * <p>This is a <strong>startup</strong> check: it reads {@code cleanup.policy} once at {@code init()}.
     * Flipping a running source topic to {@code compact} (via {@code kafka-configs --alter}) is not caught
     * here — it punches consumer-visible holes with no fetch error, so it must be avoided operationally.
     */
    private void validateSourcesNotCompacted(Map<String, String> sourceCleanupPolicies) {
        for (Map.Entry<String, String> entry : sourceCleanupPolicies.entrySet()) {
            String policy = entry.getValue();
            if (policy == null || !policy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
                continue;
            }
            throw new IllegalStateException("causal source topic '" + entry.getKey()
                    + "' has cleanup.policy=" + policy + "; a compacted source can drop a real committed "
                    + "mid-log record, leaving the same consumer-visible hole a transaction marker does, "
                    + "which the skip-bridge would cross — releasing a dependent before its cause. Set "
                    + "cleanup.policy=delete on any topic consumed by a causal stage.");
        }
    }

    /**
     * Fails the task at {@code init()}, unconditionally, when a {@link CausalStreams} sink topic's
     * {@code cleanup.policy} includes {@code compact}. A protocol null message is a null-value
     * record wire-indistinguishable from a compaction tombstone, so under compaction it can be
     * removed from the log before a slow consumer reads it — silently losing the vector time it
     * carried (and read as a <em>delete</em> by any table-style consumer of the sink).
     * {@code compact,delete} is equally unsafe: compaction still runs. No viable deployment
     * tolerates this, so there is no opt-down. Best-effort per sink, like
     * {@link #validateSinkPartitionWidth}: a sink whose describe failed is skipped here.
     */
    private static void validateSinksNotCompacted(Map<String, String> cleanupPolicies) {
        for (Map.Entry<String, String> entry : cleanupPolicies.entrySet()) {
            String policy = entry.getValue();
            if (policy == null || !policy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
                continue;
            }
            throw new IllegalStateException("sink topic '" + entry.getKey() + "' has cleanup.policy="
                    + policy + "; a Parsley null message is a null-value record wire-indistinguishable "
                    + "from a compaction tombstone and can be compacted away before a slow consumer "
                    + "reads it — set cleanup.policy=delete");
        }
    }
}
