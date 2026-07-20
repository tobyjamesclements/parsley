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
 * A Decorator (GoF) over a user {@link Processor}, gating delegation on the causal frontier: an incoming
 * record is held until the completeness frontier dominates its causal dependencies. Delivery is strictly
 * fail-closed — there is no eviction, buffer limit, or timeout that forwards a record ahead of its
 * dependencies. Every record that is delivered reaches {@code delegate.process(...)} exactly once,
 * and the state reads/writes the delegate performs and every record it forwards are causally ordered;
 * forwards are stamped by a {@link ParsleyProcessorContext} via {@link ParsleyCausalBroadcast#broadcast}.
 *
 * <p>Held records are persisted to a changelog-backed buffer store and restored on {@code init}, so
 * they survive a restart (a buffered record's source offset is committed past it, so it would
 * otherwise be lost). The frontier-before-forward invariant from {@link ParsleyCausalBroadcast} is preserved
 * on both the admit and punctuator paths.
 *
 * <p><strong>Clock-invisible markers.</strong> A received watermark
 * ({@link #handleWatermark}) is relayed downstream only when it genuinely
 * taught this node's channel something it did not already know
 * ({@link ParsleyCausalBroadcast.WatermarkOutcome#learnedSomethingNew()}, via {@link #foldMarkerCompleteness}).
 * A marker's own delivery is never itself treated as a reason to relay further — unlike a genuine
 * business record, whose delivery always unconditionally causes this node to emit something on its own
 * sink (see {@link #delivered}). Gating on data-taught-something rather than on "a record was delivered"
 * is what keeps a topology cycle — a marker-only passthrough channel included — from ping-ponging the
 * same marker forever: a node that has already converged with its peers has nothing new to say, and
 * simply stops, rather than needing separate per-edge "have I already relayed this" bookkeeping. A
 * dependency can only ever be created after something real has already been observed, so a marker that
 * taught nothing new could not have just formed a new dependency on that non-event either — skipping
 * the re-emission strands nothing.
 *
 * <p><strong>Passthrough topics.</strong> {@code passthroughTopics} (a subset of {@code topics})
 * names a topic this stage does not otherwise consume or produce — declared solely so its causal
 * progress reaches this task's frontier. It is wired as an ordinary extra source into this same processor node,
 * deserialised as raw {@code byte[]}/{@code byte[]} (never the stage's own {@code KIn}/{@code VIn} —
 * a passthrough topic's value schema is unrelated to this stage's business types). {@link #process}
 * and {@link #delivered} recognise it by its own source topic (never a header) and route it through the
 * ordinary {@link ParsleyCausalBroadcast} gate exactly like any other channel, but skip {@code delegate.process}
 * for it specifically, emitting a watermark instead. Critically, this check is per <em>released</em>
 * message, not per triggering record: a passthrough record's own delivery can, as a side effect of the
 * shared buffer/candidate-index, release an unrelated held business record in the very same batch — that
 * business record still reaches the real delegate correctly, on its own turn through {@link #delivered}'s
 * loop, keyed by its own topic, never the passthrough record that happened to trigger the drain.
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
    // A subset of topics wired as extra, raw byte[]/byte[] sources into this same processor node — a
    // topic this stage does not otherwise consume or produce (see the class Javadoc's "Passthrough
    // topics" paragraph). Empty for the ordinary case.
    private final Set<String> passthroughTopics;
    // The topics this stage produces. Feeds the partition-count parity check.
    private final Set<String> sinkTopics;
    // Every child node this processor forwards a control-plane record (watermark) to by
    // name — a stage's processor node addresses every such forward explicitly rather than a zero-arg
    // broadcast. See ParsleyProcessorContext.forward.
    private final List<String> sinkNodeNames;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final @Nullable ParsleyQuiesce quiesce;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Source topic name -> stable UUID, resolved from the broker at init() (the topology decorator
    // has no broker config until then). Used by ingest() to stamp each record's causal identity.
    private Map<String, Uuid> topicUuids = Map.of();
    // This stage's own sink topics, name -> UUID, best-effort resolved at init() (see
    // resolveSinkTopicUuids) — the UUIDs feed causalBroadcast() so ParsleyCausalBroadcast can strip a node's own
    // produced coordinates from any inbound dependency/marker clock, and the name keys translate the
    // producer-ack registry's topic names into UUID identity for the ownOutputs fold (D2). Never used
    // to route or gate an inbound record by itself.
    private Map<String, Uuid> sinkTopicUuids = Map.of();
    // Each resolved sink topic's per-partition end offsets, captured at init() alongside the UUID
    // resolution (same best-effort admin session) — the ownOutputs seed claims endOffset - 1, the
    // sink's last appended position, per partition (D2/O1; an over-claim that is I8-sound and heals
    // the "f" blob trailing the last transaction's acks). A sink that could not be described is
    // simply absent, like its UUID.
    private Map<String, Map<Integer, Long>> sinkEndOffsets = Map.of();
    // The effective producer delivery.timeout.ms, resolved at init(). Bounds the crossing wait (a
    // send unacked past it has failed — the wait must throw, A8) and doubles as the A9 stall
    // threshold (past it, no in-flight upstream send can still land at the claimed position, so a
    // hold above highestReceived is a genuine stall, not latency). Leaning on the existing producer
    // config rather than a new knob — its deadline is exactly the boundary both uses care about.
    private long deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS;
    // A marker forward's exact destination set — every declared sink at this task's own partition
    // (ParsleyMarkerPartition routes every marker there) — excluded from the marker stamp's crossing
    // wait: same-coordinate pending sends are covered by partition FIFO + I3, and the cross-sink
    // exemption is O4's recorded null-message exemption. Business forwards never get an exclusion
    // (their destination partition is unknowable at stamp time; see ParsleyCausalBroadcast#broadcast).
    private Set<TopicPartition> markerDestinations = Set.of();

    // The most-recent business key seen on this task's owned partition — reused to route a self-injected
    // marker back to that partition lane (null until the first record).
    private @Nullable KIn lastSeenKey;

    private ProcessorContext<KOut, VOut> context;
    // The task's one causal-broadcast core, built once at init() over the task's state stores. Exactly one Processor
    // instance ever touches these stores within a task (passthrough topics are wired as extra sources
    // into this SAME node, never as a separate processor node), so the cached ParsleyChannels's
    // in-memory copy of the persisted state cannot diverge from a concurrent writer — there is none.
    private ParsleyCausalBroadcast<KIn, VIn> causalBroadcast;
    // The completeness as of the task's last committed transaction, snapshotted at each commit-cycle
    // flush (see ParsleyCommittedCompleteness). The store survives until the coordination subsystem
    // is deleted outright (T3.3); nothing reads the snapshot any more.
    private ParsleyCommittedCompleteness commitHook;
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
    // per-record forward count and emit a watermark when the delegate forwarded nothing. The stamp
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
                     ParsleyConfig config,
                     @Nullable ParsleyQuiesce quiesce) {
        this(delegate, serializer, frontierStoreName, bufferStoreName, candidateIndexStoreName,
                forwardedIndexStoreName, topics, Set.of(), sinkTopics, sinkNodeNames,
                adminFactory, config, quiesce);
    }

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     Set<String> topics,
                     Set<String> passthroughTopics,
                     Set<String> sinkTopics,
                     List<String> sinkNodeNames,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     ParsleyConfig config,
                     @Nullable ParsleyQuiesce quiesce) {
        this.delegate = delegate;
        this.serializer = serializer;
        this.passthroughTopics = passthroughTopics;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.topics = topics;
        this.sinkTopics = sinkTopics;
        this.sinkNodeNames = sinkNodeNames;
        this.adminFactory = adminFactory;
        this.config = config;
        this.quiesce = quiesce;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.topicUuids = resolveTopicUuids(context);
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.candidateIndexStore = context.getStateStore(candidateIndexStoreName);
        this.forwardedIndexStore = context.getStateStore(forwardedIndexStoreName);
        this.commitHook = context.getStateStore(ParsleyStores.commitHookName(frontierStoreName));

        boolean restored = frontierStore.get(ParsleyStores.FRONTIER_KEY) != null;

        this.wiredMetrics = ParsleyMetrics.wire(context);

        // The crossing-wait bound / A9 stall threshold, and every marker forward's destination set —
        // both fixed for the task's lifetime, resolved before the causal-broadcast core binds them.
        this.deliveryTimeoutMs = deliveryTimeoutMs(context.appConfigs());
        Set<TopicPartition> destinations = new HashSet<>();
        for (String sink : sinkTopics) {
            destinations.add(new TopicPartition(sink, context.taskId().partition()));
        }
        this.markerDestinations = Set.copyOf(destinations);

        // Build the task's one causal-broadcast core: constructs the real ParsleyChannels (restoring from the store if
        // restored is true), prunes/seeds it to this task's current scope, and persists. Cached for the
        // processor's whole lifetime — see buildCausalBroadcast().
        this.causalBroadcast = buildCausalBroadcast();
        ParsleyCausalBroadcast<KIn, VIn> causalBroadcast = this.causalBroadcast;
        if (restored) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), causalBroadcast.frontier());
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }

        // Seed the commit hook with the restored completeness (rebuilt from the committed changelog,
        // durable by definition) and hand it the live supplier it snapshots at each commit-cycle
        // flush. Nothing reads the snapshot any more; the store dies with the coordination
        // subsystem's deletion (T3.3).
        commitHook.bind(() -> causalBroadcast().completeness(), causalBroadcast.completeness());

        this.stampingContext = new ParsleyProcessorContext<>(
                context, causalBroadcast, () -> Optional.ofNullable(deliveryMetadata), sinkNodeNames);
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
                    // The A9 stalled-dependency scan (O(buffer × deps)) rides the same tick, never
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
     * causal state stores: passthrough topics are wired as extra <em>sources into this same processor
     * node</em> ({@link CausalTopology}), never as a separate processor node sharing the stores. (An
     * earlier design anticipated such a separate node and rebuilt the core — a full buffer scan,
     * candidate re-index, and frontier-blob re-persist — at the top of every operation to keep two
     * hypothetical instances coherent; that made every operation O(buffer-depth) for a sharer that was
     * never built.)
     */
    private ParsleyCausalBroadcast<KIn, VIn> causalBroadcast() {
        return causalBroadcast;
    }

    /**
     * Builds the causal-broadcast core over this task's state stores: constructs the {@link ParsleyChannels}
     * (restoring the frontier clock and channel clocks from the {@code "f"} blob when
     * present), prunes restored state to this task's current scope, seeds a channel entry for every
     * consumed input, and wires the buffer, candidate index, and forwarded index. Called exactly once,
     * from {@link #init}; {@link #wiredMetrics} must already be set.
     */
    private ParsleyCausalBroadcast<KIn, VIn> buildCausalBroadcast() {
        ParsleyBufferStore<KIn, VIn> buffer = new StoreBackedBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new StoreBackedCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new StoreBackedForwardedIndex(forwardedIndexStore);
        // The single owner of the persisted causal metadata: loads the frontier clock and channel
        // clocks from key "f" of the frontier store and rewrites that value on change. The forwarded
        // index keeps its own keyed store and is injected here.
        ParsleyChannels channels = new ParsleyChannels(frontierStore, forwardedIndex);

        // The coordinates this task consumes: a registered input topic, on the partition this task
        // owns. Streams co-partitions a sub-topology's sources, so the task owns partition
        // taskId().partition() of every input topic. Derived here, never persisted, so it is
        // recomputed identically after a rebalance. Used for restore-time pruning — not as a delivery
        // filter (the gate waits for every channel; see completeness()).
        Set<Uuid> consumedTopicIds = Set.copyOf(topicUuids.values());
        int taskPartition = context.taskId().partition();
        ParsleyVectorClock.CoordinatePredicate inScope = (topicId, partition) ->
                partition == taskPartition && consumedTopicIds.contains(topicId);

        // Reconcile restored causal state with the currently declared input set (the #21 fix: the
        // scope decision keys on "input set unchanged since the persisted blob", not blob presence
        // alone). Retired ancestry re-homes into the carried-ancestry clock the stamp keeps merging
        // (A6); an added input's frontier seeds at the carried-ancestry value so its already-carried
        // prefix is skipped, never replayed as live (A5); a recreated input's old UUID is destroyed.
        // Then seed an entry for every consumed input
        // channel so a channel that has not yet advertised anything is present in the min (holding it
        // down until it does), rather than absent — which would let a record deliver before that
        // channel confirmed the dependency. Idempotent against an already-rescoped/seeded store (the
        // common case, every call after the first), so this costs a redundant write, never a wrong one.
        Map<String, Uuid> previousInputs = channels.declaredInputs();
        channels.rescope(topicUuids, taskPartition);
        logScopeDiff(previousInputs);
        for (Uuid topicId : consumedTopicIds) {
            channels.channelUpdate(topicId, taskPartition, ParsleyVectorClock.empty());
        }

        // Wire the ownOutputs clock (D2): bind the producer-ack registry (when this task runs under
        // a CausalStreams instance — a TopologyTestDriver run has none) so every stamp's preceding
        // fold can translate acked sink names to UUID identity — the registry is also the
        // pending-send view the crossing wait blocks on before each stamp (O1/A7), bounded by the
        // producer's delivery.timeout.ms (past it the unacked send has failed, so the wait throws
        // and the transaction dies with it, A8) — then seed each resolved sink
        // partition at its end offset - 1 — the sink's last appended position. The seed is an
        // over-claim (it covers siblings' records on a shared sink, aborted tails, and markers) and
        // is I8-sound for exactly that reason; it also heals the restored blob trailing the last
        // transaction's acks. Runs after rescope so a recreated input-sink's destroyed UUID is
        // already purged before its successor seeds.
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
            Map<Integer, Long> ends = sinkEndOffsets.get(sink.getKey());
            if (ends == null) {
                continue;
            }
            for (Map.Entry<Integer, Long> end : ends.entrySet()) {
                if (end.getValue() > 0) {
                    channels.acknowledge(sink.getValue(), end.getKey(), end.getValue() - 1);
                }
            }
        }

        // This stage's own sink topics — never a delivery-scope concern (inScope, above), but fed
        // to the core for the I8 reflected-claim diagnostic: an inbound claim on an own-sink
        // coordinate above the ownOutputs clock is worth seeing (a stale own-output view or an
        // untruthful peer stamp), never worth failing over. The gate treats reflected claims like
        // any other dependency (consumed → gated on local delivery; unconsumed → ignored, D1).
        Set<Uuid> ownSinkTopicIds = Set.copyOf(sinkTopicUuids.values());
        ParsleyVectorClock.CoordinatePredicate ownSinkTopics = (topicId, partition) -> ownSinkTopicIds.contains(topicId);

        return new ParsleyCausalBroadcast<>(channels, buffer, candidateIndex,
                wiredMetrics.metrics(), context::currentSystemTimeMs, inScope, ownSinkTopics);
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
        switch (classify(record)) {
            case WATERMARK -> {
                handleWatermark(record);
                return;
            }
            case BUSINESS -> { }
        }
        // A passthrough record's key/value are raw bytes, not genuine KIn/VIn values (see the class
        // Javadoc's "Passthrough topics" paragraph) — never recorded as the most-recent business key.
        boolean passthrough = isPassthroughRecord(record);
        if (!passthrough) {
            // Remember the most-recent business key seen on this task's owned partition; a source-layer
            // task reuses it to route a self-injected marker back onto that partition lane.
            lastSeenKey = record.key();
        }
        ParsleyMessage<KIn, VIn> ingested = ingest(record);
        ParsleyVectorClock completenessBefore = causalBroadcast().completeness();
        ParsleyCausalBroadcast.Outcome<KIn, VIn> outcome = causalBroadcast().receive(ingested);
        deliver(outcome.delivered());
        // Advertise this node's progress so downstream channel clocks advance gap-free. A delivered
        // record advertises through its business output's completeness stamp — or, if the delegate
        // forwarded nothing, the watermark emitted in deliver(). A consumed record that was buffered
        // produces neither, so emit a heartbeat watermark — but only when receiving it genuinely
        // advanced completeness (a coordinate's first sighting seeds the frontier), so an unrelated
        // held record does not flood downstream with no-op watermarks.
        if (outcome.delivered().isEmpty() && !causalBroadcast().completeness().equals(completenessBefore)) {
            // Key the heartbeat with the buffered record's own key so it routes to that record's
            // partition, matching where its eventual business output will land — except a passthrough
            // record, whose own key is not a genuine KIn/KOut value; lastSeenKey routes just as well
            // (ParsleyMarkerPartition ignores the key for routing regardless).
            forwardMarker(ParsleyHeader.WATERMARK, new byte[0], passthrough ? lastSeenKey : record.key());
        }
    }

    /**
     * Returns {@code true} if {@code record} arrived on a {@link #passthroughTopics} source — a domain
     * topic this stage does not otherwise consume or produce, wired only so its causal progress reaches
     * this task's frontier (see the class Javadoc's "Passthrough topics" paragraph). Its key/value are raw
     * bytes, never genuine {@code KIn}/{@code VIn} values.
     */
    private boolean isPassthroughRecord(Record<KIn, VIn> record) {
        if (passthroughTopics.isEmpty()) {
            return false;
        }
        return context.recordMetadata().map(RecordMetadata::topic).map(passthroughTopics::contains).orElse(false);
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
            // causalBroadcast.broadcast() — the node's completeness at forward time, sound across all
            // input branches (never a per-record frontier-snapshot merged with inbound deps, which was
            // correct only in single-layer topologies).
            deliveryMetadata = new ParsleyRecordMetadata(message.topic(), message.partition(), message.offset());
            if (passthroughTopics.contains(message.topic())) {
                // A passthrough message never reaches the delegate — its key/value are raw bytes, not a
                // genuine KIn/VIn value the delegate could make sense of (see the class Javadoc's
                // "Passthrough topics" paragraph). Its own delivery still needs advertising, exactly like a
                // genuinely non-emitting delegate invocation; lastSeenKey (not this message's own raw key)
                // keeps the watermark's routing key well-typed. This also correctly delivers a *different*
                // message this passthrough delivery released as a side effect (e.g. a held business
                // record depending on this coordinate) through the real delegate below, on that message's
                // own turn through this same loop — every released message is routed by its own topic,
                // never by whichever message triggered the drain.
                forwardMarker(ParsleyHeader.WATERMARK, new byte[0], lastSeenKey);
            } else {
                // User headers + the producer's dependencies only; the source coordinate is surfaced via
                // context.recordMetadata(), and ParsleyProcessorContext re-stamps the frontier on forward.
                stampingContext.resetForwardCount();
                delegate.process(new Record<>(message.key(), message.value(), message.timestamp(),
                        message.headersWithDependencies()));
                // If the delegate did not forward any business record for this input, emit a watermark
                // carrying the current completeness frontier so that downstream nodes still learn about
                // this node's causal progress. Without this, a non-emitting processor silently stalls
                // downstream completeness, breaking the inductive correctness of multi-layer topologies.
                if (stampingContext.forwardCount() == 0) {
                    // Reuse the delivered record's key so the stand-in watermark routes to the same
                    // partition its business output would have.
                    forwardMarker(ParsleyHeader.WATERMARK, new byte[0], message.key());
                }
            }
        }
        deliveryMetadata = null;
        updateQuiesceState();
    }

    /**
     * Reports this task's current buffer-drained state to {@link #quiesce} (if registered). Called
     * after every buffer-depth-changing event (every path that can
     * hold or release a record funnels through {@link #delivered}), so the signal reflects the
     * current buffer depth without polling. Never fabricates completeness — it only observes the buffer
     * depth the ordinary delivery path already produced. Quiesce additionally gates its drained flag on
     * {@link ParsleyQuiesce#isQuiesceRequested()}.
     */
    private void updateQuiesceState() {
        boolean empty = causalBroadcast().bufferSize() == 0;
        if (quiesce != null) {
            quiesce.setDrained(context.taskId().toString(), quiesce.isQuiesceRequested() && empty);
        }
    }

    /** Which Parsley protocol marker, if any, {@link #classify} identified a record as. */
    private enum RecordKind { WATERMARK, BUSINESS }

    /**
     * Classifies {@code record} by a single pass over its headers, identifying a Parsley protocol
     * watermark ({@link ParsleyHeader#WATERMARK}) —
     * never by a record's key, which for a marker carries the triggering record's key for routing. A
     * marker carries no business payload and must never be forwarded to the user delegate or buffered —
     * a watermark exists only to propagate causal completeness progress through non-emitting layers.
     * Anything else is {@link RecordKind#BUSINESS}.
     */
    private RecordKind classify(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.WATERMARK.equals(h.key())) {
                return RecordKind.WATERMARK;
            }
        }
        return RecordKind.BUSINESS;
    }

    /**
     * The one path every received marker takes through the
     * core's {@link ParsleyCausalBroadcast#onWatermark channel-clock fold}. It does two independent things, and
     * the distinction is the crux of correctness here:
     * <ol>
     *   <li><strong>Always</strong> delivers the marker's own offset into this channel's contiguous
     *       frontier (via {@code onWatermark}), releasing anything that offset satisfies. A marker
     *       occupies a real offset on its partition, so the frontier's gap-free absorb walk must count
     *       it or it stalls below the marker forever — stranding every later record on that channel that
     *       waits on anything. This happens even when the completeness header is absent or corrupt.</li>
     *   <li>Folds the marker's carried completeness (the {@link ParsleyHeader#CAUSAL_DEPENDENCIES}
     *       header, present on every relayed marker) into the channel clock — the outbound-stamp input,
     *       never the gate. A missing or undecodable header is treated as an <em>empty</em> clock: the
     *       marker teaches the channel no new peer progress, but its offset is still absorbed.</li>
     * </ol>
     *
     * <p>Treating a decode failure as empty-but-still-delivered (rather than an early return) is what
     * fixed the bug where a corrupt marker permanently gapped the frontier.
     *
     * @return whether the carried clock genuinely taught this channel something new
     *         ({@link ParsleyCausalBroadcast.WatermarkOutcome#learnedSomethingNew()}) — {@code false} for an
     *         unregistered topic, an absent/undecodable header, or a clock this channel already
     *         dominates. This is the clock-news signal watermark relay gates on (see the
     *         class Javadoc); it is never the offset-delivery signal, which is unconditional.
     */
    private boolean foldMarkerCompleteness(Record<KIn, VIn> record) {
        RecordMetadata meta = requireRecordMetadata();
        String topic = meta.topic();
        int partition = meta.partition();
        long offset = meta.offset();
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            // Not expected in a correctly wired topology, but fail safe rather than crash so a
            // misconfiguration does not fail a healthy task.
            log.warn("Received marker on unregistered topic '{}'; ignoring", topic);
            return false;
        }
        ParsleyVectorClock frontierClock = ParsleyVectorClock.empty();
        Header dependencies = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
        if (dependencies != null && dependencies.value() != null) {
            try {
                frontierClock = ParsleyVectorClock.fromBytes(dependencies.value());
            } catch (Exception e) {
                log.warn("Failed to decode marker completeness on {}-{}; treating as empty",
                        topic, partition, e);
            }
        }
        // Deliberately OUTSIDE the decode catch: past this point a failure is a delivery failure, not a
        // decode failure — onWatermark has already advanced the frontier and removed released records
        // from the buffer, so swallowing an exception from the user delegate (or the core's own
        // fail-fast paths) would let the task commit past records the delegate never processed, silently
        // losing them. Fail-closed: let it kill the task.
        ParsleyCausalBroadcast.WatermarkOutcome<KIn, VIn> watermarkOutcome =
                causalBroadcast().onWatermark(topicId, partition, offset, frontierClock);
        deliver(watermarkOutcome.outcome().delivered());
        return watermarkOutcome.learnedSomethingNew();
    }

    /**
     * Handles a received protocol watermark: decodes its carried completeness frontier, updates the
     * per-channel clock for the watermark's source channel, releases any records newly satisfying the
     * gate, and re-emits a watermark downstream carrying this node's updated completeness frontier —
     * but only when this watermark genuinely taught the channel something new. The watermark is never
     * forwarded to the user delegate and never buffered.
     *
     * <p>The downstream re-emission (inductive propagation) ensures that a node which never produces
     * business records on this path still advertises its causal progress, so a grandchild node's
     * channel clock can advance without any business record on the path. But a watermark's own delivery
     * is never itself a reason to relay further — only a genuine change to what this channel knows is —
     * or a topology cycle (a marker-only passthrough channel included) would ping-pong the same
     * watermark forever (clock-invisible markers; see the class Javadoc).
     */
    private void handleWatermark(Record<KIn, VIn> record) {
        // Fold the carried completeness and absorb the watermark's own offset (see foldMarkerCompleteness),
        // then re-emit downstream only when this watermark genuinely advanced the channel, so the
        // completeness boundary still propagates through non-subscribing layers when it carries real
        // news, without ping-ponging a marker that taught this node nothing around a topology cycle.
        // Reuse the incoming watermark's own key: upstream keyed it to route to this partition, so
        // re-emitting under the same key keeps the propagated watermark on the co-partitioned
        // downstream partition.
        if (foldMarkerCompleteness(record)) {
            forwardMarker(ParsleyHeader.WATERMARK, new byte[0], record.key());
        }
    }

    /**
     * Forwards a protocol watermark ({@link ParsleyHeader#WATERMARK}) carrying this node's current
     * {@link ParsleyCausalBroadcast#completeness() completeness} in the {@link ParsleyHeader#CAUSAL_DEPENDENCIES}
     * header, so one record both signals the marker and advances the downstream channel clock.
     *
     * <p>Sent to every business sink ({@link #sinkNodeNames} — never a zero-arg broadcast, so a sibling
     * child node with an incompatible type never receives one; see {@link ParsleyProcessorContext}),
     * keyed with {@code triggerKey} — the key of the input record that triggered this emission, carried
     * through as informational wire content, not for routing: {@link ParsleyMarkerPartition} (set by
     * {@link #forwardToSinks}) routes the marker to this task's own owned partition regardless of
     * {@code triggerKey}, including when it is {@code null} — so a downstream task's channel clock for
     * that partition always advances across a sink boundary, never dependent on a business key having
     * been observed yet.
     *
     * <p>The marker carries a null value and is distinguished from a business tombstone by its
     * {@code markerHeader}; downstream Parsley consumers skip it by that header, not by its key. It
     * bypasses the business-forward counter in {@link ParsleyProcessorContext} (it is forwarded through
     * the raw context, not the stamping proxy) so it does not prevent watermark emission for a
     * genuinely non-emitting delegate invocation. Its completeness header is attached by the same
     * {@link ParsleyCausalBroadcast#broadcast} request that stamps business forwards — the single
     * stamping site — so a marker's stamp and a business record's stamp cannot diverge by construction.
     *
     * <p>The {@code KIn}-to-{@code KOut} cast is sound under the co-partitioning contract: a causal
     * processor must not change the key across the node (doing so reshards the causally-related
     * events), so the input and output key types coincide.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardMarker(String markerHeader, byte[] markerValue, @Nullable KIn triggerKey) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(markerHeader, markerValue);
        // Stamped with the current wall clock, never 0L: a marker's timestamp carries no causal
        // meaning (only its headers do), but it does drive broker time-based retention — a sink
        // segment holding only 0L-timestamped markers (a marker-only passthrough channel) would look
        // expired the moment it rolled and be deleted before a slow consumer read it. A marker's
        // destination is exact — every sink at this task's own partition — so its crossing wait
        // excludes exactly that set (see the markerDestinations field).
        forwardToSinks(causalBroadcast().broadcast(
                new Record<>((KOut) (Object) triggerKey, null, context.currentSystemTimeMs(), headers),
                markerDestinations));
    }

    /**
     * Forwards a control-plane record (a watermark) to every business sink by name, or
     * broadcasts (Kafka Streams' own zero-arg {@code forward}) when {@link #sinkNodeNames} is empty —
     * the common case, with no incompatibly-typed sibling sink configured. Named forwarding is required
     * the moment one is a sibling child of this processor's business sink(s): see
     * {@link ParsleyProcessorContext}.
     *
     * <p>The sole call site for every marker forward (business forwards go through the stamping proxy,
     * never here), so this is also the sole place {@link ParsleyMarkerPartition} is set: {@link
     * CausalTopology} installs a {@link ParsleyMarkerPartitioner} on every sink this stage declares,
     * which reads this override to route the marker to this task's own owned partition — {@code
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
        } catch (ParsleyVectorClockResolutionException e) {
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
     * Handles an inbound record whose causal-dependencies header could not be decoded. Its real
     * dependencies are unknown, so forwarding it on the ordinary path would deliver on an unknown
     * premise — never permitted, so this fails the task fast: the record was never buffered and its
     * source offset is not committed past it, so it is reprocessed on restart.
     */
    private ParsleyVectorClockResolutionException onUnresolvableClock(ParsleyVectorClockResolutionException e) {
        wiredMetrics.metrics().recordClockResolutionError();
        log.error("Unresolvable causal-dependencies header on {}-{} @{}; failing fast (fail-closed). "
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
        Map<String, Integer> partitionCounts;
        Map<String, String> cleanupPolicies;
        Map<String, String> sourceCleanupPolicies;
        try (ParsleyTopicAdmin admin = adminFactory.apply(context.appConfigs())) {
            resolved = admin.topicIds(topicList);
            Map<String, Integer> sourcePartitionCounts = admin.partitionCounts(topicList);
            partitionCounts = new HashMap<>(sourcePartitionCounts);
            partitionCounts.putAll(additionalTopicInfo(admin, "partition count", ParsleyTopicAdmin::partitionCounts));
            cleanupPolicies = additionalTopicInfo(admin, "cleanup.policy", ParsleyTopicAdmin::cleanupPolicies);
            // Source cleanup.policy is resolved separately, over the input topics (which must already
            // exist), and always — never gated by parsley.topology.validation — because a compacted
            // source is a correctness hazard for the skip-bridge, not a topology lint (see
            // validateSourcesNotCompacted).
            sourceCleanupPolicies = admin.cleanupPolicies(topicList);
            this.sinkTopicUuids = resolveSinkTopicUuids(admin);
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
        // Validate outside the resolve try/catch so a strict-mode failure surfaces as itself rather
        // than being wrapped as a broker-reachability error.
        validateSourcesNotCompacted(sourceCleanupPolicies);
        validatePartitionParity(partitionCounts);
        validateCleanupPolicy(cleanupPolicies);
        return resolved;
    }

    /**
     * Best-effort, per-topic UUID resolution over {@link #sinkTopics} — unlike {@link
     * #additionalTopicInfo}, this always runs, never gated by {@code parsley.topology.validation}: it
     * feeds the {@code ownOutputs} fold's name → UUID translation (D2) and {@link
     * #causalBroadcast}'s I8 reflected-claim diagnostic — correctness and observability
     * mechanisms, not topology-misconfiguration lints. Also captures each resolved sink's
     * per-partition end offsets ({@link #sinkEndOffsets}) in the same admin session, for the
     * {@code ownOutputs} init-time seed.
     *
     * <p>A sink that does not exist yet is skipped, and — because this resolution runs once, at
     * {@code init()}, and is never re-attempted — the ownOutputs fold and the reflected-claim
     * diagnostic for that topic stay off for this task instance's whole lifetime, until the next
     * restart re-runs {@code init()}. Delivery safety never depends on this resolution: a
     * dependency reflecting the not-yet-resolved sink is handled by the ordinary two-branch gate
     * (consumed → gated on local delivery; unconsumed → ignored, D1). Nothing can depend on the
     * sink before its first record exists, so the exposure starts only at first produce and ends
     * at the next init.
     */
    private Map<String, Uuid> resolveSinkTopicUuids(ParsleyTopicAdmin admin) {
        Map<String, Uuid> ids = new HashMap<>();
        Map<String, Map<Integer, Long>> endOffsets = new HashMap<>();
        for (String topic : sinkTopics) {
            Uuid id = null;
            try {
                id = admin.topicIds(List.of(topic)).get(topic);
            } catch (Exception e) {
                log.warn("Could not resolve topic id for sink topic '{}' (it may not exist yet); "
                        + "own-output tracking and the reflected-claim diagnostic for it stay off "
                        + "until the next restart re-resolves it", topic, e);
            }
            if (id == null) {
                continue;
            }
            ids.put(topic, id);
            try {
                endOffsets.put(topic, Map.copyOf(admin.endOffsets(topic)));
            } catch (Exception e) {
                log.warn("Could not read end offsets for sink topic '{}'; the ownOutputs seed for it "
                        + "is skipped this start (live acks still fold; the next restart re-seeds)",
                        topic, e);
            }
        }
        this.sinkEndOffsets = Map.copyOf(endOffsets);
        return ids;
    }

    /**
     * Best-effort, per-topic resolution of {@code describe} over {@link #sinkTopics}
     * — extra topics (e.g. a {@link CausalStreams} sink) folded into validation without being
     * consumed. Unlike a registered input buffer, such a topic is not required to exist yet (a sink
     * is often auto-created on first write), so a topic that cannot be described is logged and
     * omitted from the result rather than failing the task.
     *
     * <p>Resolved <strong>one topic at a time</strong>, deliberately not batched: the real
     * {@link ParsleyTopicAdmin} calls this delegates to (via {@code describeTopics}/
     * {@code describeConfigs}) fail their <em>entire</em> batch if any single topic in it errors, so
     * batching all sink topics together would let one not-yet-created sink mask a genuine
     * misconfiguration on another, already-existing sink — even under {@code strict}. Skipped
     * entirely under {@code parsley.topology.validation=off}, so a disabled check costs no admin
     * round-trip.
     */
    private <T> Map<String, T> additionalTopicInfo(
            ParsleyTopicAdmin admin, String what, TopicDescriptor<T> describe) {
        if (sinkTopics.isEmpty()
                || config.topologyValidation() == ParsleyConfig.ValidationMode.OFF) {
            return Map.of();
        }
        Map<String, T> result = new HashMap<>();
        for (String topic : sinkTopics) {
            try {
                result.putAll(describe.describe(admin, List.of(topic)));
            } catch (Exception e) {
                log.warn("Could not resolve {} for sink topic '{}' (it may not exist yet); "
                        + "skipping the check for it", what, topic, e);
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
     * Warns or fails (per {@code parsley.topology.validation}) when the causal input topics — and,
     * when built through {@link CausalStreams}, this stage's sink topics too — do not share a
     * partition count. Co-partitioning requires an equal partition count across all causally-related
     * topics so that a single task owns the complete partition set for a related group; unequal
     * counts make that impossible and let the completeness frontier evaluate against an incomplete
     * partition set. A single topic in the check (or {@code off}) is always vacuously fine.
     *
     * <p>Note the produce-time consequence a mismatch carries under {@code warn}: {@link
     * ParsleyMarkerPartitioner} routes a protocol marker to this task's own owned partition
     * ({@code taskId().partition()}) unconditionally, so a sink with fewer partitions than a source
     * makes the marker produce fail outright, crash-looping the task. {@code strict} turns that
     * into the startup failure it really is; the default stays {@code warn} for parity with the
     * pre-coordination behaviour (the escalation that rode on the epoch subsystem left with it).
     */
    private void validatePartitionParity(Map<String, Integer> partitionCounts) {
        ParsleyConfig.ValidationMode mode = config.topologyValidation();
        if (mode == ParsleyConfig.ValidationMode.OFF || partitionCounts.size() < 2) {
            return;
        }
        if (Set.copyOf(partitionCounts.values()).size() <= 1) {
            return;
        }
        String detail = "causal topics have mismatched partition counts " + partitionCounts
                + "; co-partitioning requires an equal partition count across all causally-related "
                + "topics so each task owns the complete partition set for a related group";
        if (mode == ParsleyConfig.ValidationMode.STRICT) {
            throw new IllegalStateException("parsley.topology.validation=strict: " + detail);
        }
        log.warn("parsley.topology.validation=warn: {}", detail);
    }

    /**
     * The effective producer {@code delivery.timeout.ms}: the {@code producer.}-prefixed override
     * wins, then the un-prefixed client config Streams also passes through, then Kafka's default.
     * See the {@link #deliveryTimeoutMs} field for the two uses (crossing-wait bound, A9 threshold).
     */
    private static long deliveryTimeoutMs(Map<String, Object> appConfigs) {
        Object configured = appConfigs.get("producer." + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        if (configured == null) {
            configured = appConfigs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        }
        if (configured instanceof Number number) {
            return number.longValue();
        }
        if (configured instanceof String text && !text.isBlank()) {
            try {
                return Long.parseLong(text.trim());
            } catch (NumberFormatException ignored) {
                // Malformed override — fall back to the default rather than fail init on a config typo.
            }
        }
        return DEFAULT_DELIVERY_TIMEOUT_MS;
    }

    /**
     * Warns or fails (per {@code parsley.topology.validation}) when a {@link CausalStreams} sink
     * topic's {@code cleanup.policy} includes {@code compact}. A protocol watermark is a null-key,
     * null-value record wire-indistinguishable from a compaction tombstone, so under compaction it
     * can be removed from the log before a slow consumer reads it — silently losing the completeness
     * frontier it carried. {@code compact,delete} is equally unsafe: compaction still runs.
     */
    /**
     * Fails the task, unconditionally, when a causal <em>source</em> topic has {@code cleanup.policy}
     * including {@code compact}. Unlike {@link #validateCleanupPolicy} (a sink lint, gated by {@code
     * parsley.topology.validation}), this is a correctness guard on the skip-bridge and is never gated —
     * same precedent as {@link #resolveSinkTopicUuids}. The bridge ({@link ParsleyChannels#bridge}) treats
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
                    + "cleanup.policy=delete on any topic consumed by a causal stage. This check is not "
                    + "governed by parsley.topology.validation because it guards causal correctness, not "
                    + "topology hygiene.");
        }
    }

    private void validateCleanupPolicy(Map<String, String> cleanupPolicies) {
        ParsleyConfig.ValidationMode mode = config.topologyValidation();
        if (mode == ParsleyConfig.ValidationMode.OFF) {
            return;
        }
        for (Map.Entry<String, String> entry : cleanupPolicies.entrySet()) {
            String policy = entry.getValue();
            if (policy == null || !policy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
                continue;
            }
            String detail = "sink topic '" + entry.getKey() + "' has cleanup.policy=" + policy
                    + "; a Parsley watermark is a null-value record wire-indistinguishable from a "
                    + "compaction tombstone and can be compacted away before a slow consumer reads it "
                    + "— set cleanup.policy=delete";
            if (mode == ParsleyConfig.ValidationMode.STRICT) {
                throw new IllegalStateException("parsley.topology.validation=strict: " + detail);
            }
            log.warn("parsley.topology.validation=warn: {}", detail);
        }
    }
}
