package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.StreamsConfig;
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
import java.util.function.Function;

/**
 * A Decorator (GoF) over a user {@link Processor}, gating delegation on the causal frontier: an incoming
 * record is held until the completeness frontier dominates its causal dependencies. Delivery is strictly
 * fail-closed — there is no eviction, buffer limit, or timeout that forwards a record ahead of its
 * dependencies. Every record that is delivered reaches {@code delegate.process(...)} exactly once,
 * and the state reads/writes the delegate performs and every record it forwards are causally ordered;
 * forwards are stamped with the current frontier by a {@link ParsleyProcessorContext}.
 *
 * <p>Held records are persisted to a changelog-backed buffer store and restored on {@code init}, so
 * they survive a restart (a buffered record's source offset is committed past it, so it would
 * otherwise be lost). The frontier-before-forward invariant from {@link ParsleyEngine} is preserved
 * on both the admit and punctuator paths.
 *
 * <p><strong>Clock-invisible markers.</strong> A received watermark or epoch-snapshot marker
 * ({@link #handleWatermark}, {@link #handleEpochSnapshot}) is relayed downstream only when it genuinely
 * taught this node's channel something it did not already know
 * ({@link ParsleyEngine.WatermarkOutcome#channelAdvanced()}, via {@link #foldMarkerCompleteness}).
 * A marker's own delivery is never itself treated as a reason to relay further — unlike a genuine
 * business record, whose delivery always unconditionally causes this node to emit something on its own
 * sink (see {@link #deliver}). Gating on data-taught-something rather than on "a record was delivered"
 * is what keeps a topology cycle — a marker-only passthrough channel included — from ping-ponging the
 * same marker forever: a node that has already converged with its peers has nothing new to say, and
 * simply stops, rather than needing separate per-edge "have I already relayed this" bookkeeping. A
 * dependency can only ever be created after something real has already been observed, so a marker that
 * taught nothing new could not have just formed a new dependency on that non-event either — skipping
 * the re-emission strands nothing.
 *
 * <p>The <strong>epoch-boundary</strong> marker ({@link #handleEpochBoundary}) is the one exception: it
 * relays on its channel's first sight of the boundary regardless of whether the carried clock advanced
 * anything ({@link ParsleyEngine.BoundaryOutcome#markerWasNew()} {@code || channelAdvanced}). A boundary
 * re-carries the completeness the preceding snapshot already taught the channel, so on an idle, quiesced
 * round it teaches nothing new — but the downstream still needs the marker on this channel to close its
 * own marker-on-every-channel transition window. The per-epoch, per-channel newly-recorded signal fires
 * exactly once per channel (a duplicate records nothing new), so a cycle still cannot ping-pong it. A
 * boundary is boundary news, not merely clock news.
 *
 * <p><strong>Passthrough topics.</strong> {@code passthroughTopics} (a subset of {@code topics}, wired
 * by {@link CausalTopology} from {@code parsley.coordination.domain-topics}) names a coordinated
 * domain's topic this stage does not otherwise consume or produce — declared solely so this member's
 * subscriptions cover the whole domain ({@link #validateFullMeshCoverage}) and so its causal progress
 * reaches this task's frontier. It is wired as an ordinary extra source into this same processor node,
 * deserialised as raw {@code byte[]}/{@code byte[]} (never the stage's own {@code KIn}/{@code VIn} —
 * a passthrough topic's value schema is unrelated to this stage's business types). {@link #process}
 * and {@link #deliver} recognise it by its own source topic (never a header) and route it through the
 * ordinary {@link ParsleyEngine} gate exactly like any other channel, but skip {@code delegate.process}
 * for it specifically, emitting a watermark instead. Critically, this check is per <em>released</em>
 * message, not per triggering record: a passthrough record's own delivery can, as a side effect of the
 * shared buffer/candidate-index, release an unrelated held business record in the very same batch — that
 * business record still reaches the real delegate correctly, on its own turn through {@link #deliver}'s
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

    private static final Duration EPOCH_POLL_INTERVAL = Duration.ofMillis(200);

    // Kafka's default max.poll.interval.ms, used when the app config does not set one explicitly.
    private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000L;
    // The topology-epoch join wait (on the StreamThread, in init()) is bounded to this fraction of
    // max.poll.interval.ms, leaving margin for the rest of init() and the next poll — so an admission
    // that cannot happen fails loudly (ParsleyJoinTimeoutException) before the broker silently evicts
    // the consumer into a rebalance crash-loop. See awaitJoinCommit / ParsleyJoinTimeoutException.
    private static final double JOIN_BUDGET_FRACTION = 0.9;

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final Set<String> topics;
    // A subset of topics wired as extra, raw byte[]/byte[] sources into this same processor node — a
    // domain topic this stage does not otherwise consume or produce (see the class Javadoc's "Passthrough
    // topics" paragraph). Empty for the ordinary case (no CausalTopology domain-topics configured).
    private final Set<String> passthroughTopics;
    // The topics this stage produces. Feeds the partition-count parity check and, when coordination is
    // configured, this member's declaration on the epoch-events log for the DAG-wide source-topic registry.
    private final Set<String> sinkTopics;
    // Every child node this processor forwards a control-plane record (watermark/epoch marker) to by
    // name — a stage's processor node addresses every such forward explicitly rather than a zero-arg
    // broadcast. See ParsleyProcessorContext.forward.
    private final List<String> sinkNodeNames;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final @Nullable ParsleyQuiesce quiesce;
    // The publisher for the snapshot frontier. Non-final: when coordination is configured, init() installs
    // the runtime-backed publisher (append to the epoch-events log) over whatever was passed.
    private ParsleyEpochSnapshotPublisher snapshotPublisher;
    // The per-instance epoch coordination handle, or null when no topology coordination is configured
    // (epoch 0). Resolved to the shared runtime at init(); the source-topic registry is derived from the
    // log, not carried here.
    private final @Nullable ParsleyCoordination coordination;
    // The shared epoch runtime resolved from the coordination handle at init(), or null in epoch 0. A
    // source-layer task polls it to initiate the in-band snapshot/boundary waves.
    private @Nullable ParsleyEpochRuntime epochRuntime;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Source topic name -> stable UUID, resolved from the broker at init() (the topology decorator
    // has no broker config until then). Used by ingest() to stamp each record's causal identity.
    private Map<String, Uuid> topicUuids = Map.of();
    // This stage's own sink topics' UUIDs, best-effort resolved at init() (see resolveSinkTopicUuids) —
    // fed to engine() so ParsleyEngine can strip a node's own produced coordinates from any inbound
    // dependency/marker clock. Never used to route or gate an inbound record by itself.
    private Set<Uuid> sinkTopicUuids = Set.of();

    // The most-recent business key seen on this task's owned partition — reused to route a self-injected
    // marker back to that partition lane (null until the first record). And the last snapshot-round /
    // committed epoch this task has already acted on, so each is injected once. (Whether this task is
    // source-layer is derived per poll from the log's source-topic registry, not cached here.)
    private @Nullable KIn lastSeenKey;
    private long lastSnapshotRoundEpoch;
    private long lastAdoptedEpoch;
    // The snapshot round this task has already published its completeness for (log-driven, so every member
    // publishes — not just source-layer). Reset in memory on restart, so a task that crashes mid-round
    // re-publishes off the folded log alone; that keeps a blocked round from deadlocking when a member's
    // side-channel publish was lost but its Streams offset commit survived. -1 = none yet.
    private long lastPublishedRoundEpoch = -1;
    // This task's globally-unique member id on the shared epoch-events log: application.id + task id. The
    // task id alone collides across the many applications that make up a production causal DAG (each app's
    // "0_0" is a different node); the application.id prefix disambiguates them, while two instances of the
    // same app share it (they are the same logical member, only one live at a time).
    private String memberId = "";

    private ProcessorContext<KOut, VOut> context;
    // The task's one engine, built once at init() over the task's state stores. Exactly one Processor
    // instance ever touches these stores within a task (passthrough topics are wired as extra sources
    // into this SAME node, never as a separate processor node), so the cached ParsleyFrontier's
    // in-memory copy of the persisted state cannot diverge from a concurrent writer — there is none.
    private ParsleyEngine<KIn, VIn> engine;
    // The completeness as of the task's last committed transaction — the only clock ever published on
    // the non-transactional epoch-events side channel (see ParsleyCommittedCompleteness). In-band
    // stamps (forwards, watermark/marker headers) stay live: they ride the same transaction as the
    // records they stamp and abort with them.
    private ParsleyCommittedCompleteness commitHook;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> candidateIndexStore;
    private KeyValueStore<byte[], byte[]> forwardedIndexStore;
    // The one-time seed for a fresh ParsleyEpochState (see buildEngine()) — computed once at init()
    // from whether this task joined an already-established epoch (epochSeedEpochId > 0) or starts
    // fresh at epoch 0 (0, the sentinel: no real epoch is ever id 0). Consumed by buildEngine()'s one
    // ParsleyEpochState construction; a restored task's real state overrides it from the "f" blob.
    private ParsleyClock epochSeedFloor = ParsleyClock.empty();
    private long epochSeedEpochId;
    private ParsleyMetrics.Wired wiredMetrics;
    // Set once delegate.init() has returned, so close() closes the delegate only if it was actually
    // initialised. init() can throw before delegate.init() (e.g. an interrupted awaitJoinCommit on a
    // clean shutdown mid-join), after which Streams still calls close(); closing an un-inited delegate
    // — or dereferencing the still-null wiredMetrics — would mask the real init failure. See close().
    private boolean delegateInitialized;
    // The stamping proxy context handed to the delegate. Held here so deliver() can check the
    // per-record forward count and emit a watermark when the delegate forwarded nothing.
    private ParsleyProcessorContext<KOut, VOut> stampingContext;
    // The completeness clock stamped onto every record the delegate forwards (see
    // ParsleyProcessorContext). Read live by the stamping proxy at forward time and confined to the
    // task thread — both the delegate's forwards and its punctuator fires run on the StreamThread, so
    // no cross-thread visibility is needed. The epoch runtime's off-thread publication of a wedged
    // task's completeness rides a separate channel (commitHook::committed via registerLocalCompleteness,
    // whose own volatiles carry it across threads), not this field.
    private ParsleyClock stampCompleteness = ParsleyClock.empty();
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
                adminFactory, config, quiesce, ParsleyEpochSnapshotPublisher.NOOP);
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
                     @Nullable ParsleyQuiesce quiesce,
                     ParsleyEpochSnapshotPublisher snapshotPublisher) {
        this(delegate, serializer, frontierStoreName, bufferStoreName, candidateIndexStoreName,
                forwardedIndexStoreName, topics, passthroughTopics, sinkTopics, sinkNodeNames,
                adminFactory, config, quiesce, snapshotPublisher, null);
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
                     @Nullable ParsleyQuiesce quiesce,
                     ParsleyEpochSnapshotPublisher snapshotPublisher,
                     @Nullable ParsleyCoordination coordination) {
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
        // May be replaced at init() by the runtime-backed publisher when coordination is configured.
        this.snapshotPublisher = snapshotPublisher;
        this.coordination = coordination;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.memberId = memberId(context);
        this.topicUuids = resolveTopicUuids(context);
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.candidateIndexStore = context.getStateStore(candidateIndexStoreName);
        this.forwardedIndexStore = context.getStateStore(forwardedIndexStoreName);
        this.commitHook = context.getStateStore(ParsleyStores.commitHookName(frontierStoreName));

        boolean restored = frontierStore.get(ParsleyStores.FRONTIER_KEY) != null;

        // Resolve epoch coordination from the handle before computing the epoch-state seed: build/share
        // the per-instance runtime (from this task's appConfigs), install the runtime-backed snapshot
        // publisher, and join as a member, then block until this member is a running member. That block is
        // called unconditionally — its block-until-running rule decides per case: a fresh joiner or an
        // evicted-then-restarted member (not a running member) blocks until an epoch re-includes it, while
        // a normal restart (still a running member on the log) and a cold start (epoch 0) return at once.
        // NB: a restored task must NOT skip this — a member that crashed while evicted is restored yet must
        // still block until re-admitted, or it would resume under its stale floor and self-evict in a loop.
        if (coordination != null) {
            ParsleyEpochRuntime runtime = coordination.runtimeFor(context.appConfigs());
            this.epochRuntime = runtime;
            this.snapshotPublisher = runtime::publishFrontier;
            // Declare this member's input channels and sink topics so the fold can derive the DAG-wide
            // source-topic registry; then block until this member is a running member.
            runtime.join(memberId, topics, sinkTopics);
            coordination.awaitJoinCommit(runtime, memberId, joinBudget(context.appConfigs()));
            validateFullMeshCoverage(runtime);
        }

        // The seed for buildEngine()'s ParsleyEpochState, gating against the settled epoch. A fresh
        // task that joined an established epoch settles DIRECTLY at the committed floor F_{k+1}: it has
        // no in-flight prior-epoch records, so every below-floor replay record is pre-epoch history to
        // strip — no overlap window. Otherwise it starts fresh at epoch 0; a restored task's state
        // (settled floor plus any in-progress transition) is loaded from the frontier "f" blob by the
        // ParsleyFrontier constructor buildEngine() runs below — which is why this is only ever a
        // seed (see the epochSeedFloor/epochSeedEpochId field Javadoc).
        //
        // lastAdoptedEpoch is deliberately left at its default (0) even here: settling epochState
        // directly is purely local (this task has no in-flight prior-epoch records to gate), but
        // pollEpochCoordination()'s first poll must still get a genuine chance to relay the admitting
        // epoch's boundary downstream on this task's own external-source inputs (if any) — a downstream
        // task reachable only through this one's output, never touching that source topic directly,
        // has no other way to learn the floor advanced. Re-injecting it locally on this task's own
        // already-settled epochState is a no-op (ParsleyEpochState#onBoundary short-circuits at
        // epochId <= settledEpochId), so retrying costs nothing.
        if (epochRuntime != null && !restored) {
            // Read the id and its lower bounds together (ParsleyEpochRuntime#committedEpoch), not via two
            // independent volatile reads: a commit landing in between the two would otherwise pair a
            // fresher id with a stale bounds snapshot.
            ParsleyEpochRuntime.CommittedEpoch committed = epochRuntime.committedEpoch();
            this.epochSeedFloor = committed.lowerBounds();
            this.epochSeedEpochId = committed.epochId();
        }

        this.wiredMetrics = ParsleyMetrics.wire(context);

        // Build the task's one engine: constructs the real ParsleyFrontier (restoring from the store if
        // restored is true), prunes/seeds it to this task's current scope, and persists. Cached for the
        // processor's whole lifetime — see buildEngine().
        this.engine = buildEngine();
        ParsleyEngine<KIn, VIn> engine = this.engine;
        if (restored) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), engine.frontier());
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }

        // Initialise stampCompleteness from completeness() so the stamping proxy reflects the restored
        // channel-clock state (not just the in-scope frontier) from the first forward onward.
        this.stampCompleteness = engine.completeness();
        // Seed the commit hook with the restored completeness (rebuilt from the committed changelog,
        // durable by definition) and hand it the live supplier it snapshots at each commit-cycle
        // flush. Every side-channel publication below reads commitHook.committed(), never the live
        // clock — see ParsleyCommittedCompleteness for why.
        commitHook.bind(() -> engine().completeness(), engine.completeness());
        // Registers the committed-completeness snapshot so the shared runtime can publish this
        // member's completeness on its behalf from its own background thread if this task's thread is ever
        // wedged (e.g. sharing a StreamThread with a joiner blocked in awaitJoinCommit) and so can never run
        // pollEpochCoordination() itself. See ParsleyEpochRuntime#registerLocalCompleteness.
        if (epochRuntime != null) {
            epochRuntime.registerLocalCompleteness(memberId, commitHook::committed);
        }

        this.stampingContext = new ParsleyProcessorContext<>(
                context, () -> stampCompleteness, () -> Optional.ofNullable(deliveryMetadata), sinkNodeNames);
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
                    ParsleyEngine.Outcome<KIn, VIn> outcome = engine().drainAfterRestore();
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
                    engine().reportBufferState();
                    updateQuiesceState();
                });

        // A source-layer task self-initiates the in-band wave off the coordination log, so it must react
        // even while idle (no inbound records to piggy-back on). Poll the runtime on a wall-clock tick as
        // well as on process(). Scheduled only when coordination is configured — epoch 0 adds no tick.
        if (epochRuntime != null) {
            context.schedule(EPOCH_POLL_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> pollEpochCoordination());
        }

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
     * The task's one {@link ParsleyEngine}, built by {@link #buildEngine()} at {@code init()} and
     * cached for the processor's whole lifetime.
     *
     * <p>Caching is sound because exactly one {@link Processor} instance ever touches this task's
     * causal state stores: passthrough topics are wired as extra <em>sources into this same processor
     * node</em> ({@link CausalTopology}), never as a separate processor node sharing the stores. (An
     * earlier design anticipated such a separate node and rebuilt the engine — a full buffer scan,
     * candidate re-index, and frontier-blob re-persist — at the top of every operation to keep two
     * hypothetical instances coherent; that made every operation O(buffer-depth) for a sharer that was
     * never built.)
     */
    private ParsleyEngine<KIn, VIn> engine() {
        return engine;
    }

    /**
     * Builds the engine over this task's state stores: constructs the {@link ParsleyFrontier}
     * (restoring the frontier clock, channel clocks, and epoch state from the {@code "f"} blob when
     * present), prunes restored state to this task's current scope, seeds a channel entry for every
     * consumed input, and wires the buffer, candidate index, and forwarded index. Called exactly once,
     * from {@link #init}; {@link #wiredMetrics} and the {@code epochSeed*} fields must already be set.
     */
    private ParsleyEngine<KIn, VIn> buildEngine() {
        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new RocksCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new RocksForwardedIndex(forwardedIndexStore);
        // The single owner of the persisted causal metadata: loads the frontier clock and channel
        // clocks from key "f" of the frontier store and rewrites that value on change. The forwarded
        // index keeps its own keyed store and is injected here.
        ParsleyEpochState epochState = epochSeedEpochId > 0
                ? new ParsleyEpochState(epochSeedFloor, epochSeedEpochId)
                : new ParsleyEpochState();
        ParsleyFrontier frontier = new ParsleyFrontier(frontierStore, forwardedIndex, epochState);

        // The coordinates this task consumes: a registered input topic, on the partition this task
        // owns. Streams co-partitions a sub-topology's sources, so the task owns partition
        // taskId().partition() of every input topic. Derived here, never persisted, so it is
        // recomputed identically after a rebalance. Used for restore-time pruning — not as a delivery
        // filter (the gate waits for every channel; see completeness()).
        Set<Uuid> consumedTopicIds = Set.copyOf(topicUuids.values());
        int taskPartition = context.taskId().partition();
        ParsleyClock.CoordinatePredicate inScope = (topicId, partition) ->
                partition == taskPartition && consumedTopicIds.contains(topicId);

        // Prune restored causal state to the current scope — topic UUIDs change when a topic is
        // dropped and recreated, leaving stale frontier/channel entries that would pin the completeness
        // min on a coordinate that can never advance. Then seed an entry for every consumed input
        // channel so a channel that has not yet advertised anything is present in the min (holding it
        // down until it does), rather than absent — which would let a record deliver before that
        // channel confirmed the dependency. Idempotent against an already-pruned/seeded store (the
        // common case, every call after the first), so this costs a redundant write, never a wrong one.
        frontier.pruneToScope(inScope);
        for (Uuid topicId : consumedTopicIds) {
            frontier.channelUpdate(topicId, taskPartition, ParsleyClock.empty());
        }

        // This stage's own sink topics — never a delivery-scope concern (inScope, above), but fed to
        // the engine so it can strip a node's own produced coordinates from any inbound dependency or
        // marker clock; see ParsleyEngine's Javadoc on ownSinkTopics for why this is sound.
        Set<Uuid> ownSinkTopicIds = sinkTopicUuids;
        ParsleyClock.CoordinatePredicate ownSinkTopics = (topicId, partition) -> ownSinkTopicIds.contains(topicId);

        return new ParsleyEngine<>(frontier, buffer, candidateIndex,
                wiredMetrics.metrics(), context::currentSystemTimeMs, inScope, ownSinkTopics);
    }

    /**
     * This task's globally-unique member id for the shared epoch-events log: {@code application.id/taskId}.
     * The task id alone is not unique across the many applications of a production causal DAG (each app's
     * {@code 0_0} is a different node), so the {@code application.id} from the task's config disambiguates
     * them; two instances of the same app share it (the same logical member, only one live at a time).
     * Falls back to the bare task id when no {@code application.id} is configured (e.g. a test context).
     */
    private String memberId(ProcessorContext<KOut, VOut> context) {
        Object applicationId = context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG);
        String task = context.taskId().toString();
        return (applicationId == null || applicationId.toString().isEmpty()) ? task : applicationId + "/" + task;
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        switch (classify(record)) {
            case WATERMARK -> {
                handleWatermark(record);
                return;
            }
            case EPOCH_BOUNDARY -> {
                handleEpochBoundary(record);
                return;
            }
            case EPOCH_SNAPSHOT -> {
                handleEpochSnapshot(record);
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
        ParsleyClock completenessBefore = engine().completeness();
        ParsleyEngine.Outcome<KIn, VIn> outcome = engine().receive(ingested);
        deliver(outcome.delivered());
        // Advertise this node's progress so downstream channel clocks advance gap-free. A delivered
        // record advertises through its business output's completeness stamp — or, if the delegate
        // forwarded nothing, the watermark emitted in deliver(). A consumed record that was buffered
        // produces neither, so emit a heartbeat watermark — but only when receiving it genuinely
        // advanced completeness (a coordinate's first sighting seeds the frontier), so an unrelated
        // held record does not flood downstream with no-op watermarks.
        if (outcome.delivered().isEmpty() && !engine().completeness().equals(completenessBefore)) {
            // Key the heartbeat with the buffered record's own key so it routes to that record's
            // partition, matching where its eventual business output will land — except a passthrough
            // record, whose own key is not a genuine KIn/KOut value; lastSeenKey routes just as well
            // (ParsleyMarkerPartition ignores the key for routing regardless).
            forwardMarker(ParsleyHeader.WATERMARK, new byte[0], passthrough ? lastSeenKey : record.key());
        }
        // A source-layer task also checks the coordination log after each record, so a round that opened
        // (or an epoch that committed) is acted on promptly without waiting for the wall-clock tick.
        pollEpochCoordination();
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
        // Unconditional, not just for a genuine decommission: Kafka Streams calls close() whenever this
        // task stops running here, including a rebalance that migrates it to another instance. Without
        // this, this instance's runtime would keep treating the departed member as local forever — stuck
        // in allLocalMembersDrained()'s (never-refreshed) last report and in leaveLocalMembers()'s scope —
        // so a later leave() here could hang waiting on it or wrongly evict it while it runs on. The
        // member itself is unaffected: if the task is only migrating, the new instance's init() re-joins
        // it; unregistering here is purely local bookkeeping, not a log event.
        if (epochRuntime != null) {
            epochRuntime.unregisterMember(memberId);
        }
        // init() may have thrown before it finished — most plausibly an interrupted awaitJoinCommit
        // unwinding a clean shutdown mid-join (see ParsleyCoordination#awaitJoinCommit) — yet Streams
        // still calls close(). Close only what init() actually set up: closing an un-inited delegate, or
        // dereferencing the still-null wiredMetrics, would throw here and mask the real init failure with
        // a spurious NPE. (quiesce/epochRuntime cleanup above is already safe on a partial init: the
        // quiesce sets no-op on an unregistered id, and epochRuntime is set together with the join it
        // undoes.)
        if (delegateInitialized) {
            delegate.close();
        }
        if (wiredMetrics != null) {
            wiredMetrics.close(context.metrics());
        }
    }

    private void deliver(List<ParsleyMessage<KIn, VIn>> admitted) {
        for (ParsleyMessage<KIn, VIn> message : admitted) {
            // The stamp is the node's completeness frontier — the per-channel-min-then-frontier-max
            // boundary that is sound across all input branches. This replaces the old per-record
            // frontier-snapshot-merged-with-inbound-deps approach, which was correct only in
            // single-layer topologies. Downstream nodes receive the sound multi-layer boundary.
            stampCompleteness = engine().completeness();
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
        stampCompleteness = engine().completeness();
        updateQuiesceState();
    }

    /**
     * Reports this task's current buffer-drained state, to {@link #quiesce} (if registered) and to the
     * epoch runtime (if coordinated). Called after every buffer-depth-changing event (every path that can
     * hold or release a record funnels through {@link #deliver}), so the signal reflects the
     * current buffer depth without polling. Never fabricates completeness — it only observes the buffer
     * depth the ordinary delivery path already produced. Quiesce additionally gates its drained flag on
     * {@link ParsleyQuiesce#isQuiesceRequested()}; the runtime tracks the raw depth so
     * {@link ParsleyCoordination#leave()} can wait for a drained buffer before removing the member.
     */
    private void updateQuiesceState() {
        boolean empty = engine().bufferSize() == 0;
        if (quiesce != null) {
            quiesce.setDrained(context.taskId().toString(), quiesce.isQuiesceRequested() && empty);
        }
        if (epochRuntime != null) {
            epochRuntime.reportDrained(memberId, empty);
        }
    }

    /** Which Parsley protocol marker, if any, {@link #classify} identified a record as. */
    private enum RecordKind { WATERMARK, EPOCH_BOUNDARY, EPOCH_SNAPSHOT, BUSINESS }

    /**
     * Classifies {@code record} by a single pass over its headers, identifying a Parsley protocol
     * watermark ({@link ParsleyHeader#WATERMARK}), epoch-boundary marker ({@link
     * ParsleyHeader#EPOCH_BOUNDARY}), or epoch-snapshot marker ({@link ParsleyHeader#EPOCH_SNAPSHOT}) —
     * never by a record's key, which for a marker carries the triggering record's key for routing. Every
     * marker carries no business payload and must never be forwarded to the user delegate or buffered —
     * a watermark exists only to propagate causal completeness progress through non-emitting layers; the
     * epoch markers drive each node's local epoch transition and the Mattern-cut publish respectively.
     * Anything else is {@link RecordKind#BUSINESS}.
     */
    private RecordKind classify(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            String key = h.key();
            if (ParsleyHeader.WATERMARK.equals(key)) {
                return RecordKind.WATERMARK;
            }
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(key)) {
                return RecordKind.EPOCH_BOUNDARY;
            }
            if (ParsleyHeader.EPOCH_SNAPSHOT.equals(key)) {
                return RecordKind.EPOCH_SNAPSHOT;
            }
        }
        return RecordKind.BUSINESS;
    }

    /**
     * Handles a received epoch-snapshot marker: publishes this node's committed completeness frontier
     * ({@link ParsleyCommittedCompleteness}) via {@link ParsleyEpochSnapshotPublisher}, so the log's
     * fold can merge-min the published clocks into the next epoch's lower bounds. The marker is
     * never delivered to the user delegate and never buffered, but — unlike the earlier
     * coordinator-broadcast model — it is <strong>relayed</strong> downstream on the same key (the
     * leaderless in-band cut propagates edge by edge through the DAG) <em>only when it genuinely taught
     * this node's channel something new</em> ({@link #foldMarkerCompleteness}) — a marker's own
     * delivery is never itself a reason to relay further, or a topology cycle would ping-pong the same
     * marker forever (clock-invisible markers; see the class Javadoc). The relayed marker also carries
     * this node's completeness, so a single record both propagates the cut and advances the downstream
     * channel clock.
     */
    private void handleEpochSnapshot(Record<KIn, VIn> record) {
        snapshotPublisher.publish(memberId, commitHook.committed());
        boolean channelAdvanced = foldMarkerCompleteness(record);
        if (channelAdvanced) {
            forwardMarker(ParsleyHeader.EPOCH_SNAPSHOT, new byte[0], record.key());
        }
    }

    /**
     * Handles a received epoch-boundary marker: decodes it, records it on its source channel and (if the
     * transition is now ready) closes the epoch window in the engine, delivers any records the raised
     * floor releases, and <strong>relays the marker downstream</strong> on the same key so the boundary
     * propagates edge by edge through the DAG (the leaderless in-band model). It relays when the marker
     * was newly recorded for its epoch on this channel ({@link ParsleyEngine.BoundaryOutcome#markerWasNew()})
     * <em>or</em> its carried completeness taught this channel something new
     * ({@link #foldMarkerCompleteness}).
     *
     * <p>Boundary relay cannot gate on the clock signal alone the way watermark and snapshot relay do
     * (clock-invisible markers; see the class Javadoc). A boundary re-carries the very completeness the
     * preceding snapshot marker already advertised, so on an idle, quiesced round the boundary teaches
     * the channel nothing new and {@code channelAdvanced} is false — yet the downstream still needs the
     * marker on this channel to close its own marker-on-every-channel window. Gating on
     * {@code markerWasNew} propagates the boundary to every channel exactly once (a duplicate on an
     * already-seen channel records nothing new and does not relay), so a topology cycle still cannot
     * ping-pong it. A boundary is boundary news, not merely clock news. The relayed marker carries this
     * node's completeness, so a single downstream record both adopts the boundary and advances the
     * channel clock. The marker is never forwarded to the user delegate and never buffered.
     */
    private void handleEpochBoundary(Record<KIn, VIn> record) {
        RecordMetadata meta = requireRecordMetadata();
        String topic = meta.topic();
        int partition = meta.partition();
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            log.warn("Received epoch boundary on unregistered topic '{}'; ignoring", topic);
            return;
        }

        Header marker = record.headers().lastHeader(ParsleyHeader.EPOCH_BOUNDARY);
        byte[] boundaryBytes = marker == null ? null : marker.value();
        if (boundaryBytes == null) {
            return;
        }
        ParsleyEpochBoundary boundary;
        try {
            boundary = ParsleyEpochBoundary.fromBytes(boundaryBytes);
        } catch (Exception e) {
            log.warn("Failed to decode epoch boundary on {}-{}; ignoring", topic, partition, e);
            return;
        }

        // A relayed marker carries the upstream node's completeness; adopt it into this channel's clock
        // and absorb the marker's own offset (draining anything either releases) before driving the
        // transition, so one record does both.
        boolean channelAdvanced = foldMarkerCompleteness(record);

        ParsleyEngine.BoundaryOutcome<KIn, VIn> outcome = engine().onEpochBoundary(boundary, topicId, partition);
        deliver(outcome.outcome().delivered());

        // Relay the boundary downstream on the marker's key so it stays on the same partition lane and
        // every downstream task transitions its owned partitions. Relay when the marker was newly
        // recorded for its epoch on this channel OR its carried completeness taught this channel
        // something new. The clock signal alone is not enough: on an idle, quiesced round the boundary
        // carries the same completeness the preceding snapshot already advertised, so channelAdvanced is
        // false and gating on it alone would strand the transition at this node forever — the downstream
        // never sees the marker on this channel, so its marker-on-every-channel window never closes.
        // markerWasNew fires exactly once per channel per epoch (a duplicate records nothing new), so a
        // cyclic topology still cannot ping-pong it. See the class Javadoc's clock-invisible-markers
        // discussion, which governs watermark relay; a boundary is boundary news, not just clock news.
        if (outcome.markerWasNew() || channelAdvanced) {
            forwardMarker(ParsleyHeader.EPOCH_BOUNDARY, boundaryBytes, record.key());
        }
    }

    /**
     * The one path every received marker — watermark, epoch snapshot, epoch boundary — takes through the
     * engine's {@link ParsleyEngine#onWatermark channel-clock fold}. It does two independent things, and
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
     * fixed the bug where a corrupt marker permanently gapped the frontier; watermark and epoch handlers
     * used to diverge here, and only the watermark path got it right.
     *
     * @return whether the carried clock genuinely taught this channel something new
     *         ({@link ParsleyEngine.WatermarkOutcome#channelAdvanced()}) — {@code false} for an
     *         unregistered topic, an absent/undecodable header, or a clock this channel already
     *         dominates. This is the clock-news signal watermark and snapshot relay gate on (see the
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
        ParsleyClock frontierClock = ParsleyClock.empty();
        Header dependencies = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
        if (dependencies != null && dependencies.value() != null) {
            try {
                frontierClock = ParsleyClock.fromBytes(dependencies.value());
            } catch (Exception e) {
                log.warn("Failed to decode marker completeness on {}-{}; treating as empty",
                        topic, partition, e);
            }
        }
        // Deliberately OUTSIDE the decode catch: past this point a failure is a delivery failure, not a
        // decode failure — onWatermark has already advanced the frontier and removed released records
        // from the buffer, so swallowing an exception from the user delegate (or the engine's own
        // fail-fast paths) would let the task commit past records the delegate never processed, silently
        // losing them. Fail-closed: let it kill the task.
        ParsleyEngine.WatermarkOutcome<KIn, VIn> watermarkOutcome =
                engine().onWatermark(topicId, partition, offset, frontierClock);
        deliver(watermarkOutcome.outcome().delivered());
        return watermarkOutcome.channelAdvanced();
    }

    /**
     * A source-layer task's reaction to the coordination log: when a snapshot round opens, publish this
     * node's completeness and inject the snapshot marker downstream; when a new epoch commits, adopt the
     * epoch's floor for this task's external-source coordinates and inject the boundary marker downstream.
     * A no-op for a non-source-layer task (driven purely by received in-band markers) and for epoch 0
     * (no runtime). Called both from {@link #process} and a wall-clock punctuator so an idle source-layer
     * task still reacts.
     *
     * <p><strong>Handoff grace period.</strong> {@code externalSourceTopics()} is a live, memoryless view
     * of the log's current declarations: the instant some member declares a topic as a sink, that topic
     * stops being external DAG-wide, even before the declaring member is running and able to relay
     * anything in-band — it structurally cannot relay the very epoch whose round admits it, since it was
     * not a participant in the wave that computed that epoch's cut. Without a grace period, the outgoing
     * self-adopter (this task, if it was the one consuming that topic) would stop adopting for it in the
     * same poll the topic leaves the live registry, and nobody would ever inject that one epoch's boundary
     * onto it — a permanent gap for any downstream task that only learns of that coordinate's floor
     * through this one's relay. So boundary adoption targets {@code live ∪ }{@link
     * ParsleyEpochRuntime#externalSourceTopicsAsOfPreviousCommit()} — the registry as of the commit
     * <em>before</em> the one just adopted, which still includes a topic whose declaring producer's join
     * folded during the very round that admitted it, giving that topic exactly one more adoption cycle
     * from its outgoing self-adopter. Purely log-derived (see that method's Javadoc), unlike the per-task
     * in-memory cache this replaced: a task that crashes inside the handoff window computes the identical
     * answer on restart, from the log alone, with no local memory to lose.
     *
     * <p>{@link #adoptAndInjectBoundary}/{@link #injectSnapshot} relay unconditionally, whether or not
     * this task has seen a business record yet ({@link #lastSeenKey} may be {@code null}): {@link
     * ParsleyMarkerPartition} routes the marker to this task's own owned partition regardless of the key
     * carried on the record, so there is nothing to wait for and nothing to retry. Before that routing
     * existed, a relay with no key to route on had to be skipped and retried on a later poll once a key
     * became available — the source of the restart stall this Javadoc used to document as an open gap.
     */
    private void pollEpochCoordination() {
        ParsleyEpochRuntime runtime = epochRuntime;
        if (runtime == null) {
            return;
        }
        // Keep the runtime's drained mirror current even while idle (no deliveries drive updateQuiesceState),
        // so leave() observes a drained buffer promptly. Cheap and self-correcting on each poll tick.
        runtime.reportDrained(memberId, engine().bufferSize() == 0);
        // Every member — not just source-layer — publishes its completeness for an open round it still owes
        // one for, driven off the folded log. This makes publication restart-safe: a member that restarts
        // mid-round re-derives from the log that it owes a publication and re-publishes, without depending
        // on having consumed a one-shot in-band snapshot marker exactly once (the case timeout eviction used
        // to paper over). The per-round guard bounds it to one append per round per task lifetime; a restart
        // resets the guard, so a lost publish is re-sent. Publishes the committed (never live) completeness
        // — see ParsleyCommittedCompleteness; staleness is safe pre-cut, since completeness is monotonic
        // and the committed floor is a conservative merge-min.
        if (runtime.owesPublication(memberId)) {
            long round = runtime.committedEpochId() + 1;
            if (round != lastPublishedRoundEpoch) {
                snapshotPublisher.publish(memberId, commitHook.committed());
                lastPublishedRoundEpoch = round;
            }
        }
        // Whether this task is source-layer is derived per poll from the log's DAG-wide source-topic
        // registry: the external source topics (inputs no member produces) that this task actually consumes.
        // Derived, not configured, and re-read each poll because the registry changes as members join —
        // including, mid-round, dropping a topic a new member just declared as a sink (see above).
        Set<Uuid> liveExternalSourceTopicIds = resolveExternalSourceTopicIds(runtime.externalSourceTopics());
        // Read the id and its lower bounds together (see ParsleyEpochRuntime.CommittedEpoch's Javadoc): a
        // commit landing between two independent reads would otherwise stamp the relayed boundary with a
        // fresher id than the bounds it carries — a boundary that is then never re-adopted (the per-epoch
        // guard below only ever advances), leaving every downstream consumer merely conservative until the
        // next commit rather than outright wrong, but avoidable entirely by reading one snapshot.
        ParsleyEpochRuntime.CommittedEpoch committedEpoch = runtime.committedEpoch();
        long committed = committedEpoch.epochId();
        if (committed > lastAdoptedEpoch) {
            Set<Uuid> adoptionTargets = new HashSet<>(liveExternalSourceTopicIds);
            adoptionTargets.addAll(resolveExternalSourceTopicIds(runtime.externalSourceTopicsAsOfPreviousCommit()));
            if (!adoptionTargets.isEmpty()) {
                adoptAndInjectBoundary(new ParsleyEpochBoundary(committed, committedEpoch.lowerBounds()), adoptionTargets);
            }
            lastAdoptedEpoch = committed;
        }
        if (liveExternalSourceTopicIds.isEmpty()) {
            return;
        }
        if (runtime.isRoundOpen()) {
            long round = committed + 1;   // the epoch a commit of the currently open round would carry
            if (round != lastSnapshotRoundEpoch) {
                injectSnapshot();
                lastSnapshotRoundEpoch = round;
            }
        }
    }

    /**
     * Resolves {@code topicNames} — a log-derived external-source-topic name set, either {@link
     * ParsleyEpochRuntime#externalSourceTopics()} (the live registry) or {@link
     * ParsleyEpochRuntime#externalSourceTopicsAsOfPreviousCommit()} (the handoff grace set) — to the
     * broker UUIDs of the ones this task actually consumes. Non-empty for the live registry iff this task
     * is source-layer for the current registry.
     */
    private Set<Uuid> resolveExternalSourceTopicIds(Set<String> topicNames) {
        return topicNames.stream()
                .map(topicUuids::get)
                .filter(id -> id != null)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /**
     * Publishes this node's completeness for the open round (so the owner's merge-min includes it) and
     * relays the snapshot marker downstream, opening the in-band cut for the next layer. The relay is
     * keyed with {@link #lastSeenKey} when available (informational — a real business key on the wire is
     * still preferable to none) but routes correctly to this task's own owned partition either way, via
     * {@link ParsleyMarkerPartition}, so a task that has not yet seen a business record ({@code
     * lastSeenKey} still {@code null}) relays exactly the same as one that has.
     */
    private void injectSnapshot() {
        snapshotPublisher.publish(memberId, commitHook.committed());
        forwardMarker(ParsleyHeader.EPOCH_SNAPSHOT, new byte[0], lastSeenKey);
    }

    /**
     * Adopts {@code boundary} for this task's external-source coordinates — a topology-source channel's
     * floor arrives from the log, since no in-band marker will ever reach it (break #1) — then relays the
     * boundary downstream so the next layer transitions in-band. See {@link #injectSnapshot} for why the
     * relay needs no business key to route correctly.
     */
    private void adoptAndInjectBoundary(ParsleyEpochBoundary boundary, Set<Uuid> externalSourceTopicIds) {
        int partition = context.taskId().partition();
        List<ParsleyMessage<KIn, VIn>> released = new ArrayList<>();
        for (Uuid topicId : externalSourceTopicIds) {
            ParsleyEngine.BoundaryOutcome<KIn, VIn> outcome = engine().onEpochBoundary(boundary, topicId, partition);
            released.addAll(outcome.outcome().delivered());
        }
        deliver(released);
        forwardMarker(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes(), lastSeenKey);
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
     * Forwards a protocol marker — a watermark ({@link ParsleyHeader#WATERMARK}), an epoch-snapshot
     * marker ({@link ParsleyHeader#EPOCH_SNAPSHOT}), or an epoch-boundary marker
     * ({@link ParsleyHeader#EPOCH_BOUNDARY}) — carrying this node's current
     * {@link ParsleyEngine#completeness() completeness} in the {@link ParsleyHeader#CAUSAL_DEPENDENCIES}
     * header, so one record both signals the marker and advances the downstream channel clock. The
     * marker type is set by {@code markerHeader} (with {@code markerValue}: an empty array for a
     * watermark or snapshot, the encoded boundary for an epoch boundary).
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
     * genuinely non-emitting delegate invocation, and its completeness header is written here directly
     * rather than by the proxy.
     *
     * <p>The {@code KIn}-to-{@code KOut} cast is sound under the co-partitioning contract: a causal
     * processor must not change the key across the node (doing so reshards the causally-related
     * events), so the input and output key types coincide.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardMarker(String markerHeader, byte[] markerValue, @Nullable KIn triggerKey) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(markerHeader, markerValue);
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine().completeness().toBytes());
        // Stamped with the current wall clock, never 0L: a marker's timestamp carries no causal
        // meaning (only its headers do), but it does drive broker time-based retention — a sink
        // segment holding only 0L-timestamped markers (a marker-only passthrough channel) would look
        // expired the moment it rolled and be deleted before a slow consumer read it.
        forwardToSinks(new Record<>((KOut) (Object) triggerKey, null, context.currentSystemTimeMs(), headers));
    }

    /**
     * Forwards a control-plane record (watermark or epoch marker) to every business sink by name, or
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
     * Decodes {@code record} into a {@link ParsleyMessage}, ready for {@link ParsleyEngine#receive}.
     */
    private ParsleyMessage<KIn, VIn> ingest(Record<KIn, VIn> record) {
        RecordMetadata meta = requireRecordMetadata();
        String topic = meta.topic();
        TopicPartition source = new TopicPartition(topic, meta.partition());
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no ParsleyBuffer registered for topic '" + topic
                            + "'; call addBuffer(...) on the ParsleyProcessors builder for every input topic");
        }
        try {
            return ParsleyMessage.from(record, source, meta.offset(), topicId);
        } catch (ParsleyClockResolutionException e) {
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
    private ParsleyClockResolutionException onUnresolvableClock(ParsleyClockResolutionException e) {
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
        try (ParsleyTopicAdmin admin = adminFactory.apply(context.appConfigs())) {
            resolved = admin.topicIds(topicList);
            partitionCounts = new HashMap<>(admin.partitionCounts(topicList));
            partitionCounts.putAll(additionalTopicInfo(admin, "partition count", ParsleyTopicAdmin::partitionCounts));
            cleanupPolicies = additionalTopicInfo(admin, "cleanup.policy", ParsleyTopicAdmin::cleanupPolicies);
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
        validatePartitionParity(partitionCounts);
        validateCleanupPolicy(cleanupPolicies);
        return resolved;
    }

    /**
     * Best-effort, per-topic UUID resolution over {@link #sinkTopics} — unlike {@link
     * #additionalTopicInfo}, this always runs, never gated by {@code parsley.topology.validation}: it
     * feeds {@link #engine}'s own-coordinate stripping (see {@link ParsleyEngine}'s Javadoc on {@code
     * ownSinkTopics}), a correctness mechanism, not a topology-misconfiguration lint.
     *
     * <p>A sink that does not exist yet is skipped, and — because this resolution runs once, at
     * {@code init()}, and is never re-attempted — own-coordinate stripping for that topic stays off
     * for this task instance's whole lifetime, until the next restart re-runs {@code init()}. In a
     * topology cycle that means a dependency reflecting this node's own not-yet-resolved sink back at
     * it fails the task fast as "unreachable" (fail-closed, recoverable: the restart re-resolves the
     * now-existing sink) rather than being wrongly stripped. Nothing can depend on the sink before its
     * first record exists, so the exposure starts only at first produce and ends at the next init.
     */
    private Set<Uuid> resolveSinkTopicUuids(ParsleyTopicAdmin admin) {
        Set<Uuid> ids = new HashSet<>();
        for (String topic : sinkTopics) {
            try {
                Uuid id = admin.topicIds(List.of(topic)).get(topic);
                if (id != null) {
                    ids.add(id);
                }
            } catch (Exception e) {
                log.warn("Could not resolve topic id for sink topic '{}' (it may not exist yet); "
                        + "own-coordinate stripping for it stays off until the next restart re-resolves it",
                        topic, e);
            }
        }
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
     * <p>When topology-epoch coordination is configured ({@link #coordination} non-null), a mismatch is
     * escalated to a hard failure even under the default {@code warn} — {@link ParsleyMarkerPartitioner}
     * routes an epoch marker to this task's own owned partition ({@code taskId().partition()})
     * unconditionally, so a sink with fewer partitions than a source makes the produce fail outright,
     * crash-looping the task instead of surfacing what is actually a startup misconfiguration. {@code
     * off} is still honoured as an explicit, deliberate opt-out of every check this method performs.
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
        if (coordination != null) {
            throw new IllegalStateException("parsley.topology.validation=warn, but topology-epoch "
                    + "coordination is configured: " + detail + "; a mismatch under coordination fails "
                    + "epoch-marker routing at produce time instead of at startup, so it is treated as "
                    + "strict regardless of the configured mode");
        }
        log.warn("parsley.topology.validation=warn: {}", detail);
    }

    /**
     * Warns or fails (per {@code parsley.topology.validation}) when this member's own declared
     * subscriptions do not cover the coordinated domain's full topic set — some other member on the
     * shared epoch-events log consumes or produces a topic this member neither consumes nor produces.
     * Escalated to a hard failure even under the default {@code warn} — mirroring {@link
     * #validatePartitionParity}'s coordination precedent — since this member could later be asked to
     * gate a record on a coordinate it can never see (fail-closed in {@link ParsleyEngine}, not silently
     * satisfied), and an incomplete mesh also blocks every epoch round from ever completing (see {@link
     * ParsleyEpochLog#isFullMeshSatisfied()}). Surfacing this loudly at startup is better than a
     * data-path crash loop discovering it record by record, or every round silently hanging forever.
     * {@code off} is still honoured as an explicit, deliberate opt-out.
     *
     * <p>Called immediately after {@link ParsleyCoordination#awaitJoinCommit}, so this member's own join
     * has already folded and the runtime has already bootstrapped — {@code runtime.domainTopics()}
     * reflects every member declared on the log as of at least this member's own join.
     */
    /**
     * The topology-epoch join wait's time budget: {@link #JOIN_BUDGET_FRACTION} of the effective
     * {@code max.poll.interval.ms} (Kafka's default when unset), so the wait fails loudly before the
     * broker would evict this consumer. Leaning on the existing consumer config rather than a new knob —
     * the poll deadline is exactly the constraint the bound exists to respect.
     */
    private static Duration joinBudget(Map<String, Object> appConfigs) {
        long maxPollMs = DEFAULT_MAX_POLL_INTERVAL_MS;
        Object configured = appConfigs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        if (configured instanceof Number number) {
            maxPollMs = number.longValue();
        } else if (configured instanceof String text && !text.isBlank()) {
            try {
                maxPollMs = Long.parseLong(text.trim());
            } catch (NumberFormatException ignored) {
                // Malformed override — fall back to the default rather than fail init on a config typo.
            }
        }
        return Duration.ofMillis((long) (maxPollMs * JOIN_BUDGET_FRACTION));
    }

    private void validateFullMeshCoverage(ParsleyEpochRuntime runtime) {
        ParsleyConfig.ValidationMode mode = config.topologyValidation();
        if (mode == ParsleyConfig.ValidationMode.OFF) {
            return;
        }
        Set<String> domain = runtime.domainTopics();
        Set<String> missing = new HashSet<>(domain);
        missing.removeAll(topics);
        missing.removeAll(sinkTopics);
        if (missing.isEmpty()) {
            return;
        }
        String detail = "this member's own subscriptions (inputs " + topics + ", sinks " + sinkTopics
                + ") do not cover the coordinated domain " + domain + "; missing: " + missing
                + " — every running member must be able to see every domain coordinate for the "
                + "completeness it publishes to be sound";
        if (mode == ParsleyConfig.ValidationMode.STRICT) {
            throw new IllegalStateException("parsley.topology.validation=strict: " + detail);
        }
        throw new IllegalStateException("parsley.topology.validation=warn, but topology-epoch "
                + "coordination is configured: " + detail + "; an incomplete mesh under coordination "
                + "blocks every epoch round from ever completing, so it is treated as strict regardless "
                + "of the configured mode");
    }

    /**
     * Warns or fails (per {@code parsley.topology.validation}) when a {@link CausalStreams} sink
     * topic's {@code cleanup.policy} includes {@code compact}. A protocol watermark is a null-key,
     * null-value record wire-indistinguishable from a compaction tombstone, so under compaction it
     * can be removed from the log before a slow consumer reads it — silently losing the completeness
     * frontier it carried. {@code compact,delete} is equally unsafe: compaction still runs.
     */
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
