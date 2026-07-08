package io.github.tobyjamesclements.parsley;

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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
 * Wraps a user {@link Processor} and gates delegation on the causal frontier: an incoming record is
 * held until the completeness frontier dominates its causal dependencies. Delivery is strictly
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
 * @param <KIn>  the input key type
 * @param <VIn>  the input value type
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class ParsleyProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyProcessor.class);

    private static final Duration METRICS_REFRESH_INTERVAL = Duration.ofSeconds(5);

    private static final Duration EPOCH_POLL_INTERVAL = Duration.ofMillis(200);

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final String orphanIndexStoreName;
    private final Set<String> topics;
    // The topics this stage produces. Feeds the partition-count parity check and, when coordination is
    // configured, this member's declaration on the epoch-events log for the DAG-wide source-topic registry.
    private final Set<String> sinkTopics;
    // Every child node this processor forwards to (business sinks and, if configured, the dead-letter
    // sink): a stage's processor node addresses every forward by name (never zero-arg broadcast), since
    // a dead-letter sink registered with Serdes.ByteArray() would otherwise receive every business/control
    // forward too and throw ClassCastException on the runtime cast. See ParsleyProcessorContext.forward.
    private final List<String> sinkNodeNames;
    // This stage's dead-letter sink node name, or null if none is configured — in which case a
    // poison/unresolvable-clock record fails the task fast, exactly as before dead-lettering existed.
    private final @Nullable String deadLetterSinkName;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final CausalAudit audit;
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

    // The most-recent business key seen on this task's owned partition — reused to route a self-injected
    // marker back to that partition lane (null until the first record). And the last snapshot-round /
    // committed epoch this task has already acted on, so each is injected once. (Whether this task is
    // source-layer is derived per poll from the log's source-topic registry, not cached here.)
    private @Nullable KIn lastSeenKey;
    private long lastSnapshotRoundEpoch;
    private long lastAdoptedEpoch;
    // The DAG-wide external-source topic IDs this task injected the boundary onto at its last adopted
    // epoch. Deliberately kept alongside (not replaced by) the live registry each poll: a topic that
    // stops being external mid-round (a new member just declared it as a sink) still needs this one
    // transition epoch's boundary self-adopted by its outgoing self-adopter, since the newly-declared
    // producer cannot possibly have relayed an in-band marker for the very epoch that admitted it. See
    // pollEpochCoordination().
    private Set<Uuid> lastAdoptedExternalSourceTopicIds = Set.of();
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
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> candidateIndexStore;
    private KeyValueStore<byte[], byte[]> forwardedIndexStore;
    private KeyValueStore<byte[], byte[]> orphanIndexStore;
    private ParsleyEngine<KIn, VIn> engine;
    private ParsleyMetrics.Wired wiredMetrics;
    // The stamping proxy context handed to the delegate. Held here so deliver() can check the
    // per-record forward count and emit a watermark when the delegate forwarded nothing.
    private ParsleyProcessorContext<KOut, VOut> stampingContext;
    // Read live by the stamping proxy; volatile as belt-and-suspenders (single task thread owns this).
    private volatile ParsleyClock stampFrontier = ParsleyClock.empty();
    private volatile @Nullable RecordMetadata deliveryMetadata;
    private Cancellable restoredDrainSchedule;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     String orphanIndexStoreName,
                     Set<String> topics,
                     Set<String> sinkTopics,
                     List<String> sinkNodeNames,
                     @Nullable String deadLetterSinkName,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     ParsleyConfig config,
                     CausalAudit audit,
                     @Nullable ParsleyQuiesce quiesce) {
        this(delegate, serializer, frontierStoreName, bufferStoreName, candidateIndexStoreName,
                forwardedIndexStoreName, orphanIndexStoreName, topics, sinkTopics, sinkNodeNames,
                deadLetterSinkName, adminFactory, config, audit, quiesce, ParsleyEpochSnapshotPublisher.NOOP);
    }

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     String orphanIndexStoreName,
                     Set<String> topics,
                     Set<String> sinkTopics,
                     List<String> sinkNodeNames,
                     @Nullable String deadLetterSinkName,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     ParsleyConfig config,
                     CausalAudit audit,
                     @Nullable ParsleyQuiesce quiesce,
                     ParsleyEpochSnapshotPublisher snapshotPublisher) {
        this(delegate, serializer, frontierStoreName, bufferStoreName, candidateIndexStoreName,
                forwardedIndexStoreName, orphanIndexStoreName, topics, sinkTopics, sinkNodeNames,
                deadLetterSinkName, adminFactory, config, audit, quiesce, snapshotPublisher, null);
    }

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     String orphanIndexStoreName,
                     Set<String> topics,
                     Set<String> sinkTopics,
                     List<String> sinkNodeNames,
                     @Nullable String deadLetterSinkName,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     ParsleyConfig config,
                     CausalAudit audit,
                     @Nullable ParsleyQuiesce quiesce,
                     ParsleyEpochSnapshotPublisher snapshotPublisher,
                     @Nullable ParsleyCoordination coordination) {
        this.delegate = delegate;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.orphanIndexStoreName = orphanIndexStoreName;
        this.topics = topics;
        this.sinkTopics = sinkTopics;
        this.sinkNodeNames = sinkNodeNames;
        this.deadLetterSinkName = deadLetterSinkName;
        this.adminFactory = adminFactory;
        this.config = config;
        this.audit = audit;
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
        this.orphanIndexStore = context.getStateStore(orphanIndexStoreName);

        boolean restored = frontierStore.get(ParsleyStores.FRONTIER_KEY) != null;

        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new RocksCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new RocksForwardedIndex(forwardedIndexStore);
        ParsleyOrphanIndex orphanIndex = new RocksOrphanIndex(orphanIndexStore);
        // The single owner of the persisted causal metadata: loads the frontier clock and channel
        // clocks from key "f" of the frontier store and rewrites that value on change. The forwarded
        // and orphan indexes keep their own keyed stores and are injected here.
        // Resolve epoch coordination from the handle before building the epoch state: build/share the
        // per-instance runtime (from this task's appConfigs), install the runtime-backed snapshot
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
            coordination.awaitJoinCommit(runtime, memberId);
        }

        // The task's epoch state floors gating against the settled epoch. A fresh task that joined an
        // established epoch settles DIRECTLY at the committed floor F_{k+1}: it has no in-flight prior-
        // epoch records, so every below-floor replay record is pre-epoch history to strip — no overlap
        // window. Otherwise it starts fresh at epoch 0; a restored task's state (settled floor plus any
        // in-progress transition) is loaded from the frontier "f" blob by the ParsleyFrontier constructor.
        //
        // lastAdoptedEpoch is deliberately left at its default (0) even here: settling epochState
        // directly is purely local (this task has no in-flight prior-epoch records to gate), but
        // pollEpochCoordination()'s first poll must still get a genuine chance to relay the admitting
        // epoch's boundary downstream on this task's own external-source inputs (if any) — a downstream
        // task reachable only through this one's output, never touching that source topic directly,
        // has no other way to learn the floor advanced. Re-injecting it locally on this task's own
        // already-settled epochState is a no-op (ParsleyEpochState#onBoundary short-circuits at
        // epochId <= settledEpochId), so retrying costs nothing.
        ParsleyEpochState epochState;
        if (epochRuntime != null && !restored && epochRuntime.committedEpochId() > 0) {
            epochState = new ParsleyEpochState(epochRuntime.committedLowerBounds(), epochRuntime.committedEpochId());
        } else {
            epochState = new ParsleyEpochState();
        }
        ParsleyFrontier frontier = new ParsleyFrontier(frontierStore, forwardedIndex, orphanIndex, epochState);

        if (restored) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), frontier.snapshot());
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }
        audit.processorInitialized(context.taskId().toString(), restored);

        this.wiredMetrics = ParsleyMetrics.wire(context);

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
        // channel confirmed the dependency.
        frontier.pruneToScope(inScope);
        for (Uuid topicId : consumedTopicIds) {
            frontier.channelUpdate(topicId, taskPartition, ParsleyClock.empty());
        }

        this.engine = new ParsleyEngine<>(frontier, buffer, candidateIndex,
                wiredMetrics.metrics(), audit, context::currentSystemTimeMs, deadLetterSinkName != null);
        // Initialise stampFrontier from completeness() so the stamping proxy reflects the restored
        // channel-clock state (not just the in-scope frontier) from the first forward onward.
        this.stampFrontier = engine.completeness();
        // Registers the now-meaningful stampFrontier snapshot so the shared runtime can publish this
        // member's completeness on its behalf from its own background thread if this task's thread is ever
        // wedged (e.g. sharing a StreamThread with a joiner blocked in awaitJoinCommit) and so can never run
        // pollEpochCoordination() itself. See ParsleyEpochRuntime#registerLocalCompleteness.
        if (epochRuntime != null) {
            epochRuntime.registerLocalCompleteness(memberId, () -> stampFrontier);
        }

        this.stampingContext = new ParsleyProcessorContext<>(
                context, () -> stampFrontier, () -> Optional.ofNullable(deliveryMetadata), sinkNodeNames);
        delegate.init(stampingContext);

        // Drain any records that became satisfiable between the last committed frontier and the last
        // committed buffer-removal (the at-least-once window) once, against the buffer restored from a
        // changelog. Must run as a punctuation, not inline here: Streams hasn't finished wiring the
        // task's RecordCollector until every processor in the topology returns from init(), so
        // forward() during init() throws NPE.
        restoredDrainSchedule = context.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    restoredDrainSchedule.cancel();
                    ParsleyEngine.Outcome<KIn, VIn> outcome = engine.drainAfterRestore();
                    deliver(outcome.delivered());
                    deadLetter(outcome.deadLettered());
                });

        // Refreshes the oldest-record gauge independent of buffer traffic, so it stays current on a
        // buffer that sits idle between admits/releases/evictions (notably a size-only buffer, which
        // has no other periodic tick at all). Also re-pushes this task's quiesce-drained state: without
        // this, a task whose buffer emptied before requestQuiesce() was ever called would report
        // drained=false forever — updateQuiesceState() only runs on a depth-changing event, and once the
        // buffer is idle there is no such event left to re-evaluate isQuiesceRequested() && empty after
        // the request actually arrives — hanging CausalStreams#close()'s wait indefinitely despite the
        // buffer genuinely being empty. This tick closes that gap within one refresh interval.
        context.schedule(METRICS_REFRESH_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    engine.reportBufferState();
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
            quiesce.register(context.taskId());
        }
        // Report the restored buffer's drained state once up front, so a task that starts idle with an
        // empty buffer is known-drained to quiesce and to leave() without waiting for the first delivery.
        updateQuiesceState();
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
        if (isWatermark(record)) {
            handleWatermark(record);
            return;
        }
        if (isEpochBoundary(record)) {
            handleEpochBoundary(record);
            return;
        }
        if (isEpochSnapshot(record)) {
            handleEpochSnapshot(record);
            return;
        }
        // Remember the most-recent business key seen on this task's owned partition; a source-layer task
        // reuses it to route a self-injected marker back onto that partition lane.
        lastSeenKey = record.key();
        Optional<ParsleyMessage<KIn, VIn>> ingested = ingest(record);
        if (ingested.isEmpty()) {
            // Already dead-lettered inside onUnresolvableClock (a dead-letter sink is configured) —
            // nothing further to gate for this record.
            pollEpochCoordination();
            return;
        }
        ParsleyClock completenessBefore = engine.completeness();
        ParsleyEngine.Outcome<KIn, VIn> outcome = engine.onRecord(ingested.get());
        deliver(outcome.delivered());
        deadLetter(outcome.deadLettered());
        // Advertise this node's progress so downstream channel clocks advance gap-free. A delivered
        // record advertises through its business output's completeness stamp — or, if the delegate
        // forwarded nothing, the watermark emitted in deliver(). A consumed record that was buffered
        // produces neither, so emit a heartbeat watermark — but only when its receipt-time
        // channel-clock update actually advanced completeness, so an unrelated held record does not
        // flood downstream with no-op watermarks.
        if (outcome.delivered().isEmpty() && !engine.completeness().equals(completenessBefore)) {
            // Key the heartbeat with the buffered record's own key so it routes to that record's
            // partition, matching where its eventual business output will land.
            forwardWatermark(record.key());
        }
        // A source-layer task also checks the coordination log after each record, so a round that opened
        // (or an epoch that committed) is acted on promptly without waiting for the wall-clock tick.
        pollEpochCoordination();
    }

    @Override
    public void close() {
        log.info("Processor closing [task: {}]", context.taskId());
        audit.processorClosing(context.taskId().toString());
        if (quiesce != null) {
            quiesce.unregister(context.taskId());
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
        delegate.close();
        wiredMetrics.close(context.metrics());
    }

    private void deliver(List<ParsleyMessage<KIn, VIn>> admitted) {
        for (ParsleyMessage<KIn, VIn> message : admitted) {
            // The stamp is the node's completeness frontier — the per-channel-min-then-frontier-max
            // boundary that is sound across all input branches. This replaces the old per-record
            // frontier-snapshot-merged-with-inbound-deps approach, which was correct only in
            // single-layer topologies. Downstream nodes receive the sound multi-layer boundary.
            stampFrontier = engine.completeness();
            deliveryMetadata = new ParsleyRecordMetadata(message.topic(), message.partition(), message.offset());
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
                forwardWatermark(message.key());
            }
        }
        deliveryMetadata = null;
        stampFrontier = engine.completeness();
        updateQuiesceState();
    }

    /**
     * Forwards every dead-lettered record to the dead-letter sink. Paired with {@link #deliver}:
     * dead-lettering, like delivery, changes buffer depth, so {@link #updateQuiesceState} runs
     * afterward here too. Auditing happens inside {@link ParsleyEngine} itself (the same place
     * {@code recordForwarded}/{@code recordReleased} are audited from), so this method only meters and
     * forwards.
     */
    private void deadLetter(List<ParsleyEngine.DeadLetter<KIn, VIn>> deadLettered) {
        for (ParsleyEngine.DeadLetter<KIn, VIn> letter : deadLettered) {
            wiredMetrics.metrics().recordDeadLetter();
            forwardDeadLetter(letter, List.of());
        }
        if (!deadLettered.isEmpty()) {
            updateQuiesceState();
        }
    }

    /**
     * Forwards {@code letter} to the dead-letter sink as raw bytes, bypassing the stamping proxy (like
     * the watermark/marker forwards) since a dead-lettered record is leaving the causal path, not
     * advancing it. A {@link ParsleyEngine.DeadLetter.Decoded} record's key/value are re-serialised with
     * this stage's own serdes (they already decoded fine); a {@link ParsleyEngine.DeadLetter.Undecodable}
     * record's raw bytes — recovered before the failed decode attempt — are carried through as-is, since
     * its {@code V} could never be reconstructed. {@code extraHeaders} carries forensics specific to one
     * dead-letter reason (e.g. the original undecodable dependencies header for an unresolvable clock).
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // raw bytes forwarded through KOut/VOut at runtime; see class javadoc
    private void forwardDeadLetter(ParsleyEngine.DeadLetter<KIn, VIn> letter, List<ParsleyHeader> extraHeaders) {
        if (deadLetterSinkName == null) {
            // Dead-lettering is off (no sink configured) — the engine and onUnresolvableClock never
            // produce a DeadLetter in that case, so this is unreachable in practice; guards misuse.
            throw new IllegalStateException("dead-lettered a record with no dead-letter sink configured");
        }
        byte @Nullable [] keyBytes;
        byte @Nullable [] valueBytes;
        long timestamp;
        Headers headers = ParsleyHeader.mutableHeaders();
        switch (letter) {
            case ParsleyEngine.DeadLetter.Decoded<KIn, VIn> decoded -> {
                ParsleyMessage<KIn, VIn> message = decoded.record();
                keyBytes = serializer.keyBytes(message.topic(), message.key());
                valueBytes = serializer.valueBytes(message.topic(), message.value());
                timestamp = message.timestamp();
                for (ParsleyHeader h : message.headers()) {
                    headers.add(h.key(), h.value());
                }
            }
            case ParsleyEngine.DeadLetter.Undecodable<KIn, VIn> undecodable -> {
                keyBytes = undecodable.rawKey();
                valueBytes = undecodable.rawValue();
                timestamp = undecodable.timestamp();
                for (ParsleyHeader h : undecodable.headers()) {
                    headers.add(h.key(), h.value());
                }
            }
        }
        headers.add(ParsleyHeader.DEADLETTER_REASON, letter.reason().name().getBytes(StandardCharsets.UTF_8));
        headers.add(ParsleyHeader.DEADLETTER_SOURCE_TOPIC, letter.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(ParsleyHeader.DEADLETTER_SOURCE_TOPIC_ID, ParsleyHeader.uuidToBytes(letter.topicId()));
        headers.add(ParsleyHeader.DEADLETTER_SOURCE_PARTITION, ByteBuffer.allocate(4).putInt(letter.partition()).array());
        headers.add(ParsleyHeader.DEADLETTER_SOURCE_OFFSET, ByteBuffer.allocate(8).putLong(letter.offset()).array());
        for (ParsleyHeader h : extraHeaders) {
            headers.add(h.key(), h.value());
        }
        context.forward(new Record<>((KOut) (Object) keyBytes, (VOut) (Object) valueBytes, timestamp, headers),
                deadLetterSinkName);
    }

    /**
     * Reports this task's current buffer-drained state, to {@link #quiesce} (if registered) and to the
     * epoch runtime (if coordinated). Called after every buffer-depth-changing event (every path that can
     * hold, release, or evict a record funnels through {@link #deliver}), so the signal reflects the
     * current buffer depth without polling. Never fabricates completeness — it only observes the buffer
     * depth the ordinary delivery path already produced. Quiesce additionally gates its drained flag on
     * {@link ParsleyQuiesce#isQuiesceRequested()}; the runtime tracks the raw depth so
     * {@link ParsleyCoordination#leave()} can wait for a drained buffer before removing the member.
     */
    private void updateQuiesceState() {
        boolean empty = engine.bufferSize() == 0;
        if (quiesce != null) {
            quiesce.setDrained(context.taskId(), quiesce.isQuiesceRequested() && empty);
        }
        if (epochRuntime != null) {
            epochRuntime.reportDrained(memberId, empty);
        }
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley protocol watermark, identified solely by the
     * presence of the {@link ParsleyHeader#WATERMARK} header — never by its key, which carries the
     * triggering record's key for routing. Watermarks carry a null value and must never be forwarded
     * to the user delegate or buffered — they exist only to propagate causal completeness progress
     * through non-emitting layers.
     */
    private boolean isWatermark(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.WATERMARK.equals(h.key())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley topology epoch-boundary marker, identified by
     * the {@link ParsleyHeader#EPOCH_BOUNDARY} header. Like a watermark it carries no business payload
     * and must never be forwarded to the user delegate or buffered — it exists only to drive each
     * node's local overlapping-epoch transition.
     */
    private boolean isEpochBoundary(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley topology epoch-snapshot marker, identified by
     * the {@link ParsleyHeader#EPOCH_SNAPSHOT} header. Like a watermark it carries no business payload
     * and must never be forwarded to the user delegate or buffered — it triggers this node to publish
     * its completeness frontier for the Mattern cut.
     */
    private boolean isEpochSnapshot(Record<KIn, VIn> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.EPOCH_SNAPSHOT.equals(h.key())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Handles a received epoch-snapshot marker: publishes this node's current completeness frontier to
     * the coordinator (tagged with the task id) via {@link ParsleyEpochSnapshotPublisher}, so the
     * coordinator can merge-min the published clocks into the next epoch's lower bounds. The marker is
     * never delivered to the user delegate and never buffered, but — unlike the earlier
     * coordinator-broadcast model — it is now <strong>relayed</strong> downstream on the same key (the
     * leaderless in-band cut propagates edge by edge through the DAG). The relayed marker also carries
     * this node's completeness, so a single record both propagates the cut and advances the downstream
     * channel clock; if the received marker itself carried completeness (a relay from upstream), that
     * completeness advances this node's clock for the marker channel too.
     */
    private void handleEpochSnapshot(Record<KIn, VIn> record) {
        snapshotPublisher.publish(memberId, engine.completeness());
        advanceChannelClockFromMarker(record);
        forwardEpochSnapshot(record.key());
    }

    /**
     * Handles a received epoch-boundary marker: decodes it, records it on its source channel and (if the
     * transition is now ready) closes the epoch window in the engine, delivers any records the raised
     * floor releases, and <strong>relays the marker downstream</strong> on the same key so the boundary
     * propagates edge by edge through the DAG (the leaderless in-band model). The relayed marker carries
     * this node's completeness, so a single downstream record both adopts the boundary and advances the
     * channel clock; if the received marker carried completeness (a relay from upstream), that
     * completeness advances this node's clock for the marker channel first. The marker is never forwarded
     * to the user delegate and never buffered.
     */
    private void handleEpochBoundary(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        int partition = meta.map(RecordMetadata::partition).orElse(0);
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            log.warn("Received epoch boundary on unregistered topic '{}'; ignoring", topic);
            return;
        }

        ParsleyEpochBoundary boundary = null;
        byte[] boundaryBytes = null;
        for (Header h : record.headers()) {
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key()) && h.value() != null) {
                try {
                    boundaryBytes = h.value();
                    boundary = ParsleyEpochBoundary.fromBytes(boundaryBytes);
                } catch (Exception e) {
                    log.warn("Failed to decode epoch boundary on {}-{}; ignoring", topic, partition, e);
                }
                break;
            }
        }
        if (boundary == null || boundaryBytes == null) {
            return;
        }

        // A relayed marker carries the upstream node's completeness; adopt it into this channel's clock
        // (draining anything it releases) before driving the transition, so one record does both.
        advanceChannelClockFromMarker(record);

        ParsleyEngine.Outcome<KIn, VIn> outcome = engine.onEpochBoundary(boundary, topicId, partition);
        deliver(outcome.delivered());
        deadLetter(outcome.deadLettered());

        // Relay the boundary downstream on the marker's key so it stays on the same partition lane and
        // every downstream task transitions its owned partitions. The relay carries this node's own
        // completeness so the next layer's channel clock advances from the same record.
        forwardEpochBoundary(record.key(), boundaryBytes);
    }

    /**
     * If {@code record} (an epoch marker) carries a completeness frontier in its
     * {@link ParsleyHeader#CAUSAL_DEPENDENCIES} header — as every relayed marker does — advances the
     * marker channel's clock by that frontier and delivers anything the advance releases. A bare marker
     * with no completeness header (e.g. a source-layer injection or a test-injected marker) is a no-op.
     */
    private void advanceChannelClockFromMarker(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        int partition = meta.map(RecordMetadata::partition).orElse(0);
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            return;
        }
        for (Header h : record.headers()) {
            if (ParsleyHeader.CAUSAL_DEPENDENCIES.equals(h.key()) && h.value() != null) {
                try {
                    ParsleyClock frontier = ParsleyClock.fromBytes(h.value());
                    ParsleyEngine.Outcome<KIn, VIn> outcome = engine.onWatermark(topicId, partition, frontier);
                    deliver(outcome.delivered());
                    deadLetter(outcome.deadLettered());
                } catch (Exception e) {
                    log.warn("Failed to decode marker completeness on {}-{}; ignoring", topic, partition, e);
                }
                return;
            }
        }
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
     * through this one's relay. So boundary adoption targets {@code live ∪ lastAdopted} (the registry as
     * of this task's previous adoption), giving a departing topic exactly one more adoption cycle from its
     * outgoing self-adopter before {@link #lastAdoptedExternalSourceTopicIds} catches up to the live set.
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
        runtime.reportDrained(memberId, engine.bufferSize() == 0);
        // Every member — not just source-layer — publishes its completeness for an open round it still owes
        // one for, driven off the folded log. This makes publication restart-safe: a member that restarts
        // mid-round re-derives from the log that it owes a publication and re-publishes, without depending
        // on having consumed a one-shot in-band snapshot marker exactly once (the case timeout eviction used
        // to paper over). The per-round guard bounds it to one append per round per task lifetime; a restart
        // resets the guard, so a lost publish is re-sent. Safe to publish current completeness pre-cut:
        // completeness is monotonic and the committed floor is a conservative merge-min.
        if (runtime.owesPublication(memberId)) {
            long round = runtime.committedEpochId() + 1;
            if (round != lastPublishedRoundEpoch) {
                snapshotPublisher.publish(memberId, engine.completeness());
                lastPublishedRoundEpoch = round;
            }
        }
        // Whether this task is source-layer is derived per poll from the log's DAG-wide source-topic
        // registry: the external source topics (inputs no member produces) that this task actually consumes.
        // Derived, not configured, and re-read each poll because the registry changes as members join —
        // including, mid-round, dropping a topic a new member just declared as a sink (see above).
        Set<Uuid> liveExternalSourceTopicIds = resolveExternalSourceTopicIds(runtime);
        long committed = runtime.committedEpochId();
        if (committed > lastAdoptedEpoch) {
            Set<Uuid> adoptionTargets = new HashSet<>(liveExternalSourceTopicIds);
            adoptionTargets.addAll(lastAdoptedExternalSourceTopicIds);
            if (!adoptionTargets.isEmpty()) {
                adoptAndInjectBoundary(new ParsleyEpochBoundary(committed, runtime.committedLowerBounds()), adoptionTargets);
            }
            lastAdoptedEpoch = committed;
            lastAdoptedExternalSourceTopicIds = liveExternalSourceTopicIds;
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
     * The external source coordinates this task owns: the topology's log-derived external source topics
     * (inputs no member produces) intersected with this task's consumed topics, resolved to their broker
     * UUIDs. Non-empty iff this task is source-layer for the current registry.
     */
    private Set<Uuid> resolveExternalSourceTopicIds(ParsleyEpochRuntime runtime) {
        return runtime.externalSourceTopics().stream()
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
        snapshotPublisher.publish(memberId, engine.completeness());
        forwardEpochSnapshot(lastSeenKey);
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
        List<ParsleyEngine.DeadLetter<KIn, VIn>> deadLettered = new ArrayList<>();
        for (Uuid topicId : externalSourceTopicIds) {
            ParsleyEngine.Outcome<KIn, VIn> outcome = engine.onEpochBoundary(boundary, topicId, partition);
            released.addAll(outcome.delivered());
            deadLettered.addAll(outcome.deadLettered());
        }
        deliver(released);
        deadLetter(deadLettered);
        forwardEpochBoundary(lastSeenKey, boundary.toBytes());
    }

    /**
     * Handles a received protocol watermark: decodes its carried completeness frontier, updates the
     * per-channel clock for the watermark's source channel, performs a full-buffer drain to release
     * any records newly satisfying the two-part gate, and re-emits a watermark downstream carrying
     * this node's updated completeness frontier. The watermark is never forwarded to the user
     * delegate and never buffered.
     *
     * <p>The downstream re-emission (inductive propagation) ensures that a node which never
     * produces business records on this path still advertises its causal progress, so a grandchild
     * node's channel clock can advance without any business record on the path.
     */
    private void handleWatermark(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        int partition = meta.map(RecordMetadata::partition).orElse(0);
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            // Watermark on an unregistered topic — not expected in a correctly wired topology, but
            // fail safe rather than crash so a misconfiguration does not fail a healthy task.
            log.warn("Received watermark on unregistered topic '{}'; ignoring", topic);
            return;
        }

        // Decode the completeness frontier carried in the parsley-causal-dependencies header.
        ParsleyClock frontierClock = ParsleyClock.empty();
        for (Header h : record.headers()) {
            if (ParsleyHeader.CAUSAL_DEPENDENCIES.equals(h.key()) && h.value() != null) {
                try {
                    frontierClock = ParsleyClock.fromBytes(h.value());
                } catch (Exception e) {
                    log.warn("Failed to decode watermark frontier on {}-{}; treating as empty",
                            topic, partition, e);
                }
                break;
            }
        }

        // Update the channel clock and drain any newly releasable records.
        ParsleyEngine.Outcome<KIn, VIn> outcome = engine.onWatermark(topicId, partition, frontierClock);
        deliver(outcome.delivered());
        deadLetter(outcome.deadLettered());

        // Always re-emit a watermark downstream so the completeness boundary propagates through
        // non-subscribing layers even when no business records were released. Reuse the incoming
        // watermark's own key: upstream keyed it to route to this partition, so re-emitting under the
        // same key keeps the propagated watermark on the co-partitioned downstream partition.
        forwardWatermark(record.key());
    }

    /**
     * Forwards a protocol watermark carrying this node's current {@link ParsleyEngine#completeness()
     * completeness frontier} to every business sink ({@link #sinkNodeNames} — never a zero-arg
     * broadcast, so a configured dead-letter sink never receives one; see {@link ParsleyProcessorContext}),
     * keyed with {@code triggerKey} — the key of the input record that triggered this emission, carried
     * through as informational wire content, not for routing: {@link ParsleyMarkerPartition} (set by
     * {@link #forwardToSinks}) routes the watermark to this task's own owned partition regardless of
     * {@code triggerKey}, including when it is {@code null} — so a downstream task's channel clock for
     * that partition always advances across a sink boundary, never dependent on a business key having
     * been observed yet.
     *
     * <p>The watermark carries a null value and is distinguished from a business tombstone by the
     * {@link ParsleyHeader#WATERMARK} header; downstream Parsley consumers skip it by that header, not
     * by its key. It bypasses the business-forward counter in {@link ParsleyProcessorContext} (it is
     * forwarded through the raw context, not the stamping proxy) so it does not prevent watermark
     * emission for a genuinely non-emitting delegate invocation, and its frontier header is written
     * here directly rather than by the proxy.
     *
     * <p>The {@code KIn}-to-{@code KOut} cast is sound under the co-partitioning contract: a causal
     * processor must not change the key across the node (doing so reshards the causally-related
     * events), so the input and output key types coincide.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardWatermark(@Nullable KIn triggerKey) {
        Headers wm = ParsleyHeader.mutableHeaders();
        wm.add(ParsleyHeader.WATERMARK, new byte[0]);
        wm.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine.completeness().toBytes());
        // Use 0L as the watermark timestamp: a watermark's timestamp carries no business meaning —
        // only its headers matter for causal gating — so 0L is safe in all execution contexts
        // (MockProcessorContext may not have time initialised).
        forwardToSinks(new Record<>((KOut) (Object) triggerKey, null, 0L, wm));
    }

    /**
     * Relays an epoch-snapshot marker to every business sink, keyed with {@code triggerKey} — carried
     * through as informational wire content only, not for routing (see {@link #forwardWatermark} for the
     * routing rationale, the {@code KIn}-to-{@code KOut} cast, the targeted-forwarding rationale, and the
     * null-value / null-key handling — identical here). The marker also carries this node's current
     * completeness in the {@link ParsleyHeader#CAUSAL_DEPENDENCIES} header, so one record both propagates
     * the cut and advances the downstream channel clock.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardEpochSnapshot(@Nullable KIn triggerKey) {
        Headers marker = ParsleyHeader.mutableHeaders();
        marker.add(ParsleyHeader.EPOCH_SNAPSHOT, new byte[0]);
        marker.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine.completeness().toBytes());
        forwardToSinks(new Record<>((KOut) (Object) triggerKey, null, 0L, marker));
    }

    /**
     * Relays an epoch-boundary marker (carrying the unchanged {@code boundaryBytes}: the same epochId +
     * lowerBounds everywhere) to every business sink, keyed with {@code triggerKey} — informational only,
     * not for routing. Also carries this node's completeness so the downstream channel clock advances
     * from the same record. See {@link #forwardWatermark} for the routing/cast/targeted-forward rationale.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardEpochBoundary(@Nullable KIn triggerKey, byte[] boundaryBytes) {
        Headers marker = ParsleyHeader.mutableHeaders();
        marker.add(ParsleyHeader.EPOCH_BOUNDARY, boundaryBytes);
        marker.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine.completeness().toBytes());
        forwardToSinks(new Record<>((KOut) (Object) triggerKey, null, 0L, marker));
    }

    /**
     * Forwards a control-plane record (watermark or epoch marker) to every business sink by name, or
     * broadcasts (Kafka Streams' own zero-arg {@code forward}) when {@link #sinkNodeNames} is empty —
     * the common case, with no dead-letter sink configured. Named forwarding is required the moment a
     * dead-letter sink is a sibling child of this processor's business sink(s): see
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
     * Decodes {@code record} into a {@link ParsleyMessage}, ready for {@link ParsleyEngine#onRecord}.
     * Empty means the record was already dead-lettered (its causal-dependencies header was undecodable
     * and a dead-letter sink is configured, see {@link #onUnresolvableClock}) — there is nothing further
     * to gate.
     */
    private Optional<ParsleyMessage<KIn, VIn>> ingest(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        TopicPartition source = new TopicPartition(topic, meta.map(RecordMetadata::partition).orElse(0));
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no ParsleyBuffer registered for topic '" + topic
                            + "'; call addBuffer(...) on the ParsleyProcessors builder for every input topic");
        }
        long offset = meta.map(RecordMetadata::offset).orElse(0L);
        try {
            return Optional.of(ParsleyMessage.from(record, source, offset, topicId));
        } catch (ParsleyClockResolutionException e) {
            return onUnresolvableClock(e, record, source, offset, topicId);
        }
    }

    /**
     * Handles an inbound record whose causal-dependencies header could not be decoded. Its real
     * dependencies are unknown, so forwarding it on the ordinary path would deliver on an unknown
     * premise — never permitted. With a dead-letter sink configured, the record — its key/value already
     * decoded fine by Kafka Streams, only this header failed — is dead-lettered (carrying the original
     * undecodable header bytes for forensics), its own coordinate is durably orphaned at this offset via
     * {@link ParsleyEngine#deadLetterAtIngest} (cascading to any buffered dependent, exactly as the
     * engine's other dead-letter paths do), and this returns empty. Without a dead-letter sink, the task
     * fails fast, exactly as before dead-lettering existed: the record was never buffered and its source
     * offset is not committed past it, so it is reprocessed on restart.
     */
    private Optional<ParsleyMessage<KIn, VIn>> onUnresolvableClock(ParsleyClockResolutionException e,
            Record<KIn, VIn> record, TopicPartition source, long offset, Uuid topicId) {
        wiredMetrics.metrics().recordClockResolutionError();
        audit.recordClockResolutionFailure(e.topic(), e.partition(), e.offset(), e.details());
        if (deadLetterSinkName == null) {
            log.error("Unresolvable causal-dependencies header on {}-{} @{}; failing fast (fail-closed). "
                    + "The record was not forwarded and is reprocessed on restart. {}",
                    e.topic(), e.partition(), e.offset(), e.details(), e);
            throw e;
        }
        log.warn("Unresolvable causal-dependencies header on {}-{} @{}; dead-lettering. {}",
                e.topic(), e.partition(), e.offset(), e.details(), e);
        ParsleyMessage<KIn, VIn> message = ParsleyMessage.from(record, source, offset, topicId, ParsleyClock.empty());
        ParsleyEngine.DeadLetter<KIn, VIn> letter =
                new ParsleyEngine.DeadLetter.Decoded<>(message, ParsleyEngine.DeadLetter.Reason.UNRESOLVABLE_CLOCK);
        wiredMetrics.metrics().recordDeadLetter();
        audit.recordDeadLetter(e.topic(), e.partition(), e.offset(), ParsleyEngine.DeadLetter.Reason.UNRESOLVABLE_CLOCK.name());
        forwardDeadLetter(letter, List.of(new ParsleyHeader(ParsleyHeader.DEADLETTER_ORIGINAL_DEPENDENCIES,
                e.encodedDependencies())));
        // Never went through onRecord (its dependencies header was undecodable), so it has no
        // candidate-index/frontier footprint of its own — the engine's ordinary dead-letter paths never
        // ran for it. Without this, its coordinate's contiguous frontier freezes at offset - 1 forever
        // with nothing durably recording why, and any buffered record depending on this exact offset (or
        // later) is held indefinitely. Route it through the engine so the coordinate is orphaned at this
        // offset and any already-buffered dependent is cascaded, exactly as the buffered-poison path is.
        ParsleyEngine.Outcome<KIn, VIn> outcome = engine.deadLetterAtIngest(topicId, source.partition(), offset);
        deliver(outcome.delivered());
        deadLetter(outcome.deadLettered());
        return Optional.empty();
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
