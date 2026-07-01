package io.github.tobyjamesclements.parsley;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Wraps a user {@link Processor} and gates delegation on the causal frontier: an incoming record is
 * held until the frontier dominates its causal dependencies, or until the configured
 * {@link CausalBufferLimit} forces delivery anyway. Every record reaches
 * {@code delegate.process(...)} exactly once. State reads/writes the delegate performs and every
 * record it forwards are causally ordered unless the record was force-delivered by an eviction
 * (logged and counted by the violation metric); forwards are stamped with the current frontier by a
 * {@link ParsleyProcessorContext}.
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

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final CausalBufferLimit limit;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final Set<String> topics;
    private final Set<String> additionalPartitionCountTopics;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final CausalAudit audit;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Source topic name -> stable UUID, resolved from the broker at init() (the topology decorator
    // has no broker config until then). Used by ingest() to stamp each record's causal identity.
    private Map<String, Uuid> topicUuids = Map.of();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> candidateIndexStore;
    private KeyValueStore<byte[], byte[]> forwardedIndexStore;
    private ParsleyEngine<KIn, VIn> engine;
    private ParsleyMetrics.Wired wiredMetrics;
    // The stamping proxy context handed to the delegate. Held here so deliver() can check the
    // per-record forward count and emit a watermark when the delegate forwarded nothing.
    private ParsleyProcessorContext<KOut, VOut> stampingContext;
    // Read live by the stamping proxy; volatile as belt-and-suspenders (single task thread owns this).
    private volatile ParsleyClock stampFrontier = ParsleyClock.empty();
    private volatile @Nullable RecordMetadata deliveryMetadata;
    private Cancellable restoredOverflowSchedule;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     CausalBufferLimit limit,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String candidateIndexStoreName,
                     String forwardedIndexStoreName,
                     Set<String> topics,
                     Set<String> additionalPartitionCountTopics,
                     Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                     ParsleyConfig config,
                     CausalAudit audit) {
        this.delegate = delegate;
        this.limit = limit;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.topics = topics;
        this.additionalPartitionCountTopics = additionalPartitionCountTopics;
        this.adminFactory = adminFactory;
        this.config = config;
        this.audit = audit;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.topicUuids = resolveTopicUuids(context);
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.candidateIndexStore = context.getStateStore(candidateIndexStoreName);
        this.forwardedIndexStore = context.getStateStore(forwardedIndexStoreName);

        boolean restored = frontierStore.get(ParsleyStores.FRONTIER_KEY) != null;

        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new RocksCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new RocksForwardedIndex(forwardedIndexStore);
        // The single owner of the persisted causal metadata: loads the frontier clock and channel
        // clocks from key "f" of the frontier store and rewrites that value on change. The forwarded
        // index keeps its own keyed store (growable, order-sensitive) and is injected here.
        ParsleyFrontier frontier = new ParsleyFrontier(frontierStore, forwardedIndex);

        if (restored) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), frontier.snapshot());
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }
        audit.processorInitialized(context.taskId().toString(), restored);

        this.wiredMetrics = ParsleyMetrics.wire(context,
                ParsleyLimits.sizeLimitOf(limit), ParsleyLimits.durationLimitOf(limit));

        // The coordinates this task consumes: a registered input topic, on the partition this task
        // owns. Streams co-partitions a sub-topology's sources, so the task owns partition
        // taskId().partition() of every input topic. Derived here, never persisted, so it is
        // recomputed identically after a rebalance. Used for restore-time pruning and eviction-gap
        // diagnostics — not as a delivery filter (the gate waits for every channel; see completeness()).
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

        this.engine = new ParsleyEngine<>(limit, frontier, inScope, buffer, candidateIndex,
                wiredMetrics.metrics(), audit, context::currentSystemTimeMs,
                config.skipOnDecodeFailure(), config.failOnEvictionLimit());
        // Initialise stampFrontier from completeness() so the stamping proxy reflects the restored
        // channel-clock state (not just the in-scope frontier) from the first forward onward.
        this.stampFrontier = engine.completeness();

        this.stampingContext = new ParsleyProcessorContext<>(
                context, () -> stampFrontier, () -> Optional.ofNullable(deliveryMetadata));
        delegate.init(stampingContext);

        // Enforce the size limit once against a buffer restored from a changelog (e.g. after a
        // restart following a reconfiguration that lowered the limit); onRecord()'s inline check
        // only fires on the next admission, which may never come. Must run as a punctuation, not
        // inline here: Streams hasn't finished wiring the task's RecordCollector until every
        // processor in the topology returns from init(), so forward() during init() throws NPE.
        restoredOverflowSchedule = context.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    restoredOverflowSchedule.cancel();
                    deliver(evictRestoredOverflow());
                });

        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timestamp -> deliver(evict())));

        // Refreshes the oldest-record gauge independent of buffer traffic, so it stays current on a
        // buffer that sits idle between admits/releases/evictions (notably a size-only buffer, which
        // has no other periodic tick at all).
        context.schedule(METRICS_REFRESH_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                timestamp -> engine.reportBufferState());
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        if (isWatermark(record)) {
            handleWatermark(record);
            return;
        }
        ParsleyClock completenessBefore = engine.completeness();
        List<ParsleyMessage<KIn, VIn>> admitted = gate(ingest(record));
        deliver(admitted);
        // Advertise this node's progress so downstream channel clocks advance gap-free. A delivered
        // record advertises through its business output's completeness stamp — or, if the delegate
        // forwarded nothing, the watermark emitted in deliver(). A consumed record that was buffered
        // produces neither, so emit a heartbeat watermark — but only when its receipt-time
        // channel-clock update actually advanced completeness, so an unrelated held record does not
        // flood downstream with no-op watermarks.
        if (admitted.isEmpty() && !engine.completeness().equals(completenessBefore)) {
            // Key the heartbeat with the buffered record's own key so it routes to that record's
            // partition, matching where its eventual business output will land.
            forwardWatermark(record.key());
        }
    }

    @Override
    public void close() {
        log.info("Processor closing [task: {}]", context.taskId());
        audit.processorClosing(context.taskId().toString());
        delegate.close();
        wiredMetrics.close(context.metrics());
    }

    private List<ParsleyMessage<KIn, VIn>> gate(ParsleyMessage<KIn, VIn> record) {
        return engine.onRecord(record);
    }

    private List<ParsleyMessage<KIn, VIn>> evict() {
        return engine.evictExpired();
    }

    private List<ParsleyMessage<KIn, VIn>> evictRestoredOverflow() {
        List<ParsleyMessage<KIn, VIn>> out = new ArrayList<>(engine.drainRestoredSatisfied());
        out.addAll(engine.evictOverflow());
        return out;
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
        List<ParsleyMessage<KIn, VIn>> released = engine.onWatermark(topicId, partition, frontierClock);
        deliver(released);

        // Always re-emit a watermark downstream so the completeness boundary propagates through
        // non-subscribing layers even when no business records were released. Reuse the incoming
        // watermark's own key: upstream keyed it to route to this partition, so re-emitting under the
        // same key keeps the propagated watermark on the co-partitioned downstream partition.
        forwardWatermark(record.key());
    }

    /**
     * Forwards a protocol watermark carrying this node's current {@link ParsleyEngine#completeness()
     * completeness frontier} to all downstream children, keyed with {@code triggerKey} — the key of
     * the input record that triggered this emission. Reusing that key routes the watermark, through
     * whatever partitioner the business records use, to the same partition those records land on, so a
     * downstream task's channel clock for that partition advances even across a sink boundary. With a
     * null key (the previous behaviour) a sink partitioner sends the watermark to an arbitrary
     * partition, so a downstream task can starve waiting on a channel that never advances.
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
     * events), so the input and output key types coincide. A null-keyed input yields a null-keyed
     * watermark, which degrades to arbitrary routing — consistent with the fact that null-keyed
     * records are not shard-partitioned to begin with.
     */
    @SuppressWarnings({"NullAway", "unchecked"}) // null value by design; KIn==KOut under the co-partitioning contract
    private void forwardWatermark(@Nullable KIn triggerKey) {
        Headers wm = ParsleyHeader.mutableHeaders();
        wm.add(ParsleyHeader.WATERMARK, new byte[0]);
        wm.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine.completeness().toBytes());
        // Use 0L as the watermark timestamp: a watermark's timestamp carries no business meaning —
        // only its headers matter for causal gating — so 0L is safe in all execution contexts
        // (MockProcessorContext may not have time initialised).
        context.forward(new Record<>((KOut) (Object) triggerKey, null, 0L, wm));
    }

    private ParsleyMessage<KIn, VIn> ingest(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        TopicPartition source = new TopicPartition(topic, meta.map(RecordMetadata::partition).orElse(0));
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no CausalBuffer registered for topic '" + topic
                            + "'; call addBuffer(...) on the CausalProcessors builder for every input topic");
        }
        long offset = meta.map(RecordMetadata::offset).orElse(0L);
        try {
            return ParsleyMessage.from(record, source, offset, topicId);
        } catch (ParsleyClockResolutionException e) {
            return onUnresolvableClock(e, record, source, offset, topicId);
        }
    }

    /**
     * Applies {@code parsley.clock.resolution.failure.policy} when an inbound record's causal
     * dependencies header could not be decoded: {@code fail} (default) fails the task fast, leaving
     * the record to be reprocessed on restart; {@code continue} forwards it with empty (vacuously
     * satisfied) dependencies, counted as a violation. The occurrence is metered and audited either
     * way.
     */
    private ParsleyMessage<KIn, VIn> onUnresolvableClock(ParsleyClockResolutionException e,
            Record<KIn, VIn> record, TopicPartition source, long offset, Uuid topicId) {
        boolean fail = config.failOnUnresolvableClock();
        wiredMetrics.metrics().recordClockResolutionError();
        audit.recordClockResolutionFailure(e.topic(), e.partition(), e.offset(), e.details(), fail);
        if (fail) {
            log.error("Unresolvable causal-dependencies header on {}-{} @{} "
                    + "(parsley.clock.resolution.failure.policy = fail); failing fast. The record was "
                    + "not forwarded and is reprocessed on restart. {}",
                    e.topic(), e.partition(), e.offset(), e.details(), e);
            throw e;
        }
        log.warn("Unresolvable causal-dependencies header on {}-{} @{} "
                + "(parsley.clock.resolution.failure.policy = continue); forwarding with empty "
                + "dependencies. {}", e.topic(), e.partition(), e.offset(), e.details(), e);
        wiredMetrics.metrics().recordViolation();
        return ParsleyMessage.from(record, source, offset, topicId, ParsleyClock.empty());
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
            partitionCounts.putAll(additionalPartitionCounts(admin));
            cleanupPolicies = additionalCleanupPolicies(admin);
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
     * Best-effort partition counts for {@link #additionalPartitionCountTopics} — extra topics (e.g.
     * a {@link CausalStreams} sink) folded into the co-partitioning check without being consumed.
     * Unlike a registered input buffer, such a topic is not required to exist yet (a sink is often
     * auto-created on first write), so a failure here is logged and skipped rather than failing the
     * task.
     */
    private Map<String, Integer> additionalPartitionCounts(ParsleyTopicAdmin admin) {
        if (additionalPartitionCountTopics.isEmpty()) {
            return Map.of();
        }
        try {
            return admin.partitionCounts(new ArrayList<>(additionalPartitionCountTopics));
        } catch (Exception e) {
            log.warn("Could not resolve partition counts for {} (they may not exist yet); "
                    + "skipping the co-partitioning check for them", additionalPartitionCountTopics, e);
            return Map.of();
        }
    }

    /**
     * Best-effort {@code cleanup.policy} lookup for {@link #additionalPartitionCountTopics} — same
     * not-required-to-exist-yet reasoning as {@link #additionalPartitionCounts}.
     */
    private Map<String, String> additionalCleanupPolicies(ParsleyTopicAdmin admin) {
        if (additionalPartitionCountTopics.isEmpty()) {
            return Map.of();
        }
        try {
            return admin.cleanupPolicies(new ArrayList<>(additionalPartitionCountTopics));
        } catch (Exception e) {
            log.warn("Could not resolve cleanup.policy for {} (they may not exist yet); "
                    + "skipping the watermark-safety check for them", additionalPartitionCountTopics, e);
            return Map.of();
        }
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
        String detail = "causal input topics have mismatched partition counts " + partitionCounts
                + "; co-partitioning requires an equal partition count across all causally-related "
                + "input topics so each task owns the complete partition set for a related group";
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
