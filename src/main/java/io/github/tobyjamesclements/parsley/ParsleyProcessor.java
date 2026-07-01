package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
    private final String channelFrontierStoreName;
    private final Set<String> topics;
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
    private KeyValueStore<byte[], byte[]> channelFrontierStore;
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
                     String channelFrontierStoreName,
                     Set<String> topics,
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
        this.channelFrontierStoreName = channelFrontierStoreName;
        this.topics = topics;
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
        this.channelFrontierStore = context.getStateStore(channelFrontierStoreName);

        ParsleyClock initialFrontier = ParsleyClock.empty();
        byte[] stored = frontierStore.get(ParsleyStores.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = ParsleyClock.fromBytes(stored);
        }
        if (stored != null) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), initialFrontier);
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }
        audit.processorInitialized(context.taskId().toString(), stored != null);

        ParsleyEngine.FrontierCallback listener = frontier ->
                frontierStore.put(ParsleyStores.FRONTIER_KEY, frontier.toBytes());

        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new RocksCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new RocksForwardedIndex(forwardedIndexStore);
        ParsleyChannelClockStore channelClockStore = new RocksChannelClockStore(channelFrontierStore);

        this.wiredMetrics = ParsleyMetrics.wire(context,
                ParsleyLimits.sizeLimitOf(limit), ParsleyLimits.durationLimitOf(limit));

        // A dependency is gated only if this task actually consumes its coordinate: a registered
        // input topic, on the partition this task owns. Streams co-partitions a sub-topology's
        // sources, so the task owns partition taskId().partition() of every input topic — which is
        // the partition of every record it consumes. Any other coordinate (a different partition, or
        // a topic outside the registered buffers) carries no obligation here and is vacuously
        // satisfied. Derived here, never persisted, so it is recomputed identically after a rebalance.
        Set<Uuid> consumedTopicIds = Set.copyOf(topicUuids.values());
        int taskPartition = context.taskId().partition();
        ParsleyClock.CoordinatePredicate inScope = (topicId, partition) ->
                partition == taskPartition && consumedTopicIds.contains(topicId);

        // Prune coordinates that no longer belong to this processor's scope — topic UUIDs change
        // when a topic is dropped and recreated, leaving stale entries in the persisted clock.
        // Pruning here keeps the stored frontier compact and ensures the initial stampFrontier
        // does not carry coordinates that drainRestoredSatisfied would otherwise need to ignore.
        initialFrontier = initialFrontier.retaining(inScope);
        this.stampFrontier = initialFrontier;

        // Prune channel-clock entries whose topic UUID is no longer in scope (e.g. after topic
        // deletion and recreation with a new UUID). A stale entry would hold the completeness min
        // at zero for a coordinate that can never advance, pinning the completeness frontier forever.
        for (ParsleyChannelClockStore.ChannelEntry entry : channelClockStore.allEntries()) {
            if (!inScope.test(entry.topicId(), entry.partition())) {
                channelClockStore.remove(entry.topicId(), entry.partition());
            }
        }

        // Seed an entry for every consumed input channel so the completeness frontier — the
        // per-coordinate min across ALL input channels — includes a channel that has not yet observed
        // anything (it contributes nothing, holding the min until it advertises). Without this, a
        // silent channel would simply be absent from the min, and a record could be delivered before
        // that channel confirmed the dependency.
        for (Uuid topicId : consumedTopicIds) {
            channelClockStore.update(topicId, taskPartition, ParsleyClock.empty());
        }

        this.engine = new ParsleyEngine<>(limit, initialFrontier, inScope, channelClockStore,
                listener, buffer, candidateIndex, forwardedIndex, wiredMetrics.metrics(), audit,
                context::currentSystemTimeMs, config.skipOnDecodeFailure(), config.failOnEvictionLimit());
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
            forwardWatermark();
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
                forwardWatermark();
            }
        }
        deliveryMetadata = null;
        stampFrontier = engine.completeness();
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley protocol watermark, identified by the
     * presence of the {@link ParsleyHeader#WATERMARK} header. Watermarks carry null key/null value
     * and must never be forwarded to the user delegate or buffered — they exist only to propagate
     * causal completeness progress through non-emitting layers.
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
        // non-subscribing layers even when no business records were released.
        forwardWatermark();
    }

    /**
     * Forwards a protocol watermark carrying this node's current {@link ParsleyEngine#completeness()
     * completeness frontier} to all downstream children. The watermark carries null key and null
     * value (distinguishable from business tombstones by the {@link ParsleyHeader#WATERMARK} header)
     * and bypasses the business-forward counter in {@link ParsleyProcessorContext} so it does not
     * prevent watermark emission for a genuinely non-emitting delegate invocation.
     *
     * <p>Null key/value are intentional: watermark records have no business payload; they are
     * protocol metadata only. Any non-Parsley consumer of a topic containing watermarks will see
     * tombstone-shaped records — Parsley's own consumers skip them.
     */
    @SuppressWarnings("NullAway") // watermark records carry null key/value by protocol design
    private void forwardWatermark() {
        Headers wm = ParsleyHeader.mutableHeaders();
        wm.add(ParsleyHeader.WATERMARK, new byte[0]);
        wm.add(ParsleyHeader.CAUSAL_DEPENDENCIES, engine.completeness().toBytes());
        // Use 0L as the watermark timestamp: a watermark's timestamp carries no business meaning —
        // only its headers matter for causal gating — so 0L is safe in all execution contexts
        // (MockProcessorContext may not have time initialised).
        context.forward(new Record<>(null, null, 0L, wm));
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
        Map<String, Uuid> resolved;
        try (ParsleyTopicAdmin admin = adminFactory.apply(context.appConfigs())) {
            resolved = admin.topicIds(new ArrayList<>(topics));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to resolve topic UUIDs for causal buffers " + topics
                            + "; ensure the topics exist and the broker is reachable", e);
        }
        for (String topic : topics) {
            if (resolved.get(topic) == null) {
                throw new IllegalStateException("broker did not return a UUID for topic '" + topic + "'");
            }
        }
        return resolved;
    }
}
