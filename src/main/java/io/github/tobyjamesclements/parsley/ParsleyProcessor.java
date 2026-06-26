package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
    private final Set<String> topics;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final CausalAudit audit;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Source topic name -> stable UUID, resolved from the broker at init() (the topology decorator
    // has no broker config until then). Used by ingest() to stamp each record's causal identity.
    private Map<String, Uuid> topicUuids = Map.of();

    // Frontier snapshots captured per-record-admission; zips 1:1 with engine.onRecord() returns.
    private final List<ParsleyClock> snapshots = new ArrayList<>();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> candidateIndexStore;
    private KeyValueStore<byte[], byte[]> forwardedIndexStore;
    private ParsleyEngine<KIn, VIn> engine;
    private ParsleyMetrics.Wired wiredMetrics;
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

        ParsleyClock initialFrontier = ParsleyClock.empty();
        byte[] stored = frontierStore.get(ParsleyStores.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = ParsleyClock.fromBytes(stored);
        }
        this.stampFrontier = initialFrontier;
        if (stored != null) {
            log.info("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), initialFrontier);
        } else {
            log.info("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }
        audit.processorInitialized(context.taskId().toString(), stored != null);

        ParsleyEngine.FrontierCallback listener = frontier -> {
            frontierStore.put(ParsleyStores.FRONTIER_KEY, frontier.toBytes());
            snapshots.add(frontier);
        };

        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyCandidateIndex candidateIndex = new RocksCandidateIndex(candidateIndexStore);
        ParsleyForwardedIndex forwardedIndex = new RocksForwardedIndex(forwardedIndexStore);

        this.wiredMetrics = ParsleyMetrics.wire(context,
                ParsleyEngine.sizeLimitOf(limit), ParsleyEngine.durationLimitOf(limit));

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

        this.engine = new ParsleyEngine<>(limit, initialFrontier, inScope,
                listener, buffer, candidateIndex, forwardedIndex, wiredMetrics.metrics(), audit,
                context::currentSystemTimeMs, config.skipOnDecodeFailure(), config.failOnEvictionLimit());

        ProcessorContext<KOut, VOut> stamping = new ParsleyProcessorContext<>(
                context, () -> stampFrontier, () -> Optional.ofNullable(deliveryMetadata));
        delegate.init(stamping);

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
        deliver(gate(ingest(record)));
    }

    @Override
    public void close() {
        log.info("Processor closing [task: {}]", context.taskId());
        audit.processorClosing(context.taskId().toString());
        delegate.close();
        wiredMetrics.close(context.metrics());
    }

    private List<ParsleyMessage<KIn, VIn>> gate(ParsleyMessage<KIn, VIn> record) {
        snapshots.clear();
        return engine.onRecord(record);
    }

    private List<ParsleyMessage<KIn, VIn>> evict() {
        snapshots.clear();
        return engine.evictExpired();
    }

    private List<ParsleyMessage<KIn, VIn>> evictRestoredOverflow() {
        snapshots.clear();
        List<ParsleyMessage<KIn, VIn>> out = new ArrayList<>(engine.drainRestoredSatisfied());
        out.addAll(engine.evictOverflow());
        return out;
    }

    private void deliver(List<ParsleyMessage<KIn, VIn>> admitted) {
        for (int i = 0; i < admitted.size(); i++) {
            ParsleyMessage<KIn, VIn> message = admitted.get(i);
            stampFrontier = snapshots.get(i);
            deliveryMetadata = new ParsleyRecordMetadata(message.topic(), message.partition(), message.offset());
            // User headers + the producer's dependencies only; the source coordinate is surfaced via
            // context.recordMetadata(), and ParsleyProcessorContext re-stamps the frontier on forward.
            delegate.process(new Record<>(message.key(), message.value(), message.timestamp(),
                    message.headersWithDependencies()));
        }
        deliveryMetadata = null;
        stampFrontier = engine.frontier();
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
