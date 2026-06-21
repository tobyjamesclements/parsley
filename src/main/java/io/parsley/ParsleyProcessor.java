package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final CausalBufferLimit limit;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String positionIndexStoreName;
    private final Map<String, Uuid> topicUuids;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Frontier snapshots captured per-record-admission; zips 1:1 with engine.onRecord() returns.
    private final List<CausalFrontier> snapshots = new ArrayList<>();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> positionIndexStore;
    private ParsleyEngine<KIn, VIn> engine;
    private ParsleyMetrics.Wired wiredMetrics;
    // Read live by the stamping proxy; volatile as belt-and-suspenders (single task thread owns this).
    private volatile CausalFrontier stampFrontier = CausalFrontier.empty();
    private volatile RecordMetadata deliveryMetadata;
    private Cancellable restoredOverflowSchedule;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     CausalBufferLimit limit,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String positionIndexStoreName,
                     Map<String, Uuid> topicUuids) {
        this.delegate = delegate;
        this.limit = limit;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.positionIndexStoreName = positionIndexStoreName;
        this.topicUuids = topicUuids;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.positionIndexStore = context.getStateStore(positionIndexStoreName);

        CausalFrontier initialFrontier = CausalFrontier.empty();
        byte[] stored = frontierStore.get(ParsleyAttributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = CausalFrontier.fromBytes(stored);
        }
        this.stampFrontier = initialFrontier;
        if (stored != null) {
            log.debug("Processor initialized [task: {}] — frontier restored: {}", context.taskId(), initialFrontier);
        } else {
            log.debug("Processor initialized [task: {}] — frontier empty (fresh start)", context.taskId());
        }

        ParsleyEngine.FrontierCallback listener = frontier -> {
            frontierStore.put(ParsleyAttributes.FRONTIER_KEY, frontier.toBytes());
            snapshots.add(frontier);
        };

        ParsleyBufferStore<KIn, VIn> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyPositionIndex positionIndex = new RocksPositionIndex(positionIndexStore);

        this.wiredMetrics = ParsleyMetrics.wire(context);

        this.engine = new ParsleyEngine<>(limit, initialFrontier,
                listener, buffer, positionIndex, wiredMetrics.metrics(), context::currentSystemTimeMs);

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
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        deliver(gate(ingest(record)));
    }

    @Override
    public void close() {
        log.debug("Processor closing [task: {}]", context.taskId());
        delegate.close();
        wiredMetrics.close(context.metrics());
    }

    private List<ParsleyRecord<KIn, VIn>> gate(ParsleyRecord<KIn, VIn> record) {
        snapshots.clear();
        return engine.onRecord(record);
    }

    private List<ParsleyRecord<KIn, VIn>> evict() {
        snapshots.clear();
        return engine.evictExpired();
    }

    private List<ParsleyRecord<KIn, VIn>> evictRestoredOverflow() {
        snapshots.clear();
        return engine.evictOverflow();
    }

    private void deliver(List<ParsleyRecord<KIn, VIn>> admitted) {
        for (int i = 0; i < admitted.size(); i++) {
            ParsleyRecord<KIn, VIn> record = admitted.get(i);
            stampFrontier = snapshots.get(i);
            deliveryMetadata = new ParsleyRecordMetadata(
                    record.sourcePartition().topic(), record.sourcePartition().partition(), record.sourceOffset());
            delegate.process(new Record<>(record.key(), record.value(), record.timestamp(), record.toHeaders()));
        }
        deliveryMetadata = null;
        stampFrontier = engine.frontier();
    }

    private ParsleyRecord<KIn, VIn> ingest(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        TopicPartition source = new TopicPartition(topic, meta.map(RecordMetadata::partition).orElse(0));
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no CausalTopic registered for topic '" + topic
                            + "'; call addCausalTopic(...) on the CausalProcessors builder for every input topic");
        }
        return ParsleyRecord.of(record, source, meta.map(RecordMetadata::offset).orElse(0L), topicId);
    }
}
