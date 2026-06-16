package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Wraps a user {@link Processor} and gates delegation on the causal frontier: an incoming record is
 * delivered to {@code delegate.process(...)} only once the frontier dominates its dependency clock
 * (or the policy forces it). State reads/writes the delegate performs and every record it forwards
 * are therefore causally ordered, and forwards are clock-stamped by a
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

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final CausalBufferPolicy policy;
    private final CausalViolationHandler onViolation;
    private final Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink;
    private final ParsleySerializer<KIn, VIn> serializer;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final CausalFrontierListener frontierListener;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task:
    // process() and any wall-clock punctuator run on that same thread, interleaved but never
    // concurrently. So the un-synchronized fields here are safe without further guarding.

    // The engine advances the frontier once per admitted record and fires the CausalFrontierListener each
    // time, in the same order as the records it returns; we record those frontier values here so each
    // delivered record can be stamped with the frontier as of *its own* admission (not the batch end).
    private final List<CausalDependencies> snapshots = new ArrayList<>();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private ParsleyEngine<KIn, VIn> engine;
    private List<Sensor> sensorsToClose = List.of();
    // Read live by the stamping proxy: the clock to stamp on forward, and the source coordinate to
    // report from recordMetadata(), for the record currently being delivered. Written and read on the
    // one task thread (see above); volatile is belt-and-suspenders against any future caller that
    // might read them off-thread, not a fix for a real race.
    private volatile CausalDependencies stampClock = CausalDependencies.empty();
    private volatile RecordMetadata deliveryMetadata;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     CausalBufferPolicy policy,
                     CausalViolationHandler onViolation,
                     Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     CausalFrontierListener frontierListener) {
        this.delegate = delegate;
        this.policy = policy;
        this.onViolation = onViolation;
        this.deadLetterSink = deadLetterSink;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.frontierListener = frontierListener;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);

        // Restore the frontier persisted by a previous run (cold start → empty).
        CausalDependencies initialFrontier = CausalDependencies.empty();
        byte[] stored = frontierStore.get(ParsleyAttributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = CausalDependencies.fromBytes(stored);
        }
        this.stampClock = initialFrontier;

        // Publish the restored frontier so an observer (e.g. a CausalConsumer) has a correct view
        // before the first new record is admitted. This goes straight to the public listener, never
        // through the engine's CausalFrontierListener below, whose per-advance snapshot bookkeeping must
        // stay in lock-step with the records the engine returns.
        frontierListener.onFrontierAdvanced(initialFrontier);

        // The user's dead-letter sink takes a ConsumerRecord; the engine deals in ParsleyRecord.
        Consumer<ParsleyRecord<KIn, VIn>> engineDeadLetter = deadLetterSink == null
                ? null
                : record -> deadLetterSink.accept(record.toConsumerRecord());

        // Fires inside the engine *before* each advanced record is returned, so persisting here gives
        // the persist-frontier-before-forward guarantee for free; we also capture the per-record
        // frontier snapshot used for precise stamping in deliver().
        ParsleyEngine.FrontierCallback listener = frontier -> {
            frontierStore.put(ParsleyAttributes.FRONTIER_KEY, frontier.toBytes());
            snapshots.add(frontier);
            // Publish only after persisting, preserving the persist-frontier-before-forward invariant.
            frontierListener.onFrontierAdvanced(frontier);
        };

        // The buffer store IS the buffer: held records that survived the previous run are already in
        // it, so there is nothing to "restore" — ParsleyBufferStore seeds its sequence past them and the
        // engine drains them on the next frontier advance.
        CausalBufferStore<KIn, VIn> buffer = new ParsleyBufferStore<>(bufferStore, serializer);

        ParsleyMetrics metrics = buildMetrics(context);

        this.engine = new ParsleyEngine<>(policy, onViolation, initialFrontier,
                engineDeadLetter, listener, buffer, metrics);

        // The delegate runs against the stamping proxy, never the raw context, so its forwards are
        // clock-stamped and its recordMetadata() reflects the delivered record.
        ProcessorContext<KOut, VOut> stamping = new ParsleyProcessorContext<>(
                context, () -> stampClock, () -> Optional.ofNullable(deliveryMetadata));
        delegate.init(stamping);

        // Only a duration limit needs a timer; it drains records that have outstayed the limit through
        // the same admit path as process().
        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timestamp -> deliver(evict())));
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        deliver(gate(ingest(record)));
    }

    @Override
    public void close() {
        delegate.close();
        for (Sensor sensor : sensorsToClose) {
            context.metrics().removeSensor(sensor);
        }
    }

    private ParsleyMetrics buildMetrics(ProcessorContext<?, ?> ctx) {
        StreamsMetrics sm = ctx.metrics();
        String taskId = ctx.taskId().toString();

        Sensor buffered  = sm.addRateTotalSensor("parsley", taskId, "records-buffered",  Sensor.RecordingLevel.INFO);
        Sensor released  = sm.addRateTotalSensor("parsley", taskId, "records-released",  Sensor.RecordingLevel.INFO);
        Sensor evicted   = sm.addRateTotalSensor("parsley", taskId, "records-evicted",   Sensor.RecordingLevel.INFO);
        Sensor violation = sm.addRateTotalSensor("parsley", taskId, "violations",         Sensor.RecordingLevel.INFO);

        // A "last recorded value" sensor for the current buffer depth — polled as a gauge.
        Sensor depth = sm.addSensor("parsley-buffer-depth-" + taskId, Sensor.RecordingLevel.INFO);
        depth.add(new MetricName("buffer-depth", "stream-parsley-metrics",
                "Current number of records held in the causal buffer",
                Map.of("parsley-id", taskId)), new Value());

        sensorsToClose = List.of(buffered, released, evicted, violation, depth);

        return new ParsleyMetrics() {
            @Override public void recordBuffered(int d)       { buffered.record();   depth.record(d); }
            @Override public void recordReleased(int c, int d){ released.record(c);  depth.record(d); }
            @Override public void recordEvicted(int c)        { evicted.record(c);   depth.record(0); }
            @Override public void recordViolation()           { violation.record(); }
        };
    }

    /** Runs an incoming record through the engine, resetting the per-call snapshot buffer first. */
    private List<ParsleyRecord<KIn, VIn>> gate(ParsleyRecord<KIn, VIn> record) {
        snapshots.clear();
        return engine.onRecord(record);
    }

    /** Evicts the buffer (limit fired), resetting the per-call snapshot buffer first. */
    private List<ParsleyRecord<KIn, VIn>> evict() {
        snapshots.clear();
        return engine.evictNow();
    }

    /**
     * Delivers each admitted record to the delegate, stamping its forwards with the frontier as of
     * that record's admission. {@code admitted} and {@link #snapshots} are produced in lock-step by
     * the engine (one frontier advance per admitted record), so they zip 1:1.
     */
    private void deliver(List<ParsleyRecord<KIn, VIn>> admitted) {
        for (int i = 0; i < admitted.size(); i++) {
            ParsleyRecord<KIn, VIn> record = admitted.get(i);
            // snapshots.get(i) is the frontier as of this record's admission — the clock its forwards
            // should carry. A drained record's metadata differs from the trigger record Streams is
            // currently processing, so we surface the drained record's own source coordinate.
            stampClock = snapshots.get(i);
            deliveryMetadata = new ParsleyRecordMetadata(
                    record.sourcePartition().topic(), record.sourcePartition().partition(), record.sourceOffset());
            delegate.process(new Record<>(record.key(), record.value(), record.timestamp(), record.toHeaders()));
        }
        // Between deliveries (e.g. a user punctuator firing) stamp the live frontier and let
        // recordMetadata() fall back to the real context.
        deliveryMetadata = null;
        stampClock = engine.frontier();
    }

    private ParsleyRecord<KIn, VIn> ingest(Record<KIn, VIn> record) {
        // Capture the source coordinate now, at ingest, while recordMetadata() is still about this
        // record — it is carried on the envelope so it survives buffering (see deliver()).
        Optional<RecordMetadata> meta = context.recordMetadata();
        TopicPartition source = new TopicPartition(
                meta.map(RecordMetadata::topic).orElse(""),
                meta.map(RecordMetadata::partition).orElse(0));
        return ParsleyRecord.of(record, source, meta.map(RecordMetadata::offset).orElse(0L));
    }
}
