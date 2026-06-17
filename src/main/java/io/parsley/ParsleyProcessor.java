package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
    private final String waitIndexStoreName;
    private final CausalFrontierListener frontierListener;
    private final Map<String, Uuid> topicUuids;

    // All mutable state below is confined to the single Kafka Streams thread that owns this task.

    // Frontier snapshots captured per-record-admission; zips 1:1 with engine.onRecord() returns.
    private final List<CausalFrontier> snapshots = new ArrayList<>();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<Long, byte[]> bufferStore;
    private KeyValueStore<byte[], byte[]> waitIndexStore;
    private ParsleyEngine<KIn, VIn> engine;
    private List<Sensor> sensorsToClose = List.of();
    // Read live by the stamping proxy; volatile as belt-and-suspenders (single task thread owns this).
    private volatile CausalFrontier stampClock = CausalFrontier.empty();
    private volatile RecordMetadata deliveryMetadata;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                     CausalBufferPolicy policy,
                     CausalViolationHandler onViolation,
                     Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                     ParsleySerializer<KIn, VIn> serializer,
                     String frontierStoreName,
                     String bufferStoreName,
                     String waitIndexStoreName,
                     CausalFrontierListener frontierListener,
                     Map<String, Uuid> topicUuids) {
        this.delegate = delegate;
        this.policy = policy;
        this.onViolation = onViolation;
        this.deadLetterSink = deadLetterSink;
        this.serializer = serializer;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.waitIndexStoreName = waitIndexStoreName;
        this.frontierListener = frontierListener;
        this.topicUuids = topicUuids;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.waitIndexStore = context.getStateStore(waitIndexStoreName);

        CausalFrontier initialFrontier = CausalFrontier.empty();
        byte[] stored = frontierStore.get(ParsleyAttributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = CausalFrontier.fromBytes(stored);
        }
        this.stampClock = initialFrontier;

        // Publish the restored frontier so an observer has a correct view before the first record.
        frontierListener.onFrontierAdvanced(initialFrontier);

        Consumer<ParsleyRecord<KIn, VIn>> engineDeadLetter = deadLetterSink == null
                ? null
                : record -> deadLetterSink.accept(record.toConsumerRecord());

        ParsleyEngine.FrontierCallback listener = frontier -> {
            frontierStore.put(ParsleyAttributes.FRONTIER_KEY, frontier.toBytes());
            snapshots.add(frontier);
            frontierListener.onFrontierAdvanced(frontier);
        };

        CausalBufferStore<KIn, VIn> buffer = new ParsleyBufferStore<>(bufferStore, serializer);
        WaitIndex waitIndex = new ParsleyWaitIndex(waitIndexStore);

        ParsleyMetrics metrics = buildMetrics(context);

        this.engine = new ParsleyEngine<>(policy, onViolation, initialFrontier,
                engineDeadLetter, listener, buffer, waitIndex, metrics);

        ProcessorContext<KOut, VOut> stamping = new ParsleyProcessorContext<>(
                context, () -> stampClock, () -> Optional.ofNullable(deliveryMetadata));
        delegate.init(stamping);

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

    private List<ParsleyRecord<KIn, VIn>> gate(ParsleyRecord<KIn, VIn> record) {
        snapshots.clear();
        return engine.onRecord(record);
    }

    private List<ParsleyRecord<KIn, VIn>> evict() {
        snapshots.clear();
        return engine.evictNow();
    }

    private void deliver(List<ParsleyRecord<KIn, VIn>> admitted) {
        for (int i = 0; i < admitted.size(); i++) {
            ParsleyRecord<KIn, VIn> record = admitted.get(i);
            stampClock = snapshots.get(i);
            deliveryMetadata = new ParsleyRecordMetadata(
                    record.sourcePartition().topic(), record.sourcePartition().partition(), record.sourceOffset());
            delegate.process(new Record<>(record.key(), record.value(), record.timestamp(), record.toHeaders()));
        }
        deliveryMetadata = null;
        stampClock = engine.frontier();
    }

    private ParsleyRecord<KIn, VIn> ingest(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        TopicPartition source = new TopicPartition(topic, meta.map(RecordMetadata::partition).orElse(0));
        Uuid topicId = topicUuids.getOrDefault(topic, CausalPosition.nameUuid(topic));
        return ParsleyRecord.of(record, source, meta.map(RecordMetadata::offset).orElse(0L), topicId);
    }
}
