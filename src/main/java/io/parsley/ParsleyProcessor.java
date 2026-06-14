package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Wraps a user {@link Processor} and gates delegation on the causal frontier: an incoming record is
 * delivered to {@code delegate.process(...)} only once the frontier dominates its dependency clock
 * (or the policy forces it). State reads/writes the delegate performs and every record it forwards
 * are therefore causally ordered, and forwards are clock-stamped by a
 * {@link StampingProcessorContext}.
 *
 * <p>Held records are persisted to a changelog-backed buffer store and restored on {@code init}, so
 * they survive a restart (a buffered record's source offset is committed past it, so it would
 * otherwise be lost). The frontier-before-forward invariant from {@link CausalEngine} is preserved
 * on both the admit and punctuator paths.
 *
 * @param <KIn>  the input key type
 * @param <VIn>  the input value type
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class ParsleyProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private final Processor<KIn, VIn, KOut, VOut> delegate;
    private final BufferingPolicy policy;
    private final ViolationHandler onViolation;
    private final Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink;
    private final BufferedRecordCodec<KIn, VIn> codec;
    private final String frontierStoreName;
    private final String bufferStoreName;

    /** Frontier snapshots captured per advanced record during a single engine call, for stamping. */
    private final List<VectorClock> snapshots = new ArrayList<>();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, byte[]> frontierStore;
    private KeyValueStore<String, byte[]> bufferStore;
    private CausalEngine<KIn, VIn> engine;
    private volatile VectorClock stampClock = VectorClock.empty();
    private volatile RecordMetadata deliveryMetadata;

    ParsleyProcessor(Processor<KIn, VIn, KOut, VOut> delegate,
                              BufferingPolicy policy,
                              ViolationHandler onViolation,
                              Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                              BufferedRecordCodec<KIn, VIn> codec,
                              String frontierStoreName,
                              String bufferStoreName) {
        this.delegate = delegate;
        this.policy = policy;
        this.onViolation = onViolation;
        this.deadLetterSink = deadLetterSink;
        this.codec = codec;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        this.frontierStore = context.getStateStore(frontierStoreName);
        this.bufferStore = context.getStateStore(bufferStoreName);

        VectorClock initialFrontier = VectorClock.empty();
        byte[] stored = frontierStore.get(Attributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = VectorClock.fromBytes(stored);
        }
        this.stampClock = initialFrontier;

        Consumer<CausalRecord<KIn, VIn>> engineDeadLetter = deadLetterSink == null
                ? null
                : record -> deadLetterSink.accept(record.toConsumerRecord());

        CausalEngine.FrontierListener listener = frontier -> {
            frontierStore.put(Attributes.FRONTIER_KEY, frontier.toBytes());
            snapshots.add(frontier);
        };

        BufferPersistence<KIn, VIn> persistence = new BufferPersistence<>() {
            @Override
            public void onHeld(CausalRecord<KIn, VIn> record, VectorClock dependencies) {
                bufferStore.put(BufferedRecordCodec.storeKey(record), codec.serialize(record, dependencies));
            }

            @Override
            public void onUnheld(CausalRecord<KIn, VIn> record) {
                bufferStore.delete(BufferedRecordCodec.storeKey(record));
            }
        };

        this.engine = new CausalEngine<>(policy, onViolation, initialFrontier,
                engineDeadLetter, listener, persistence);

        restoreHeldRecords();

        ProcessorContext<KOut, VOut> stamping = new StampingProcessorContext<>(
                context, () -> stampClock, () -> Optional.ofNullable(deliveryMetadata));
        delegate.init(stamping);

        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timestamp -> deliver(evict())));
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        deliver(gate(toCausalRecord(record)));
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** Runs an incoming record through the engine, resetting the per-call snapshot buffer first. */
    private List<CausalRecord<KIn, VIn>> gate(CausalRecord<KIn, VIn> record) {
        snapshots.clear();
        return engine.onRecord(record);
    }

    /** Evicts the buffer (limit fired), resetting the per-call snapshot buffer first. */
    private List<CausalRecord<KIn, VIn>> evict() {
        snapshots.clear();
        return engine.evictNow();
    }

    /**
     * Delivers each admitted record to the delegate, stamping its forwards with the frontier as of
     * that record's admission. {@code admitted} and {@link #snapshots} are produced in lock-step by
     * the engine (one frontier advance per admitted record), so they zip 1:1.
     */
    private void deliver(List<CausalRecord<KIn, VIn>> admitted) {
        for (int i = 0; i < admitted.size(); i++) {
            CausalRecord<KIn, VIn> record = admitted.get(i);
            stampClock = snapshots.get(i);
            // So delegate.process sees the delivered record's true origin, even after buffering.
            deliveryMetadata = new DeliveredRecordMetadata(
                    record.sourcePartition().topic(), record.sourcePartition().partition(), record.sourceOffset());
            delegate.process(rebuild(record));
        }
        // Reset so a user punctuator's forwards stamp the live frontier and see the real metadata.
        deliveryMetadata = null;
        stampClock = engine.frontier();
    }

    private void restoreHeldRecords() {
        try (KeyValueIterator<String, byte[]> all = bufferStore.all()) {
            while (all.hasNext()) {
                BufferedRecordCodec.Buffered<KIn, VIn> buffered = codec.deserialize(all.next().value);
                engine.restore(buffered.record(), buffered.dependencies());
            }
        }
    }

    private CausalRecord<KIn, VIn> toCausalRecord(Record<KIn, VIn> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        int partition = meta.map(RecordMetadata::partition).orElse(0);
        long offset = meta.map(RecordMetadata::offset).orElse(0L);

        List<CausalHeader> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new CausalHeader(header.key(), header.value()));
        }
        Header clockHeader = record.headers().lastHeader(Attributes.VECTOR_CLOCK);
        return new CausalRecord<>(
                record.key(), record.value(), record.timestamp(), headers,
                clockHeader == null ? null : clockHeader.value(),
                new TopicPartition(topic, partition), offset);
    }

    private Record<KIn, VIn> rebuild(CausalRecord<KIn, VIn> record) {
        return new Record<>(record.key(), record.value(), record.timestamp(), record.toHeaders());
    }
}
