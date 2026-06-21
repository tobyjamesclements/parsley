package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The processor node behind {@link ParsleyConsumer}'s internal outbox topology: gates each
 * incoming record on {@link ParsleyEngine} and forwards admitted records straight into the
 * outbox topic, headers untouched.
 *
 * <p>Unlike {@link ParsleyProcessor} — which decorates a user {@link Processor} and stamps every
 * forward with the delivery-time frontier via {@link ParsleyProcessorContext}, so a downstream
 * causal consumer in the same topology sees the chain — this node has no delegate and does no
 * stamping. {@link ParsleyConsumer} wants the application to see the producer's original
 * {@link ParsleyAttributes#CAUSAL_DEPENDENCIES} header, not the delivery-time frontier, so
 * forwarding the engine's output unmodified is exactly what's wanted.
 */
final class ParsleyOutboxProcessor implements Processor<byte[], byte[], byte[], byte[]> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyOutboxProcessor.class);

    private final CausalBufferLimit limit;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String positionIndexStoreName;
    private final Map<String, Uuid> topicUuids;

    private ProcessorContext<byte[], byte[]> context;
    private ParsleyEngine<byte[], byte[]> engine;
    private ParsleyMetrics.Wired wiredMetrics;
    private Cancellable restoredOverflowSchedule;

    private ParsleyOutboxProcessor(CausalBufferLimit limit,
                                    String frontierStoreName,
                                    String bufferStoreName,
                                    String positionIndexStoreName,
                                    Map<String, Uuid> topicUuids) {
        this.limit = limit;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.positionIndexStoreName = positionIndexStoreName;
        this.topicUuids = topicUuids;
    }

    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
        this.context = context;
        KeyValueStore<String, byte[]> frontierStore = context.getStateStore(frontierStoreName);
        KeyValueStore<Long, byte[]> bufferStore = context.getStateStore(bufferStoreName);
        KeyValueStore<byte[], byte[]> positionIndexStore = context.getStateStore(positionIndexStoreName);

        ParsleyClock initialFrontier = ParsleyClock.empty();
        byte[] stored = frontierStore.get(ParsleyAttributes.FRONTIER_KEY);
        if (stored != null) {
            initialFrontier = ParsleyClock.fromBytes(stored);
        }
        log.debug("Outbox processor initialized [task: {}] — frontier: {}", context.taskId(), initialFrontier);

        ParsleyEngine.FrontierCallback listener = frontier ->
                frontierStore.put(ParsleyAttributes.FRONTIER_KEY, frontier.toBytes());

        ParsleySerializer<byte[], byte[]> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.ByteArray(), t -> Serdes.ByteArray()));
        ParsleyBufferStore<byte[], byte[]> buffer = new RocksBufferStore<>(bufferStore, serializer);
        ParsleyPositionIndex positionIndex = new RocksPositionIndex(positionIndexStore);

        this.wiredMetrics = ParsleyMetrics.wire(context);

        this.engine = new ParsleyEngine<>(limit, initialFrontier,
                listener, buffer, positionIndex, wiredMetrics.metrics(), context::currentSystemTimeMs);

        // See ParsleyProcessor.init() for why this must run as a punctuation, not inline here.
        restoredOverflowSchedule = context.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    restoredOverflowSchedule.cancel();
                    forward(engine.evictOverflow());
                });

        engine.evictionInterval().ifPresent(interval ->
                context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timestamp -> forward(engine.evictExpired())));
    }

    @Override
    public void process(Record<byte[], byte[]> record) {
        forward(engine.onRecord(ingest(record)));
    }

    @Override
    public void close() {
        log.debug("Outbox processor closing [task: {}]", context.taskId());
        wiredMetrics.close(context.metrics());
    }

    private void forward(List<ParsleyRecord<byte[], byte[]>> admitted) {
        for (ParsleyRecord<byte[], byte[]> record : admitted) {
            context.forward(new Record<>(record.key(), record.value(), record.timestamp(), record.toHeaders()));
        }
    }

    private ParsleyRecord<byte[], byte[]> ingest(Record<byte[], byte[]> record) {
        Optional<RecordMetadata> meta = context.recordMetadata();
        String topic = meta.map(RecordMetadata::topic).orElse("");
        TopicPartition source = new TopicPartition(topic, meta.map(RecordMetadata::partition).orElse(0));
        Uuid topicId = topicUuids.get(topic);
        if (topicId == null) {
            throw new IllegalStateException(
                    "no CausalTopic registered for topic '" + topic
                            + "'; call addCausalTopic(...) on the CausalConsumers builder for every subscribed topic");
        }
        return ParsleyRecord.of(record, source, meta.map(RecordMetadata::offset).orElse(0L), topicId);
    }

    /**
     * Builds the {@link ProcessorSupplier} for {@link ParsleyOutboxProcessor}, declaring the
     * frontier/buffer/position-index stores it needs. Store-name suffixes match
     * {@link CausalProcessors} so an existing deployment's state stores resolve to the same names.
     */
    static ProcessorSupplier<byte[], byte[], byte[], byte[]> supplier(
            CausalBufferLimit limit, String storeName, Map<String, Uuid> topicUuids) {
        String frontierStoreName = storeName + "-frontier";
        String bufferStoreName = storeName + "-buffer";
        String positionIndexStoreName = storeName + "-position-index";
        return new ProcessorSupplier<>() {
            @Override
            public Processor<byte[], byte[], byte[], byte[]> get() {
                return new ParsleyOutboxProcessor(limit, frontierStoreName, bufferStoreName,
                        positionIndexStoreName, topicUuids);
            }

            @Override
            public Set<StoreBuilder<?>> stores() {
                Set<StoreBuilder<?>> stores = new HashSet<>();
                stores.add(ParsleyStores.frontierStore(frontierStoreName));
                stores.add(ParsleyStores.bufferStore(bufferStoreName));
                stores.add(ParsleyStores.positionIndexStore(positionIndexStoreName));
                return stores;
            }
        };
    }
}
