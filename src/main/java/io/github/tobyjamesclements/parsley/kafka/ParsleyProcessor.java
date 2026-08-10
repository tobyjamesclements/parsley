package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Delivery;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.StateReader;
import io.github.tobyjamesclements.parsley.api.StoreDef;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.DeliverableMessage;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;
import io.github.tobyjamesclements.parsley.core.ReceivedMessage;

/**
 * The Kafka Streams processor driving one {@link ProcessEngine}.
 *
 * <p>Each record is fed to the engine, then every message the engine declares deliverable is
 * decoded, handed to its handler, and the handler's effects are written and forwarded. State
 * writes, sends and consumed positions commit together under {@code exactly_once_v2}.
 *
 * <p>Broker facts are refreshed on a separate executor, so a slow query cannot stall
 * delivery. The result is picked up on the next record or punctuation.
 *
 * @see ProcessTopology
 */
final class ParsleyProcessor implements Processor<byte[], byte[], byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(ParsleyProcessor.class);

    private final ProcessDefinition definition;
    private final Map<String, TopicInfo> topics;
    private final FactsSource factsSource;
    private final Duration factsInterval;
    private final java.util.concurrent.Executor factsExecutor;
    private final int metadataBudgetBytes;

    private final java.util.concurrent.atomic.AtomicLong incarnation =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicReference<GatheredRound> gathered =
            new java.util.concurrent.atomic.AtomicReference<>();
    /**
     * The gather slot: zero when no gather is in flight, otherwise the epoch that launched
     * the one that is. Acquired and freed by compare-and-set, so only the epoch that
     * acquired the slot can free it — a superseded gather completing after a revival cannot
     * free the slot a successor incarnation's own gather holds.
     */
    private final java.util.concurrent.atomic.AtomicLong gatherSlot =
            new java.util.concurrent.atomic.AtomicLong();
    private boolean budgetWarned;
    private Cancellable factsPunctuator;

    /**
     * A facts round tagged with the incarnation whose in-memory state produced its hints.
     *
     * <p>A round is applied only by the incarnation that launched it. A revived task restores
     * the engine to committed state, so hints taken from the previous incarnation may describe
     * feed progress that was rolled back; a probe answering them must not reach the restored
     * engine as durable truth.
     */
    private record GatheredRound(long epoch, io.github.tobyjamesclements.parsley.core.PositionFacts facts) {}

    private ProcessorContext<byte[], byte[]> context;
    private ProcessEngine engine;
    private final Map<String, ChannelId> channelByTopic = new HashMap<>();
    private final Map<ChannelId, String> topicByChannel = new HashMap<>();
    private final Map<String, KeyValueStore<Bytes, byte[]>> appStores = new HashMap<>();
    private StateReader stateReader;

    /**
     * @param definition          the process this instance runs
     * @param topics              resolved identity and width for every topic it uses
     * @param factsSource         where broker facts come from
     * @param factsInterval       how often to refresh them
     * @param factsExecutor       where the refresh runs
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     */
    ParsleyProcessor(ProcessDefinition definition, Map<String, TopicInfo> topics,
                     FactsSource factsSource, Duration factsInterval,
                     java.util.concurrent.Executor factsExecutor, int metadataBudgetBytes) {
        this.definition = definition;
        this.topics = topics;
        this.factsSource = factsSource;
        this.factsInterval = factsInterval;
        this.factsExecutor = factsExecutor;
        this.metadataBudgetBytes = metadataBudgetBytes;
    }

    /**
     * Builds the engine for this task and restores its ordering state.
     *
     * @param context the task context
     * @throws ParsleyFailClosedException if restored state cannot be read, or if the task
     *         width changed so that state no longer matches its partitioning
     */
    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
        // A revived task runs close() and then init() on this same instance against restored
        // state; both perform the same reset, so a lifecycle that re-initialises without
        // closing is covered too. Anything the previous incarnation left behind describes
        // rolled-back progress: its facts rounds are invalidated, its punctuator cancelled,
        // and the gather slot freed.
        incarnation.incrementAndGet();
        gathered.set(null);
        gatherSlot.set(0);
        if (factsPunctuator != null) {
            factsPunctuator.cancel();
            factsPunctuator = null;
        }

        this.context = context;
        int partition = context.taskId().partition();

        channelByTopic.clear();
        topicByChannel.clear();
        for (String topic : definition.receivedTopics()) {
            TopicInfo info = topics.get(topic);
            if (partition < info.partitions()) {
                ChannelId channel = new ChannelId(info.topicId(), partition);
                channelByTopic.put(topic, channel);
                topicByChannel.put(channel, topic);
            }
        }

        KeyValueStore<Bytes, byte[]> orderingStore = context.getStateStore(ProcessTopology.ORDERING_STORE);
        engine = new ProcessEngine(definition.name() + "-" + context.taskId(),
                topicByChannel, new StreamsOrderingStore(orderingStore), metadataBudgetBytes);

        appStores.clear();
        for (StoreDef<?, ?> def : definition.stores()) {
            appStores.put(def.name(), context.getStateStore(def.name()));
        }
        stateReader = new StoreStateReader();

        factsPunctuator = context.schedule(factsInterval, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            startGatherIfIdle();
            applyGatheredFacts();
            drain();
            engine.flushHolds();
            observeFrontier();
        });

        ingestFacts();
    }

    private void applyGatheredFacts() {
        GatheredRound round = gathered.getAndSet(null);
        if (round != null && round.epoch() == incarnation.get()) {
            engine.onFacts(round.facts());
        }
    }

    private void startGatherIfIdle() {
        long epoch = incarnation.get();
        if (!gatherSlot.compareAndSet(0, epoch)) {
            return;
        }
        java.util.Set<ChannelId> received = java.util.Set.copyOf(engine.receivedChannelSet());
        Map<ChannelId, Long> hints = probeHints();
        java.util.Set<ChannelId> frontier = engine.frontierSnapshot().byChannel().keySet();
        try {
            factsExecutor.execute(() -> {
                try {
                    io.github.tobyjamesclements.parsley.core.PositionFacts facts =
                            factsSource.gather(received, hints, frontier);
                    // Best-effort: a deposit racing a concurrent re-initialisation can still
                    // leave a superseded round behind; the apply-time epoch check discards it.
                    if (incarnation.get() == epoch) {
                        gathered.set(new GatheredRound(epoch, facts));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.warn("{}: position facts unavailable, retrying next round", definition.name(), e);
                } finally {
                    gatherSlot.compareAndSet(epoch, 0);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            gatherSlot.compareAndSet(epoch, 0);
        }
    }

    /**
     * Invalidates any in-flight facts gather and cancels the facts punctuator, so nothing
     * from this incarnation can act on a revived successor. On the revival path this runs
     * before the successor's {@code init}, which repeats the same reset.
     */
    @Override
    public void close() {
        incarnation.incrementAndGet();
        gathered.set(null);
        gatherSlot.set(0);
        if (factsPunctuator != null) {
            factsPunctuator.cancel();
            factsPunctuator = null;
        }
    }

    private void observeFrontier() {
        int bytes = engine.frontierBytes();
        if (!budgetWarned && bytes >= metadataBudgetBytes * 0.8) {
            budgetWarned = true;
            LOG.warn("{}: causal metadata at {} bytes ({} channels), at 80% of the {}-byte budget, the process"
                            + " will fail closed on reaching it; see docs/model.md for the growth law",
                    definition.name(), bytes, engine.frontierSize(), metadataBudgetBytes);
        }
        LOG.debug("{}: causal frontier {} channels, {} bytes", definition.name(), engine.frontierSize(), bytes);
    }

    /**
     * Feeds one record to the engine and delivers whatever that makes deliverable.
     *
     * @param record the record, as raw bytes
     * @throws ParsleyFailClosedException if the guarantee cannot be upheld, which stops this
     *         process
     */
    @Override
    public void process(Record<byte[], byte[]> record) {
        if (gathered.get() != null) {
            applyGatheredFacts();
        }
        RecordMetadata metadata = context.recordMetadata().orElseThrow(() ->
                new IllegalStateException("record without topic metadata reached " + definition.name()));
        ChannelId channel = channelByTopic.get(metadata.topic());
        if (channel == null) {
            throw new IllegalStateException(definition.name() + " fed from undeclared topic " + metadata.topic());
        }
        List<HeaderKV> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new HeaderKV(header.key(), header.value()));
        }
        engine.onReceive(new ReceivedMessage(
                channel, metadata.offset(), record.timestamp(), record.key(), record.value(), headers));
        drain();
        engine.flushHolds();
    }

    private Map<ChannelId, Long> probeHints() {
        Map<ChannelId, Long> hints = new java.util.TreeMap<>();
        if (engine.heldCountTotal() > 0) {
            for (ChannelId channel : engine.receivedChannelSet()) {
                engine.fedUpTo(channel).ifPresent(fed -> hints.put(channel, fed));
            }
        }
        return hints;
    }

    private void ingestFacts() {
        Map<ChannelId, Long> hints = probeHints();
        io.github.tobyjamesclements.parsley.core.PositionFacts facts;
        try {
            facts = factsSource.gather(engine.receivedChannelSet(), hints,
                    engine.frontierSnapshot().byChannel().keySet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (Exception e) {
            LOG.warn("{}: position facts unavailable, retrying next round", definition.name(), e);
            return;
        }
        engine.onFacts(facts);
    }

    private void drain() {
        while (true) {
            Optional<DeliverableMessage> next = engine.nextDeliverable();
            if (next.isEmpty()) {
                return;
            }
            DeliverableMessage message = next.get();
            engine.markDelivered(message.channel(), message.position());
            deliver(definition.input(topicByChannel.get(message.channel())), message);
        }
    }

    private <K, V> void deliver(ProcessDefinition.Input<K, V> input, DeliverableMessage message) {
        Channel<K, V> channel = input.channel();
        String topic = channel.topic();
        RecordHeaders receivedHeaders = toKafkaHeaders(message.headers());
        K key;
        V value;
        try {
            key = message.key() == null
                    ? null : channel.keySerde().deserializer().deserialize(topic, receivedHeaders, message.key());
            value = message.value() == null
                    ? null : channel.valueSerde().deserializer().deserialize(topic, receivedHeaders, message.value());
        } catch (RuntimeException e) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                    definition.name() + ": " + topic + "@" + message.position(), e);
        }

        Delivery<K, V> delivery = Delivery.of(channel, message.channel().partition(), message.position(),
                message.timestamp(), key, value, message.headers());
        Effects effects = input.handler().handle(delivery, stateReader);
        if (effects == null) {
            throw new IllegalStateException(definition.name() + ": handler for " + topic + " returned null effects");
        }
        for (Effects.StateWrite<?, ?> write : effects.writes()) {
            applyWrite(write);
        }
        for (Effects.Emission<?, ?> emission : effects.emissions()) {
            send(emission, message.timestamp());
        }
    }

    private <K, V> void applyWrite(Effects.StateWrite<K, V> write) {
        StoreDef<K, V> def = write.store();
        if (definition.store(def.name()) != def) {
            throw new IllegalStateException(
                    definition.name() + ": state write to undeclared store " + def.name());
        }
        KeyValueStore<Bytes, byte[]> store = appStores.get(def.name());
        String serdeTopic = storeSerdeTopic(def.name());
        byte[] keyBytes = def.keySerde().serializer().serialize(serdeTopic, write.key());
        if (write.value() == null) {
            store.delete(Bytes.wrap(keyBytes));
        } else {
            store.put(Bytes.wrap(keyBytes), def.valueSerde().serializer().serialize(serdeTopic, write.value()));
        }
    }

    private <K, V> void send(Effects.Emission<K, V> emission, long timestamp) {
        String topic = emission.channel().topic();
        if (definition.sendChannel(topic) == null) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL,
                    definition.name() + " emitted to undeclared channel " + topic);
        }
        RecordHeaders headers = toKafkaHeaders(emission.headers());
        byte[] keyBytes = emission.key() == null
                ? null : emission.channel().keySerde().serializer().serialize(topic, headers, emission.key());
        byte[] valueBytes = emission.value() == null
                ? null : emission.channel().valueSerde().serializer().serialize(topic, headers, emission.value());
        // The emission's own headers were checked at construction, but the serializers were
        // just handed the mutable collection; re-check before the genuine stamp goes on, so
        // a header-writing serializer fails here instead of poisoning every receiver.
        for (Header header : headers) {
            if (header.key().startsWith(io.github.tobyjamesclements.parsley.core.CausesCodec.RESERVED_HEADER_PREFIX)) {
                throw new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.RESERVED_HEADER_USED,
                        definition.name() + ": serializer for " + topic + " wrote reserved header '"
                                + header.key() + "'");
            }
        }
        headers.add(new RecordHeader(io.github.tobyjamesclements.parsley.core.CausesCodec.HEADER_KEY, engine.causesHeaderForEmission()));
        context.forward(new Record<>(keyBytes, valueBytes, timestamp, headers), ProcessTopology.sinkName(topic));
    }

    private String storeSerdeTopic(String storeName) {
        return context.applicationId() + "-" + storeName + "-changelog";
    }

    private static RecordHeaders toKafkaHeaders(List<HeaderKV> headers) {
        RecordHeaders kafkaHeaders = new RecordHeaders();
        for (HeaderKV header : headers) {
            kafkaHeaders.add(new RecordHeader(header.key(), header.value()));
        }
        return kafkaHeaders;
    }

    private final class StoreStateReader implements StateReader {
        @Override
        public <K, V> V get(StoreDef<K, V> store, K key) {
            if (definition.store(store.name()) != store) {
                throw new IllegalStateException(
                        definition.name() + ": state read from undeclared store " + store.name());
            }
            String serdeTopic = storeSerdeTopic(store.name());
            byte[] keyBytes = store.keySerde().serializer().serialize(serdeTopic, key);
            byte[] valueBytes = appStores.get(store.name()).get(Bytes.wrap(keyBytes));
            return valueBytes == null ? null : store.valueSerde().deserializer().deserialize(serdeTopic, valueBytes);
        }
    }
}
