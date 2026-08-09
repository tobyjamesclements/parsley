package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
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
 * The Kafka Streams face of one process. Byte-level in and out: the application's serdes are applied only at the seam
 * (SPEC Structural 3), so record keys and values are exactly the application's bytes (SPEC Safety 4, 5). All ordering
 * belongs to the {@link ProcessEngine}; this class feeds it records, position facts from the punctuator, and carries
 * out deliveries and emissions within the step. A {@link ParsleyFailClosedException} propagates out of {@code process},
 * aborting the step and stopping the process: failing closed (SPEC Safety 7, 8).
 */
final class ParsleyProcessor implements Processor<byte[], byte[], byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ParsleyProcessor.class);

    private final ProcessDefinition definition;
    private final Map<String, TopicInfo> topics;
    private final FactsSource factsSource;
    private final Duration factsInterval;
    private final java.util.concurrent.Executor factsExecutor;
    private final int metadataBudgetBytes;
    /** A gathered-but-unapplied facts round. Written by the facts executor, taken (exactly once — a completed
     * round is never reused) and applied on the stream thread. Every fact is a per-position lower bound, so
     * applying a round one interval late is always safe; only the copying discipline matters (D54). */
    private final java.util.concurrent.atomic.AtomicReference<io.github.tobyjamesclements.parsley.core.PositionFacts> gathered =
            new java.util.concurrent.atomic.AtomicReference<>();
    private final java.util.concurrent.atomic.AtomicBoolean gatherInFlight =
            new java.util.concurrent.atomic.AtomicBoolean();
    private boolean budgetWarned;

    private ProcessorContext<byte[], byte[]> context;
    private ProcessEngine engine;
    private final Map<String, ChannelId> channelByTopic = new HashMap<>();
    private final Map<ChannelId, String> topicByChannel = new HashMap<>();
    private final Map<String, KeyValueStore<Bytes, byte[]>> appStores = new HashMap<>();
    private StateReader stateReader;

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

    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
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

        context.schedule(factsInterval, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            // Start (or continue) a background round first, then apply whichever round has completed — with a
            // synchronous executor that is this round; with the shared background thread it is the previous one,
            // one interval late, which lower-bound facts make always safe (D54).
            startGatherIfIdle();
            applyGatheredFacts();
            drain();
            engine.flushHolds();
            observeFrontier();
        });
        // Seed read-position baselines now rather than waiting a full interval; failures here are liveness-only
        // (a fact source outage), except the engine itself refusing, which must propagate. Deliveries deliberately
        // wait for process() or the first punctuation: forwarding from within Processor#init is an unexercised host
        // path, and nothing is lost by deferring at most one facts interval. Gathering here is synchronous by
        // design — initialisation is one-time and off the per-record path.
        ingestFacts();
    }

    /** Apply a completed background round, exactly once. Every fact is a per-position lower bound (D54), so a
     * round applied one interval after it was gathered releases and prunes exactly what a fresh round would —
     * later, never wrongly. */
    private void applyGatheredFacts() {
        io.github.tobyjamesclements.parsley.core.PositionFacts facts = gathered.getAndSet(null);
        if (facts != null) {
            engine.onFacts(facts);
        }
    }

    /** Start a background facts round unless one is already running. Inputs are snapshotted on the stream thread
     * — the gather never touches engine-owned collections (D54). */
    private void startGatherIfIdle() {
        if (!gatherInFlight.compareAndSet(false, true)) {
            return;
        }
        java.util.Set<ChannelId> received = java.util.Set.copyOf(engine.receivedChannelSet());
        Map<ChannelId, Long> hints = probeHints();
        java.util.Set<ChannelId> frontier = engine.frontierSnapshot().byChannel().keySet();
        try {
            factsExecutor.execute(() -> {
                try {
                    gathered.set(factsSource.gather(received, hints, frontier));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // the runtime is closing; stand down quietly
                } catch (Exception e) {
                    LOG.warn("{}: position facts unavailable, retrying next round", definition.name(), e);
                } finally {
                    gatherInFlight.set(false);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // The runtime is closing and has shut the facts executor down while this task's punctuator is still
            // winding down: not a failure of the process — do not let it kill the stream thread and be recorded
            // as one during an intended close.
            gatherInFlight.set(false);
        }
    }

    /** SPEC Operational 5: the size of this process's causal metadata, observable in operation. */
    private void observeFrontier() {
        int bytes = engine.frontierBytes();
        if (!budgetWarned && bytes >= metadataBudgetBytes * 0.8) {
            budgetWarned = true;
            LOG.warn("{}: causal metadata at {} bytes ({} channels) — at 80% of the {}-byte budget, the process"
                            + " will fail closed on reaching it; see docs/DESIGN.md §2 for the growth law",
                    definition.name(), bytes, engine.frontierSize(), metadataBudgetBytes);
        }
        LOG.debug("{}: causal frontier {} channels, {} bytes", definition.name(), engine.frontierSize(), bytes);
    }

    @Override
    public void process(Record<byte[], byte[]> record) {
        // A completed background round may carry a stop signal (a recreated channel; a dead channel with holds);
        // apply it before feeding rather than letting it sit until the next punctuation — facts are per-position
        // lower bounds, safe to apply at any point on the stream thread (D54), and the stop signals inside them
        // should not wait a full extra interval while records keep being delivered.
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

    /** Hints invite the facts source to probe never-yielding runs just above the covered frontier; only worth
     * the round trips while something is actually held (SPEC Liveness 3). One builder feeds both the synchronous
     * seed round and every background round, so the hint policy cannot silently diverge between the two paths. */
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
            Thread.currentThread().interrupt(); // an embedder cancelled startup: keep the signal, skip the round
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
            // Delivering nothing and moving on would deliver past the message (SPEC Safety 3): fail closed instead.
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
            // SPEC Structural 19: an emission naming a channel outside the declared send set fails the step.
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL,
                    definition.name() + " emitted to undeclared channel " + topic);
        }
        RecordHeaders headers = toKafkaHeaders(emission.headers());
        byte[] keyBytes = emission.key() == null
                ? null : emission.channel().keySerde().serializer().serialize(topic, headers, emission.key());
        byte[] valueBytes = emission.value() == null
                ? null : emission.channel().valueSerde().serializer().serialize(topic, headers, emission.value());
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
