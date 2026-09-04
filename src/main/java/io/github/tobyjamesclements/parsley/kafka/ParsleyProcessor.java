package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.header.Header;
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

import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.Deliverability;
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
 * delivery. The result is picked up on the next record or punctuation. Initialisation seeds
 * one round synchronously when the source is free, and waits only a bounded time when it is
 * not — a slow broker must not stack initialising tasks against the poll interval.
 *
 * @see ProcessTopology
 * @see DeliverySeam
 */
final class ParsleyProcessor implements Processor<byte[], byte[], byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(ParsleyProcessor.class);

    private final ProcessDefinition definition;
    private final Map<String, TopicInfo> topics;
    private final FactsSource factsSource;
    private final Duration factsInterval;
    private final java.util.concurrent.Executor factsExecutor;
    private final int metadataBudgetBytes;
    private final ProcessDiagnostics diagnostics;

    private final java.util.concurrent.atomic.AtomicLong incarnation =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicReference<GatheredRound> pendingRound =
            new java.util.concurrent.atomic.AtomicReference<>();
    /**
     * The gather slot: zero when no gather is in flight, otherwise the incarnation that
     * launched the one that is. Acquired and freed by compare-and-set, so only the
     * incarnation that acquired the slot can free it — a superseded gather completing after
     * a revival cannot free the slot a successor incarnation's own gather holds.
     */
    private final java.util.concurrent.atomic.AtomicLong gatherSlot =
            new java.util.concurrent.atomic.AtomicLong();
    private final BudgetAlarm budgetAlarm = new BudgetAlarm();
    private Cancellable factsPunctuator;

    /**
     * A facts round tagged with the incarnation whose in-memory state produced its hints.
     *
     * <p>A round is applied only by the incarnation that launched it. A revived task restores
     * the engine to committed state, so hints taken from the previous incarnation may describe
     * feed progress that was rolled back; a probe answering them must not reach the restored
     * engine as durable truth.
     */
    private record GatheredRound(long incarnation, io.github.tobyjamesclements.parsley.core.PositionFacts facts) {}

    private ProcessorContext<byte[], byte[]> context;
    private ProcessEngine engine;
    private DeliverySeam seam;
    private int partition;
    /** Monotonic time facts were last applied to the engine, or zero when none have been. */
    private long factsAppliedAtNanos;
    private final Map<String, ChannelId> channelByTopic = new HashMap<>();
    private final Map<ChannelId, String> topicByChannel = new HashMap<>();

    /**
     * @param definition          the process this instance runs
     * @param topics              resolved identity and width for every topic it uses
     * @param factsSource         where broker facts come from
     * @param factsInterval       how often to refresh them
     * @param factsExecutor       where the refresh runs
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @param diagnostics         where this task publishes its status
     */
    ParsleyProcessor(ProcessDefinition definition, Map<String, TopicInfo> topics,
                     FactsSource factsSource, Duration factsInterval,
                     java.util.concurrent.Executor factsExecutor, int metadataBudgetBytes,
                     ProcessDiagnostics diagnostics) {
        this.definition = definition;
        this.topics = topics;
        this.factsSource = factsSource;
        this.factsInterval = factsInterval;
        this.factsExecutor = factsExecutor;
        this.metadataBudgetBytes = metadataBudgetBytes;
        this.diagnostics = diagnostics;
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
        pendingRound.set(null);
        gatherSlot.set(0);
        if (factsPunctuator != null) {
            factsPunctuator.cancel();
            factsPunctuator = null;
        }

        this.context = context;
        partition = context.taskId().partition();
        factsAppliedAtNanos = 0;

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

        Map<String, DeliverySeam.ByteStore> appStores = new HashMap<>();
        Map<String, String> serdeTopicByStore = new HashMap<>();
        for (Store<?, ?> store : definition.stores()) {
            KeyValueStore<Bytes, byte[]> kv = context.getStateStore(store.name());
            appStores.put(store.name(), new DeliverySeam.ByteStore() {
                @Override
                public byte[] get(byte[] key) {
                    return kv.get(Bytes.wrap(key));
                }

                @Override
                public void put(byte[] key, byte[] value) {
                    kv.put(Bytes.wrap(key), value);
                }

                @Override
                public void delete(byte[] key) {
                    kv.delete(Bytes.wrap(key));
                }
            });
            // Composed once per store: the same name start() validated, recomposing it per
            // state access would be a dead length check on the hot path.
            serdeTopicByStore.put(store.name(),
                    ProcessTopology.changelogName(context.applicationId(), store.name()));
        }
        // A fresh seam per initialisation: its latch belongs to this incarnation, so a
        // refusal the previous one left behind cannot fail the restored task's first step.
        ProcessEngine thisEngine = engine;
        seam = new DeliverySeam(definition, appStores, serdeTopicByStore, thisEngine::causesHeaderForEmission,
                (topic, key, value, headers, timestamp) ->
                        context.forward(new Record<>(key, value, timestamp, headers), ProcessTopology.sinkName(topic)));

        factsPunctuator = context.schedule(factsInterval, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            startGatherIfIdle();
            applyGatheredFacts();
            drain();
            engine.flushHolds();
            observeFrontier();
            publishStatus();
        });

        ingestFacts();
        publishStatus();
    }

    private void applyGatheredFacts() {
        GatheredRound round = pendingRound.getAndSet(null);
        if (round != null && round.incarnation() == incarnation.get()) {
            engine.onFacts(round.facts());
            factsAppliedAtNanos = System.nanoTime();
        }
    }

    /**
     * Publishes this task's delivery state for {@code status()} (D103), once per facts
     * interval on the stream thread, where the engine lives.
     */
    private void publishStatus() {
        Optional<Duration> sinceLastFacts = factsAppliedAtNanos == 0
                ? Optional.empty()
                : Optional.of(Duration.ofNanos(System.nanoTime() - factsAppliedAtNanos));
        diagnostics.publish(TaskSnapshots.snapshot(engine, partition, this::topicNameOf, sinceLastFacts));
    }

    /** A received channel's topic name; a blocker is always on a received channel. */
    private String topicNameOf(ChannelId channel) {
        String topic = topicByChannel.get(channel);
        return topic != null ? topic : channel.toString();
    }

    private void startGatherIfIdle() {
        long launchIncarnation = incarnation.get();
        if (!gatherSlot.compareAndSet(0, launchIncarnation)) {
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
                    // leave a superseded round behind; the apply-time incarnation check
                    // discards it.
                    if (incarnation.get() == launchIncarnation) {
                        pendingRound.set(new GatheredRound(launchIncarnation, facts));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.warn("{}: position facts unavailable, retrying next round", definition.name(), e);
                } finally {
                    gatherSlot.compareAndSet(launchIncarnation, 0);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            gatherSlot.compareAndSet(launchIncarnation, 0);
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
        pendingRound.set(null);
        gatherSlot.set(0);
        if (factsPunctuator != null) {
            factsPunctuator.cancel();
            factsPunctuator = null;
        }
        if (engine != null) {
            diagnostics.retire(partition);
        }
    }

    /**
     * The once-per-process latch behind the 80%-of-budget warning (D53): the operator is
     * pointed at the growth law once, ahead of the budget's fail-closed wall, not on every
     * facts round the frontier spends above the line. Deliberately never reset by
     * {@code init} or {@code close}: a revived task is the same process, and D53's "warns
     * once" is per process, not per incarnation. Extracted so the threshold and the latch
     * are pinnable without capturing log output; {@link #observeFrontier()} owns the
     * message.
     */
    static final class BudgetAlarm {
        private boolean warned;

        /**
         * Decides whether the warning fires now: exactly once, the first time the encoded
         * frontier reaches 80% of the budget.
         *
         * @param frontierBytes the frontier's encoded width, in bytes
         * @param budgetBytes   the metadata budget, in bytes
         * @return whether to emit the warning
         */
        boolean shouldWarn(int frontierBytes, int budgetBytes) {
            if (warned || frontierBytes < budgetBytes * 0.8) {
                return false;
            }
            warned = true;
            return true;
        }
    }

    private void observeFrontier() {
        int bytes = engine.frontierBytes();
        if (budgetAlarm.shouldWarn(bytes, metadataBudgetBytes)) {
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
        if (pendingRound.get() != null) {
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

    /**
     * The channels worth probing for a trailing never-yielding run: those a held head is
     * waiting on (D107). A channel that itself holds messages settles at its head whatever
     * the broker says above it, and a channel nothing waits on has nothing a probe could
     * release, so probing every received channel — the previous rule — spent a second per
     * idle channel per round for no fact anyone would act on.
     */
    private Map<ChannelId, Long> probeHints() {
        Map<ChannelId, Long> hints = new java.util.TreeMap<>();
        if (engine.heldCountTotal() == 0) {
            return hints;
        }
        for (ChannelId channel : engine.receivedChannelSet()) {
            if (engine.heldCount(channel) == 0) {
                continue;
            }
            engine.headVerdict(channel).ifPresent(verdict -> {
                if (verdict instanceof Deliverability.Held held) {
                    for (Deliverability.Blocker blocker : held.blockers()) {
                        ChannelId blocked = blocker.channel();
                        if (engine.heldCount(blocked) == 0) {
                            engine.fedUpTo(blocked).ifPresent(fed -> hints.put(blocked, fed));
                        }
                    }
                }
            });
        }
        return hints;
    }

    private void ingestFacts() {
        // Unhinted on purpose (D107): the seed runs on the stream thread inside task
        // initialisation, and a probe costs a poll loop per round. Facts are lower bounds,
        // so the first background round probes instead, one interval later.
        Map<ChannelId, Long> hints = Map.of();
        io.github.tobyjamesclements.parsley.core.PositionFacts facts;
        try {
            facts = factsSource.gatherForSeed(engine.receivedChannelSet(), hints,
                    engine.frontierSnapshot().byChannel().keySet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (Exception e) {
            LOG.warn("{}: position facts unavailable, retrying next round", definition.name(), e);
            return;
        }
        engine.onFacts(facts);
        factsAppliedAtNanos = System.nanoTime();
    }

    private void drain() {
        while (true) {
            Optional<DeliverableMessage> next = engine.nextDeliverable();
            if (next.isEmpty()) {
                return;
            }
            DeliverableMessage message = next.get();
            engine.markDelivered(message.channel(), message.position());
            seam.deliver(definition.input(topicByChannel.get(message.channel())), message);
        }
    }
}
