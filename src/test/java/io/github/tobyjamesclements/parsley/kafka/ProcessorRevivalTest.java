package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes what survives a task revival, by driving {@link ParsleyProcessor} directly.
 *
 * <p>When a commit fails retriably, Kafka Streams keeps the task and its processor objects,
 * restores state, and calls {@code init} again on the same instance. {@code TopologyTestDriver}
 * never does this, so these tests do it by hand: the facts rounds, in-flight gathers and
 * punctuator of the pre-revival incarnation must all be inert afterwards. A facts round answers
 * hints taken from the engine's in-memory feed positions, which include records fed inside the
 * transaction the revival rolled back; applying such a round to the restored engine would
 * assert rolled-back progress as durable truth and can deliver an effect before its cause.
 */
class ProcessorRevivalTest {
    private static final UUID IN1_ID = new UUID(200, 1);
    private static final UUID IN2_ID = new UUID(200, 2);
    private static final Map<String, TopicInfo> TOPICS = Map.of(
            "in1", new TopicInfo(IN1_ID, 1),
            "in2", new TopicInfo(IN2_ID, 1));
    private static final ChannelId IN1 = new ChannelId(IN1_ID, 0);

    /** Returns empty facts immediately, except that an armed call parks until released. */
    static final class ControllableFacts implements FactsSource {
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean armed;
        volatile PositionFacts armedResult = PositionFacts.EMPTY;

        @Override
        public PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                                    Set<ChannelId> frontierChannels) throws InterruptedException {
            calls.incrementAndGet();
            if (armed) {
                armed = false;
                entered.countDown();
                release.await();
                return armedResult;
            }
            return PositionFacts.EMPTY;
        }
    }

    @TempDir
    Path stateDir;

    private final List<String> delivered = new ArrayList<>();
    private ControllableFacts facts;
    private ExecutorService executor;
    private KeyValueStore<Bytes, byte[]> orderingStore;
    private ParsleyProcessor processor;
    private MockProcessorContext<byte[], byte[]> context;

    @BeforeEach
    void setUp() {
        facts = new ControllableFacts();
        executor = Executors.newSingleThreadExecutor();
        processor = new ParsleyProcessor(twoInputRecorder(delivered), TOPICS, facts,
                Duration.ofMillis(100), executor, 64 * 1024);
        context = newContext();
        orderingStore = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(ProcessTopology.ORDERING_STORE),
                        Serdes.Bytes(), Serdes.ByteArray())
                .withLoggingDisabled()
                .build();
        orderingStore.init(context.getStateStoreContext(), orderingStore);
        context.addStateStore(orderingStore);
        processor.init(context);
    }

    @AfterEach
    void tearDown() {
        processor.close();
        executor.shutdownNow();
        orderingStore.close();
    }

    /**
     * A gather launched before revival completes after it. The revived engine must never see
     * that round, even though it reports the exact evidence that would free the held effect.
     */
    @Test
    void factsGatheredBeforeRevivalNeverReachTheRevivedEngine() throws Exception {
        feedHeldEffect();
        assertEquals(List.of(), delivered, "the effect is held: its cause on in1 is undelivered");

        facts.armed = true;
        punctuate(context);
        assertTrue(facts.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        MockProcessorContext<byte[], byte[]> revived = revive();

        facts.armedResult = new PositionFacts(Map.of(IN1, 6L), Map.of(), Set.of());
        facts.release.countDown();
        awaitExecutorDrained();

        punctuate(revived);
        awaitExecutorDrained();
        punctuate(revived);
        assertEquals(List.of(), delivered,
                "a round from the previous incarnation must not free the hold");
    }

    /**
     * The same round applied by the incarnation that launched it does free the hold, so the
     * revival test above fails on any regression that lets stale rounds through.
     */
    @Test
    void theSameRoundAppliedByItsOwnIncarnationFreesTheHold() throws Exception {
        feedHeldEffect();

        facts.armed = true;
        punctuate(context);
        assertTrue(facts.entered.await(5, TimeUnit.SECONDS), "the gather must have started");
        facts.armedResult = new PositionFacts(Map.of(IN1, 6L), Map.of(), Set.of());
        facts.release.countDown();
        awaitExecutorDrained();

        punctuate(context);
        assertEquals(List.of("B"), delivered, "the report frees the hold when no revival intervened");
    }

    /** Re-initialisation cancels the previous punctuator instead of stacking a second one. */
    @Test
    void revivalCancelsThePreviousIncarnationsPunctuator() {
        MockProcessorContext<byte[], byte[]> revived = revive();

        assertEquals(1, context.scheduledPunctuators().size());
        assertTrue(context.scheduledPunctuators().get(0).cancelled(),
                "the punctuator scheduled before revival must be cancelled");
        assertEquals(1, revived.scheduledPunctuators().size());
        assertFalse(revived.scheduledPunctuators().get(0).cancelled());
    }

    /** A gather in flight across revival does not leave the gather slot occupied. */
    @Test
    void revivalWhileAGatherIsInFlightStillAllowsFreshGathers() throws Exception {
        facts.armed = true;
        punctuate(context);
        assertTrue(facts.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        MockProcessorContext<byte[], byte[]> revived = revive();
        punctuate(revived);
        facts.release.countDown();
        awaitExecutorDrained();

        // One synchronous gather per init, the parked one, and the post-revival launch. Were
        // the slot still held by the parked gather, the fourth would never have started.
        assertEquals(4, facts.calls.get());
    }

    /** Holds an effect on in2 whose cause is the undelivered in1@5. */
    private void feedHeldEffect() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 5L)))));
        context.setRecordMetadata("in2", 0, 0L);
        processor.process(new Record<>("B".getBytes(), "B".getBytes(), 0L, headers));
    }

    /**
     * Re-initialises the processor the way {@code closeDirtyAndRevive} does: same instance,
     * same stores, fresh context. The in-memory store stands for the restored committed state;
     * here everything fed so far was committed, so its content carries over unchanged.
     */
    private MockProcessorContext<byte[], byte[]> revive() {
        MockProcessorContext<byte[], byte[]> revived = newContext();
        revived.addStateStore(orderingStore);
        processor.init(revived);
        return revived;
    }

    private MockProcessorContext<byte[], byte[]> newContext() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "revival-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return new MockProcessorContext<>(props, new TaskId(0, 0), stateDir.toFile());
    }

    private static void punctuate(MockProcessorContext<byte[], byte[]> ctx) {
        List<MockProcessorContext.CapturedPunctuator> punctuators = ctx.scheduledPunctuators();
        punctuators.get(punctuators.size() - 1).getPunctuator().punctuate(0L);
    }

    /** Waits until every task already handed to the facts executor has run. */
    private void awaitExecutorDrained() throws Exception {
        executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
    }

    private static ProcessDefinition twoInputRecorder(List<String> delivered) {
        Channel<String, String> in1 = Channel.of("in1", Serdes.String(), Serdes.String());
        Channel<String, String> in2 = Channel.of("in2", Serdes.String(), Serdes.String());
        return ProcessDefinition.named("p")
                .receives(in1, (delivery, state) -> {
                    delivered.add(delivery.value());
                    return Effects.none();
                })
                .receives(in2, (delivery, state) -> {
                    delivered.add(delivery.value());
                    return Effects.none();
                })
                .build();
    }
}
