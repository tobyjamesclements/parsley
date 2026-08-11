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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes what survives a task revival, by driving {@link ParsleyProcessor} directly.
 *
 * <p>When a commit fails retriably, Kafka Streams keeps the task and its processor objects,
 * restores state, and re-initialises the same instance: the revival path suspends the task,
 * which closes the topology, so the processor sees {@code close()} followed by {@code init}.
 * {@code TopologyTestDriver} never does this, so these tests do it by hand — in that order,
 * and also as {@code init} without {@code close}, which other lifecycles may produce. A facts
 * round answers hints taken from the engine's in-memory feed positions, which include records
 * fed inside the transaction the revival rolled back; applying such a round to the restored
 * engine would assert rolled-back progress as durable truth and can deliver an effect before
 * its cause.
 *
 * <p>The stale-round defence is layered: a deposit guard on the executor thread, and the
 * authoritative epoch check where the stream thread applies a round. The interleaving the
 * apply-time check exists for is narrower than any schedule a test can construct, so the
 * tests that pin it plant the round directly through reflection.
 */
class ProcessorRevivalTest {
    private static final UUID IN1_ID = new UUID(200, 1);
    private static final UUID IN2_ID = new UUID(200, 2);
    private static final Map<String, TopicInfo> TOPICS = Map.of(
            "in1", new TopicInfo(IN1_ID, 1),
            "in2", new TopicInfo(IN2_ID, 1));
    private static final ChannelId IN1 = new ChannelId(IN1_ID, 0);

    /** The evidence that would free the held effect: positions 0..5 of in1 are empty. */
    private static PositionFacts freeingFacts() {
        return new PositionFacts(Map.of(IN1, 6L), Map.of(), Set.of());
    }

    /** Returns empty facts immediately, except that calls claimed by {@link #arm} park. */
    static final class ControllableFacts implements FactsSource {
        static final class ParkedCall {
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            volatile PositionFacts result = PositionFacts.EMPTY;
        }

        final AtomicInteger calls = new AtomicInteger();
        private final Queue<ParkedCall> armed = new ConcurrentLinkedQueue<>();

        /** The next gather call parks until released, then returns the parked result. */
        ParkedCall arm(PositionFacts result) {
            ParkedCall parked = new ParkedCall();
            parked.result = result;
            armed.add(parked);
            return parked;
        }

        @Override
        public PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                                    Set<ChannelId> frontierChannels) throws InterruptedException {
            calls.incrementAndGet();
            ParkedCall parked = armed.poll();
            if (parked != null) {
                parked.entered.countDown();
                parked.release.await();
                return parked.result;
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
     * that round, even though it reports the exact evidence that would free the held effect —
     * whether the revival closed the processor first, as the revival path does, or
     * re-initialised it directly.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void factsGatheredBeforeRevivalNeverReachTheRevivedEngine(boolean closedBeforeReinit) throws Exception {
        feedHeldEffect();
        assertEquals(List.of(), delivered, "the effect is held: its cause on in1 is undelivered");

        ControllableFacts.ParkedCall gather = facts.arm(freeingFacts());
        punctuate(context);
        assertTrue(gather.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        MockProcessorContext<byte[], byte[]> revived = revive(closedBeforeReinit);

        gather.release.countDown();
        awaitExecutorDrained();

        punctuate(revived);
        awaitExecutorDrained();
        punctuate(revived);
        assertEquals(List.of(), delivered,
                "a round from the previous incarnation must not free the hold");
    }

    /**
     * Pins the deposit guard: a superseded gather completing after revival must leave the
     * pending-round slot empty, not merely have its round rejected later.
     */
    @Test
    void aGatherFromAPreviousIncarnationDepositsNothing() throws Exception {
        ControllableFacts.ParkedCall gather = facts.arm(freeingFacts());
        punctuate(context);
        assertTrue(gather.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        revive(true);
        gather.release.countDown();
        awaitExecutorDrained();

        assertNull(pendingRound(), "a superseded gather must not deposit its round");
    }

    /**
     * Pins the authoritative apply-time check. The deposit guard is best-effort — a deposit
     * racing re-initialisation can leave a superseded round behind — and that window is
     * narrower than any schedule a test can construct, so the round is planted directly: one
     * from a previous incarnation must be discarded, and the control round from the current
     * incarnation proves the plant would otherwise be applied.
     */
    @Test
    void aStaleRoundAlreadyDepositedIsDiscardedAtApplyTime() throws Exception {
        feedHeldEffect();
        MockProcessorContext<byte[], byte[]> revived = revive(true);

        plantRound(currentIncarnation() - 1, freeingFacts());
        CountDownLatch blocker = blockExecutor();
        punctuate(revived);
        assertEquals(List.of(), delivered, "a round from a previous incarnation must be discarded");
        blocker.countDown();
        awaitExecutorDrained();

        plantRound(currentIncarnation(), freeingFacts());
        blocker = blockExecutor();
        punctuate(revived);
        assertEquals(List.of("B"), delivered, "the control round proves the plant reaches the apply path");
        blocker.countDown();
    }

    /**
     * The same round applied by the incarnation that launched it does free the hold, so the
     * revival tests fail on any regression that lets stale rounds through.
     */
    @Test
    void theSameRoundAppliedByItsOwnIncarnationFreesTheHold() throws Exception {
        feedHeldEffect();

        ControllableFacts.ParkedCall gather = facts.arm(freeingFacts());
        punctuate(context);
        assertTrue(gather.entered.await(5, TimeUnit.SECONDS), "the gather must have started");
        gather.release.countDown();
        awaitExecutorDrained();

        CountDownLatch blocker = blockExecutor();
        punctuate(context);
        assertEquals(List.of("B"), delivered, "the report frees the hold when no revival intervened");
        blocker.countDown();
    }

    /** Re-initialisation cancels the previous punctuator instead of stacking a second one. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void revivalCancelsThePreviousIncarnationsPunctuator(boolean closedBeforeReinit) {
        MockProcessorContext<byte[], byte[]> revived = revive(closedBeforeReinit);

        assertEquals(1, context.scheduledPunctuators().size());
        assertTrue(context.scheduledPunctuators().get(0).cancelled(),
                "the punctuator scheduled before revival must be cancelled");
        assertEquals(1, revived.scheduledPunctuators().size());
        assertFalse(revived.scheduledPunctuators().get(0).cancelled());
    }

    /** A gather in flight across revival does not leave the gather slot occupied. */
    @Test
    void revivalWhileAGatherIsInFlightStillAllowsFreshGathers() throws Exception {
        ControllableFacts.ParkedCall gather = facts.arm(PositionFacts.EMPTY);
        punctuate(context);
        assertTrue(gather.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        MockProcessorContext<byte[], byte[]> revived = revive(true);
        punctuate(revived);
        gather.release.countDown();
        awaitExecutorDrained();

        // One synchronous gather per init, the parked one, and the post-revival launch. Were
        // the slot still held by the parked gather, the fourth would never have started.
        assertEquals(4, facts.calls.get());
    }

    /**
     * Pins the slot release: only the epoch that acquired the gather slot may free it. A
     * superseded gather completing while the successor's own gather holds the slot must not
     * free it, or every later punctuation would stack a further gather.
     */
    @Test
    void aSupersededGatherCannotFreeTheSlotASuccessorHolds() throws Exception {
        ControllableFacts.ParkedCall superseded = facts.arm(PositionFacts.EMPTY);
        punctuate(context);
        assertTrue(superseded.entered.await(5, TimeUnit.SECONDS), "the gather must have started");

        MockProcessorContext<byte[], byte[]> revived = revive(true);
        ControllableFacts.ParkedCall successor = facts.arm(PositionFacts.EMPTY);
        punctuate(revived);

        superseded.release.countDown();
        assertTrue(successor.entered.await(5, TimeUnit.SECONDS),
                "the successor's own gather must have started");
        punctuate(revived);
        successor.release.countDown();
        awaitExecutorDrained();

        assertEquals(4, facts.calls.get(),
                "punctuating while the successor's gather holds the slot must not launch another");
    }

    /** Holds an effect on in2 whose cause is the undelivered in1@5. */
    private void feedHeldEffect() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(IN1, 5L)))));
        context.setRecordMetadata("in2", 0, 0L);
        processor.process(new Record<>("B".getBytes(), "B".getBytes(), 0L, headers));
    }

    /**
     * Re-initialises the processor the way the revival path does — {@code close()} first,
     * or directly for lifecycles that skip it — with the same instance and the same stores.
     * The in-memory store stands for the restored committed state; here everything fed so
     * far was committed, so its content carries over unchanged. Production retains the
     * context across revival where this helper builds a fresh one; the processor treats it
     * as opaque either way.
     */
    private MockProcessorContext<byte[], byte[]> revive(boolean closedBeforeReinit) {
        if (closedBeforeReinit) {
            processor.close();
        }
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

    /**
     * Parks the facts executor behind the returned latch, so a punctuation's freshly
     * launched gather cannot deposit its round before the same punctuation applies the
     * pending one. In production a later deposit only ever replaces a pending round with a
     * fresher one — facts are monotone — but this fake's rounds are not, so the tests order
     * the schedule instead.
     */
    private CountDownLatch blockExecutor() {
        CountDownLatch release = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return release;
    }

    private Object pendingRound() throws Exception {
        return gatheredField().get();
    }

    private long currentIncarnation() throws Exception {
        Field field = ParsleyProcessor.class.getDeclaredField("incarnation");
        field.setAccessible(true);
        return ((AtomicLong) field.get(processor)).get();
    }

    private void plantRound(long epoch, PositionFacts facts) throws Exception {
        Class<?> roundClass = Class.forName(ParsleyProcessor.class.getName() + "$GatheredRound");
        Constructor<?> constructor = roundClass.getDeclaredConstructor(long.class, PositionFacts.class);
        constructor.setAccessible(true);
        gatheredField().set(constructor.newInstance(epoch, facts));
    }

    @SuppressWarnings("unchecked")
    private AtomicReference<Object> gatheredField() throws Exception {
        Field field = ParsleyProcessor.class.getDeclaredField("gathered");
        field.setAccessible(true);
        return (AtomicReference<Object>) field.get(processor);
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
