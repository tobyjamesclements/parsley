package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.TopicPartition;
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

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes what a task's initialisation does, by driving {@link ParsleyProcessor} directly
 * through the revival path.
 *
 * <p>When a commit fails retriably, or a source topic goes missing, Kafka Streams keeps the
 * task's processor objects, restores state, and re-initialises the same instance: the
 * revival path suspends the task, which closes the topology, so the processor sees
 * {@code close()} followed by {@code init}. {@code TopologyTestDriver} never does this, so
 * these tests do it by hand — in that order, and also as {@code init} without {@code close},
 * which other lifecycles may produce. Every initialisation is where the one question the
 * host asks the substrate outside delivery is asked: which of the topics the task's state
 * names still exist (D115). The answers are scripted here; the classification behind them
 * is pinned over a real broker in {@code IdentityIntegrationTest}.
 */
class ProcessorRevivalTest {
    private static final UUID IN1_ID = new UUID(200, 1);
    private static final UUID IN2_ID = new UUID(200, 2);
    private static final UUID FOREIGN_ID = new UUID(200, 9);
    private static final Map<String, TopicInfo> TOPICS = Map.of(
            "in1", new TopicInfo(IN1_ID, 1),
            "in2", new TopicInfo(IN2_ID, 1));
    private static final ChannelId IN1 = new ChannelId(IN1_ID, 0);
    private static final ChannelId IN2 = new ChannelId(IN2_ID, 0);
    private static final ChannelId FOREIGN = new ChannelId(FOREIGN_ID, 0);

    @TempDir
    Path stateDir;

    private final List<String> delivered = new ArrayList<>();
    private ScriptedTopicIdentity identity;
    private ProcessDiagnostics diagnostics;
    private KeyValueStore<Bytes, byte[]> orderingStore;
    private ParsleyProcessor processor;
    private MockProcessorContext<byte[], byte[]> context;

    @BeforeEach
    void setUp() {
        identity = new ScriptedTopicIdentity();
        diagnostics = new ProcessDiagnostics();
        processor = newProcessor(Map.of());
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
        orderingStore.close();
    }

    private ParsleyProcessor newProcessor(Map<TopicPartition, Long> startPositions) {
        return new ParsleyProcessor(twoInputRecorder(delivered), TOPICS, identity, startPositions,
                Duration.ofMillis(100), 64 * 1024, diagnostics);
    }

    /** Re-initialisation cancels the previous punctuator instead of stacking a second one. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void revivalCancelsThePreviousIncarnationsPunctuator(boolean closedBeforeReinit) {
        MockProcessorContext<byte[], byte[]> original = context;
        MockProcessorContext<byte[], byte[]> revived = revive(closedBeforeReinit);

        assertEquals(1, original.scheduledPunctuators().size(), "one punctuator per incarnation");
        assertTrue(original.scheduledPunctuators().get(0).cancelled(),
                "the punctuator scheduled before revival must be cancelled");
        assertEquals(1, revived.scheduledPunctuators().size(), "the revival schedules exactly one punctuator");
        assertFalse(revived.scheduledPunctuators().get(0).cancelled(), "the revived incarnation's punctuator runs");
    }

    /**
     * Every initialisation asks about the received topics and every topic the restored
     * frontier names — the set the engine's identity report can act on — and the
     * punctuation never asks: nothing is polled between deliveries (D115).
     */
    @Test
    void everyInitialisationAsksAboutReceivedAndFrontierTopicsAndNothingElseAsks() {
        assertEquals(List.of(Set.of(IN1_ID, IN2_ID)), identity.asked, "a fresh task names only its received topics");

        feedHeldEffect();
        feed("in1", 0L, "A", Map.of(FOREIGN, 7L));
        punctuate(context);
        punctuate(context);
        assertEquals(1, identity.asked.size(), "punctuations ask nothing of the substrate");

        revive(true);
        assertEquals(Set.of(IN1_ID, IN2_ID, FOREIGN_ID), identity.asked.get(1),
                "a revival asks about the restored frontier's topics too, so a dead one can be pruned");
    }

    /**
     * A frontier topic the identity source reports deleted leaves the frontier at revival
     * (SPEC Structural 13's one permitted discarding): the task's published frontier width
     * shrinks by that channel, and the received topics are untouched.
     */
    @Test
    void aDeletedFrontierTopicIsPrunedAtRevival() {
        feed("in1", 0L, "A", Map.of(FOREIGN, 7L));
        assertEquals(List.of("A"), delivered, "a cause on a channel this task does not receive never blocks");
        punctuate(context);
        assertEquals(2, diagnostics.snapshot().get(0).frontierChannels(),
                "staging: the frontier names in1 (delivered) and the foreign channel (received cause)");

        identity.verdicts = new TopicIdentityVerdicts(Set.of(FOREIGN_ID), Set.of());
        revive(true);
        assertEquals(1, diagnostics.snapshot().get(0).frontierChannels(),
                "the deleted topic's channel is pruned at the revival's identity report");

        identity.verdicts = TopicIdentityVerdicts.NONE;
        revive(true);
        assertEquals(1, diagnostics.snapshot().get(0).frontierChannels(),
                "the prune reached the ordering store: a later revival with nothing to report does not"
                        + " restore the dead channel's cause");
    }

    /**
     * A received topic reported deleted with nothing held from it settles to its end (D21),
     * which releases an effect waiting on it — but not inside initialisation (D34): the hold
     * goes on the next punctuation, the well-trodden host path.
     */
    @Test
    void aDeletedReceivedTopicWithNothingHeldSettlesItAndTheHoldGoesOnTheNextPunctuation() {
        feedHeldEffect();
        assertEquals(List.of(), delivered, "staging: the effect waits on in1@5");

        identity.verdicts = new TopicIdentityVerdicts(Set.of(IN1_ID), Set.of());
        MockProcessorContext<byte[], byte[]> revived = assertDoesNotThrow(() -> revive(true),
                "a deleted received topic with nothing held from it is not a refusal");
        assertEquals(List.of(), delivered, "nothing is delivered from within initialisation (D34)");
        punctuate(revived);
        assertEquals(List.of("B"), delivered, "the dead channel settled every position, so the effect goes");
    }

    /**
     * A received topic reported deleted while messages from it are held refuses the
     * revival with CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES (SPEC Safety 9, D46): the held
     * messages' place in causal order can no longer be preserved, and nothing is delivered.
     */
    @Test
    void aDeletedReceivedTopicWithHeldMessagesRefusesRevival() {
        feed("in1", 0L, "H", Map.of(IN2, 3L));
        assertEquals(List.of(), delivered, "staging: H waits on in2@3");

        identity.verdicts = new TopicIdentityVerdicts(Set.of(IN1_ID), Set.of());
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> revive(true),
                "a deleted topic with a held message from it must refuse");
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason(),
                "the refusal names the deleted channel's held messages, not a feed-order or identity reason");
        assertEquals(List.of(), delivered, "nothing may be delivered past the held message");
    }

    /**
     * A received topic reported recreated under its name refuses the revival with
     * CHANNEL_IDENTITY_CHANGED (SPEC Assumption 2): records of the new incarnation must
     * never be fed under the old identity. This is the event-driven check that replaces the
     * facts round's recreation window (D115 supersedes D44/D85).
     */
    @Test
    void aRecreatedReceivedTopicRefusesRevival() {
        identity.verdicts = new TopicIdentityVerdicts(Set.of(), Set.of(IN1_ID));
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> revive(true),
                "a recreated received topic must refuse the initialisation that learns of it");
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason(),
                "a recreated received topic is an identity change, not a deletion");
    }

    /**
     * An identity source that cannot answer — a broker outage at initialisation — is not
     * evidence about any topic: the revival proceeds on the identities resolved at start,
     * the hold stays, and the frontier keeps every cause (D44's rule, D115). The question
     * stays pending: the status punctuation asks again until it is answered — backing off,
     * since each attempt can block the stream thread for the describe's timeout, from one
     * status interval to a minute — and the answer is then applied exactly as an
     * initialisation's would be: here a deleted frontier topic is pruned, and a deleted
     * received topic with nothing held from it is settled so the hold waiting on it goes.
     */
    @Test
    void anUnansweredIdentityCheckKeepsEveryCauseAndEveryHoldAndIsAskedAgainUntilAnswered() {
        feedHeldEffect();
        feed("in1", 0L, "A", Map.of(FOREIGN, 7L));
        punctuate(context);
        int frontierBefore = diagnostics.snapshot().get(0).frontierChannels();

        identity.failure = new java.util.concurrent.TimeoutException("broker unreachable");
        MockProcessorContext<byte[], byte[]> revived = assertDoesNotThrow(() -> revive(true),
                "an unanswered identity check must not fail the task");
        int askedAtRevival = identity.asked.size();
        long clock = 1_000_000L;
        punctuate(revived, clock);
        assertEquals(List.of("A"), delivered, "the hold stays: absence of an answer settles nothing");
        assertEquals(frontierBefore, diagnostics.snapshot().get(0).frontierChannels(),
                "the frontier keeps every cause: absence of an answer prunes nothing");
        assertEquals(askedAtRevival + 1, identity.asked.size(), "the punctuation asked again while unanswered");

        // The status interval here is 100 ms: the second attempt waits one interval, the
        // third two, and a punctuation inside the wait does not ask.
        punctuate(revived, clock + 50);
        assertEquals(askedAtRevival + 1, identity.asked.size(), "inside the backoff, nothing asks");
        punctuate(revived, clock + 100);
        assertEquals(askedAtRevival + 2, identity.asked.size(), "one interval on, asked again");
        punctuate(revived, clock + 250);
        assertEquals(askedAtRevival + 2, identity.asked.size(), "the wait doubled: still inside it");

        identity.failure = null;
        identity.verdicts = new TopicIdentityVerdicts(Set.of(FOREIGN_ID, IN1_ID), Set.of());
        punctuate(revived, clock + 300);
        assertEquals(askedAtRevival + 3, identity.asked.size(), "asked once more, and answered");
        assertEquals(1, diagnostics.snapshot().get(0).frontierChannels(),
                "the answer prunes both dead channels as an initialisation's would, and only the released"
                        + " effect's own channel enters the frontier");
        assertEquals(List.of("A", "B"), delivered,
                "the answer settles the deleted received topic with nothing held from it, and the hold goes");
        punctuate(revived, clock + 100_000);
        assertEquals(askedAtRevival + 3, identity.asked.size(), "answered, nothing asks again");
    }

    /**
     * A source that answers about some ids and reports the rest unanswered — a by-name
     * describe that timed out mid-corroboration — has the answered verdicts applied and the
     * question kept pending for the rest: a timed-out corroboration is no answer, exactly
     * as a failed by-id describe is none (D115), so the next punctuation asks again.
     */
    @Test
    void aPartlyUnansweredIdentityCheckAppliesWhatWasAnsweredAndAsksAgainForTheRest() {
        feed("in1", 0L, "A", Map.of(FOREIGN, 7L));
        punctuate(context);
        assertEquals(2, diagnostics.snapshot().get(0).frontierChannels(), "staging: in1 and the foreign channel");

        identity.verdicts = new TopicIdentityVerdicts(Set.of(FOREIGN_ID), Set.of(), Set.of(IN2_ID));
        MockProcessorContext<byte[], byte[]> revived = revive(true);
        int askedAtRevival = identity.asked.size();
        assertEquals(1, diagnostics.snapshot().get(0).frontierChannels(),
                "the answered verdict is applied: the deleted frontier topic is pruned");

        identity.verdicts = TopicIdentityVerdicts.NONE;
        punctuate(revived, 5_000_000L);
        assertEquals(askedAtRevival + 1, identity.asked.size(),
                "an unanswered id keeps the question pending, so the punctuation asks again");
        punctuate(revived, 9_000_000L);
        assertEquals(askedAtRevival + 1, identity.asked.size(), "answered in full, nothing asks again");
    }

    /**
     * The start position the bootstrap established covers everything below it (SPEC Host
     * obligation 2, Structural 12): an effect whose cause lies below in1's start position is
     * deliverable without in1 ever being fed, which is what lets a process started at a
     * channel's end deliver effects of history it skipped (D115).
     */
    @Test
    void theStartPositionCoversPositionsBelowIt() {
        processor.close();
        processor = newProcessor(Map.of(new TopicPartition("in1", 0), 6L));
        MockProcessorContext<byte[], byte[]> started = newContext();
        started.addStateStore(orderingStore);
        processor.init(started);
        context = started;

        feedHeldEffect();
        assertEquals(List.of("B"), delivered, "in1@5 lies below in1's start position of 6, so the cause is satisfied");
    }

    /** Holds an effect on in2 whose cause is the undelivered in1@5. */
    private void feedHeldEffect() {
        feed("in2", 0L, "B", Map.of(IN1, 5L));
    }

    private void feed(String topic, long offset, String value, Map<ChannelId, Long> causes) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes))));
        context.setRecordMetadata(topic, 0, offset);
        processor.process(new Record<>(value.getBytes(), value.getBytes(), 0L, headers));
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
        context = revived;
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
        punctuate(ctx, 0L);
    }

    private static void punctuate(MockProcessorContext<byte[], byte[]> ctx, long wallClock) {
        List<MockProcessorContext.CapturedPunctuator> punctuators = ctx.scheduledPunctuators();
        punctuators.get(punctuators.size() - 1).getPunctuator().punctuate(wallClock);
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
