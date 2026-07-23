package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the init-time former-sink heal of the restored {@code ownOutputs} clock (T3.4). The
 * persisted {@code "f"} blob always trails the final transaction's own-output acks (store caches
 * flush before the producer flush completes acks — O1), and the ordinary end-offset seed heals
 * only the <em>currently</em> declared sinks — so a redeploy that turns a sink into an input, or
 * drops it while a third party still consumes it, would otherwise restart with stamps
 * under-claiming this node's own final-transaction outputs: an I2 under-claim, which a downstream
 * consumer of that topic plus another of this node's sinks could reorder around.
 *
 * <p>Driven as two {@link ParsleyProcessor} lifetimes over the same physical stores (the
 * {@link MockProcessorContext} + {@link TestKeyValueStore} restart harness of
 * {@link ParsleyProcessorRestoreTest}): a v1 whose sink is {@code c3}, then a v2 with a different
 * shape, asserting on the causal-dependencies header of v2's first stamped forward — the exact
 * artifact the under-claim would corrupt.
 */
class ParsleyFormerSinkHealTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid C3_ID = Uuid.randomUuid();
    private static final Uuid C4_ID = Uuid.randomUuid();

    private final TestKeyValueStore<String, byte[]> frontierStore =
            new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
    private final TestKeyValueStore<Long, byte[]> bufferStore =
            new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
    private final TestKeyValueStore<byte[], byte[]> candidateIndexStore =
            new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
    private final TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
            new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

    /**
     * The main scenario: v1 produced to {@code c3} (its blob's own-output claim trails at the
     * end-offset-seeded 5); v2 consumes {@code c3} instead of producing to it, and by then the
     * topic holds records up to offset 8 — v1's final-transaction outputs, whose acks never made
     * the blob. The heal must re-cover them from the topic's end offsets, so v2's very first
     * stamp claims (c3, 0, 8), not the stale 5 — without the heal a downstream consumer of
     * {@code c3} and v2's {@code c4} could deliver the derived output before the offset-8 cause.
     *
     * Asserts the first stamped forward claims the former sink at its last appended offset (the
     * growth-seeded frontier keeping the carried pre-heal cut is pinned separately, in
     * {@link ParsleyChannelsTest}).
     */
    @Test
    void aFormerSinkTurnedInputIsHealedToItsEndOffsetsBeforeTheFirstStamp() {
        runV1WithMidAsSink(TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 6L))));

        // v2: c3 is now an input, c4 is the sink; c3's log has grown to end offset 9 (records
        // up to 8 — v1's final transaction landed there after the blob's last persist).
        TestTopicAdmin v2Admin = TestTopicAdmin.of(Map.of("c2", C2_ID, "c3", C3_ID, "c4", C4_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 9L)));
        MockProcessorContext<String, String> context = contextOverStores();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c2", "c3"), Set.of("c4"), configs -> v2Admin);
        v2.init(context);

        ParsleyVectorClock stamp = firstStampOf(v2, context);
        assertEquals(8L, stamp.offsetFor(C3_ID, 0),
                "the first post-redeploy stamp must claim the former sink at its last appended "
                        + "offset — the end-offset heal must cover the acks the blob's final "
                        + "transaction never persisted (I2; without the heal this reads the stale 5)");
    }

    /**
     * A former sink that was deleted and recreated across the redeploy: the recorded UUID's
     * records are destroyed, so its claims heal nothing and gate nothing — the heal must purge
     * them from the stamp rather than claim coordinates no receiver can ever deliver.
     *
     * Asserts v2's first stamp carries no claim under the old UUID.
     */
    @Test
    void aRecreatedFormerSinksClaimsArePurgedNotHealed() {
        runV1WithMidAsSink(TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 6L))));

        Uuid recreated = Uuid.randomUuid();
        TestTopicAdmin v2Admin = TestTopicAdmin.of(Map.of("c2", C2_ID, "c3", recreated, "c4", C4_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 3L)));
        MockProcessorContext<String, String> context = contextOverStores();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c2", "c3"), Set.of("c4"), configs -> v2Admin);
        v2.init(context);

        ParsleyVectorClock stamp = firstStampOf(v2, context);
        assertEquals(-1L, stamp.offsetFor(C3_ID, 0),
                "a recreated former sink's old-UUID claims are provably destroyed (E1) and must "
                        + "leave the stamp, not be healed to the new topic's offsets");
    }

    /**
     * A former sink deleted outright (no successor, not consumed): positive evidence of
     * destruction — {@code UnknownTopicOrPartitionException} on resolution — purges its claims,
     * and init proceeds.
     *
     * Asserts init succeeds and the first stamp carries no claim for the deleted sink.
     */
    @Test
    void aDeletedFormerSinksClaimsArePurgedAndInitProceeds() {
        runV1WithMidAsSink(TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 6L))));

        TestTopicAdmin resolving = TestTopicAdmin.of(Map.of("c2", C2_ID, "c4", C4_ID));
        ParsleyTopicAdmin v2Admin = failingFor(resolving, "c3",
                new ExecutionException(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("gone")));
        MockProcessorContext<String, String> context = contextOverStores();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c2"), Set.of("c4"), configs -> v2Admin);
        v2.init(context);

        ParsleyVectorClock stamp = firstStampOf(v2, context);
        assertEquals(-1L, stamp.offsetFor(C3_ID, 0),
                "a deleted former sink's claims must be purged — they can gate nothing and heal nothing");
    }

    /**
     * A transient resolution failure for a former sink must fail init loudly: proceeding would
     * stamp potential under-claims, and purging without proof of destruction would manufacture
     * them — only positive evidence may purge, and only a successful resolution may heal.
     */
    @Test
    void aTransientFormerSinkResolutionFailureFailsInitLoudly() {
        runV1WithMidAsSink(TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 6L))));

        TestTopicAdmin resolving = TestTopicAdmin.of(Map.of("c2", C2_ID, "c4", C4_ID));
        ParsleyTopicAdmin v2Admin = failingFor(resolving, "c3",
                new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("broker away")));
        MockProcessorContext<String, String> context = contextOverStores();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c2"), Set.of("c4"), configs -> v2Admin);

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> v2.init(context),
                "an unresolvable former sink must fail init — neither healing nor purging is provable");
        assertTrue(String.valueOf(failure.getMessage()).contains("c3")
                        || String.valueOf(failure.getCause()).contains("c3"),
                "the failure must name the unresolvable former sink: " + failure);
    }

    /**
     * The common case — the sink set is unchanged across the restart — must not open the heal's
     * admin session at all: the current-sink end-offset seed already covers the trailing acks.
     * Guarded here by an admin factory that fails the test if init calls it a second time.
     */
    @Test
    void anUnchangedSinkSetOpensNoHealSession() {
        TestTopicAdmin admin = TestTopicAdmin.of(Map.of("c1", C1_ID, "c3", C3_ID))
                .withEndOffsets(Map.of("c3", Map.of(0, 6L)));
        runV1WithMidAsSink(admin);

        int[] factoryCalls = {0};
        MockProcessorContext<String, String> context = contextOverStores();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c1"), Set.of("c3"), configs -> {
                    factoryCalls[0]++;
                    return admin;
                });
        v2.init(context);

        assertEquals(1, factoryCalls[0],
                "an unchanged sink set must resolve through the single init session — the former-sink "
                        + "heal must not open a second admin session when there is nothing to heal");
    }

    // --- harness -----------------------------------------------------------------------------------

    /** Runs a v1 processor lifetime (source c1, sink c3) against the shared stores: init only. */
    private void runV1WithMidAsSink(ParsleyTopicAdmin admin) {
        ParsleyProcessor<String, String, String, String> v1 =
                processor(Set.of("c1"), Set.of("c3"), configs -> admin);
        v1.init(contextOverStores());
        assertNotNull(frontierStore.get(ParsleyStores.FRONTIER_KEY),
                "v1's init must have persisted the \"f\" blob the v2 lifetime restores from");
    }

    private ParsleyProcessor<String, String, String, String> processor(
            Set<String> topics, Set<String> sinkTopics,
            java.util.function.Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory) {
        Processor<String, String, String, String> delegate = new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) { ctx.forward(record); }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        return new ParsleyProcessor<>(delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                topics, sinkTopics, List.of(),
                adminFactory, ParsleyConfig.from(new Properties()), null);
    }

    private MockProcessorContext<String, String> contextOverStores() {
        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        return context;
    }

    /**
     * Feeds one causally minimal {@code c2} record and returns the causal-dependencies clock of
     * the delegate's stamped forward — v2's first outbound stamp.
     */
    private ParsleyVectorClock firstStampOf(
            ParsleyProcessor<String, String, String, String> processor,
            MockProcessorContext<String, String> context) {
        context.setRecordMetadata("c2", 0, 0);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, ParsleyVectorClock.empty().toBytes());
        processor.process(new Record<>("k", "v", 0L, headers));

        List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertFalse(forwarded.isEmpty(), "the delegate's forward must have been captured");
        Header stamped = forwarded.get(0).record().headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        assertNotNull(stamped, "every business forward must carry a causal-dependencies stamp");
        return ParsleyVectorClock.fromBytes(stamped.value());
    }

    /** A delegating admin double that fails {@code topicIds} for one specific topic. */
    private static ParsleyTopicAdmin failingFor(TestTopicAdmin delegate, String failingTopic, Exception failure) {
        return new ParsleyTopicAdmin() {
            @Override public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                if (topics.contains(failingTopic)) {
                    throw failure;
                }
                return delegate.topicIds(topics);
            }
            @Override public Map<String, Integer> partitionCounts(List<String> topics) {
                return delegate.partitionCounts(topics);
            }
            @Override public Map<String, String> cleanupPolicies(List<String> topics) {
                return delegate.cleanupPolicies(topics);
            }
            @Override public Map<Integer, Long> endOffsets(String topic) throws Exception {
                return delegate.endOffsets(topic);
            }
            @Override public void close() {}
        };
    }
}
