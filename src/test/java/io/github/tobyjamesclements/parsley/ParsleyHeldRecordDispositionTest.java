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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.cause;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.message;

/**
 * Tests the init-time disposition of restored held records whose source topic left scope. A
 * rescope explicitly supports input removal and recreation, but a held record from a departed
 * source can be neither delivered (a removed topic has no registered serde) nor silently dropped
 * (fail-closed), so the L2 constructor classifies every restored entry: a current input's record
 * restores unchanged; a recreated input's old-incarnation record is purged (the incarnation
 * is deleted, its history can never be delivered anywhere); a removed-but-alive input's records
 * fail init loudly with the redeclare-or-reset remedies. Before this rule, the removed-input case
 * was an unrecoverable crash loop: the restore pass proved the record deliverable, and the fetch
 * died on the missing serde with an untyped exception, every restart, forever.
 *
 * <p>Driven as two {@link ParsleyProcessor} lifetimes over the same physical stores (the
 * {@link MockProcessorContext} + {@link TestKeyValueStore} restart harness of
 * {@link ParsleyProcessorRestoreTest}): a v1 consuming {@code c1} and {@code c2} buffers a
 * {@code c2} record held on an unsatisfied {@code c1} dependency, then a v2 with a different
 * input set restores the stores.
 */
class ParsleyHeldRecordDispositionTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();

    private final TestKeyValueStore<String, byte[]> frontierStore =
            new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
    private final TestKeyValueStore<Long, byte[]> bufferStore =
            new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
    private final TestKeyValueStore<byte[], byte[]> candidateIndexStore =
            new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
    private final TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
            new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

    private final List<String> processed = new ArrayList<>();

    /**
     * A held record whose source input was removed from the declared set fails init loudly, naming
     * the topic, the per-topic count, and both remedies (redeclare the input, or full reset) —
     * replacing the pre-disposition behaviour, where init succeeded and the drain then died
     * fetching the record through the missing serde ("no ParsleySource registered"), an untyped
     * crash loop only a full reset could clear.
     *
     * Asserts init throws the actionable {@link IllegalStateException}, not the old serde failure.
     */
    @Test
    void aHeldRecordFromARemovedInputFailsInitWithTheRemedies() {
        runV1AndHoldAMidRecord();

        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c1"), TestTopicAdmin.of(Map.of("c1", C1_ID)));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> v2.init(contextOverStores()),
                "init must fail loudly on a held record from a removed input — it can be neither "
                        + "delivered nor silently discarded");
        assertTrue(message(thrown).contains("c2=1"),
                "the failure must name the removed topic and its held-record count: " + message(thrown));
        assertTrue(message(thrown).contains("redeclare") && message(thrown).contains("reset"),
                "the failure must state both remedies: " + message(thrown));
        assertFalse(message(thrown).contains("no ParsleySource registered"),
                "the failure must be the disposition's actionable message, not the old untyped "
                        + "serde crash: " + message(thrown));
    }

    /**
     * A held record from a recreated input's old incarnation (same topic name, new UUID) is purged
     * at init and the task starts cleanly: the incarnation that produced it is deleted, so no
     * receiver can ever deliver it — delivering it here would forward a record from a dead
     * incarnation and re-enter its destroyed coordinate into the frontier rescope just purged.
     * Recreation across a restart stays self-healing: history loss, never reordering.
     *
     * Asserts init proceeds, the buffer is empty, and the first post-restart stamp carries no
     * claim under the destroyed UUID.
     */
    @Test
    void aHeldRecordFromARecreatedInputIsPurgedAndInitProceeds() {
        runV1AndHoldAMidRecord();

        Uuid recreated = Uuid.randomUuid();
        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c1", "c2"), TestTopicAdmin.of(Map.of("c1", C1_ID, "c2", recreated)));
        MockProcessorContext<String, String> context = contextOverStores();
        v2.init(context);

        assertEquals(0, bufferStore.approximateNumEntries(),
                "the destroyed incarnation's held record must be purged from the buffer at init");

        context.setRecordMetadata("c1", 0, 0);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, ParsleyVectorClock.empty().toBytes());
        v2.process(new Record<>("k", "c1-value", 0L, headers));

        assertEquals(List.of("c1-value"), processed,
                "the recreated-input task must process normally after the purge");
        ParsleyVectorClock stamp = firstForwardedStamp(context);
        assertEquals(-1L, stamp.offsetFor(C2_ID, 0),
                "the destroyed incarnation's coordinate must never re-enter the stamp — purged "
                        + "records are deleted history, not restorable state");
    }

    /**
     * A held record whose source is still a declared input restores exactly as before the
     * disposition rule existed (the pin): it stays buffered across the restart and releases
     * through the ordinary cascade once its dependency is delivered.
     *
     * Asserts the record survives v2's init and the delegate sees it after its cause arrives.
     */
    @Test
    void aHeldRecordStillInScopeRestoresAndReleasesNormally() {
        runV1AndHoldAMidRecord();

        ParsleyProcessor<String, String, String, String> v2 =
                processor(Set.of("c1", "c2"), TestTopicAdmin.of(Map.of("c1", C1_ID, "c2", C2_ID)));
        MockProcessorContext<String, String> context = contextOverStores();
        v2.init(context);

        assertEquals(1, bufferStore.approximateNumEntries(),
                "an in-scope held record must survive the restart in the buffer");

        // Deliver the held record's cause: c1@0. The frontier advance must cascade-release it.
        context.setRecordMetadata("c1", 0, 0);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, ParsleyVectorClock.empty().toBytes());
        v2.process(new Record<>("k", "c1-value", 0L, headers));

        assertEquals(List.of("c1-value", "held-value"), processed,
                "delivering the cause must release the restored held record to the delegate");
        assertEquals(0, bufferStore.approximateNumEntries(),
                "the released record must leave the buffer");
    }

    // --- harness -----------------------------------------------------------------------------------

    /**
     * Runs a v1 lifetime consuming {@code c1} and {@code c2}: one {@code c2} record with an
     * unsatisfied dependency on {@code c1@0} is processed and must be buffered, persisting a held
     * record into the shared stores for the v2 lifetime to restore.
     */
    private void runV1AndHoldAMidRecord() {
        ParsleyProcessor<String, String, String, String> v1 =
                processor(Set.of("c1", "c2"), TestTopicAdmin.of(Map.of("c1", C1_ID, "c2", C2_ID)));
        MockProcessorContext<String, String> context = contextOverStores();
        v1.init(context);

        context.setRecordMetadata("c2", 0, 0);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK,
                ParsleyVectorClock.empty().observe(C1_ID, 0, 0).toBytes());
        v1.process(new Record<>("k", "held-value", 0L, headers));

        assertEquals(1, bufferStore.approximateNumEntries(),
                "v1 must have buffered the c2 record on its unsatisfied c1 dependency");
        assertTrue(processed.isEmpty(), "the held record must not have reached the delegate in v1");
        processed.clear();
    }

    private ParsleyProcessor<String, String, String, String> processor(
            Set<String> topics, ParsleyTopicAdmin admin) {
        Processor<String, String, String, String> delegate = new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                processed.add(record.value());
                ctx.forward(record);
            }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        return new ParsleyProcessor<>(delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                topics, Set.of(), List.of(),
                configs -> admin, null);
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

    /** The causal-clock stamp of the first business forward captured by {@code context}. */
    private static ParsleyVectorClock firstForwardedStamp(MockProcessorContext<String, String> context) {
        List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertFalse(forwarded.isEmpty(), "the delegate's forward must have been captured");
        Header stamped = forwarded.get(0).record().headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        assertNotNull(stamped, "every business forward must carry a causal-clock stamp");
        return ParsleyVectorClock.fromBytes(stamped.value());
    }
}
