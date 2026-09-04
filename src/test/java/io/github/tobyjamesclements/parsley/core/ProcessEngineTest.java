package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static io.github.tobyjamesclements.parsley.core.EngineTestFactory.plain;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes engine behaviour directly, without a host.
 *
 * <p>Covers holding and release, frontier growth through receipt and delivery, restart with
 * held messages restored, and each condition that stops a process.
 */
class ProcessEngineTest {
    private static final ChannelId C1 = new ChannelId(new UUID(9, 1), 0);
    private static final ChannelId C2 = new ChannelId(new UUID(9, 2), 0);
    private static final Map<ChannelId, String> BOTH = Map.of(C1, "c1", C2, "c2");

    private static ReceivedMessage caused(ChannelId channel, long position, String uid, Map<ChannelId, Long> causes) {
        byte[] header = CausesCodec.encode(Causes.of(causes));
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)));
    }

    /**
     * A cause is satisfied by receiving and delivering the message it names, and by nothing
     * else (D115): the effect waits while the cause's position is unreceived, waits while
     * the cause is received but undelivered, and goes once the cause has been delivered.
     */
    @Test
    void holdsUntilTheCauseItNamesIsDelivered() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 0, "B", Map.of(C1, 3L)));
        assertTrue(engine.nextDeliverable().isEmpty(), "c1@3 has not been received");

        engine.onReceive(plain(C1, 3, "A"));
        Deliverability.Verdict effectWhileCauseHeld = engine.headVerdict(C2).orElseThrow();
        assertTrue(effectWhileCauseHeld instanceof Deliverability.Held,
                "received but undelivered, the cause still holds the effect: " + effectWhileCauseHeld);
        Deliverability.Blocker blocker = ((Deliverability.Held) effectWhileCauseHeld).blockers().get(0);
        assertEquals(C1, blocker.channel());
        assertEquals(3L, blocker.requiredPosition());
        assertEquals(java.util.OptionalLong.of(2), blocker.settledPosition(),
                "the settled view stops below the held head, so c1@3 is not yet settled");
        DeliverableMessage a = engine.nextDeliverable().orElseThrow();
        assertEquals(C1, a.channel(), "the cause is offered first: the effect still waits behind the held cause");
        engine.markDelivered(C1, 3);
        Optional<DeliverableMessage> next = engine.nextDeliverable();
        assertTrue(next.isPresent(), "delivering c1@3 satisfies the cause");
        assertEquals("B", new String(next.get().value()));
        engine.markDelivered(C2, 0);
        assertEquals(0, engine.heldCountTotal());
    }

    /**
     * A cause naming a position no record occupies — an aborted transaction's slot, a
     * marker — is settled by the receipt of the next record on the channel and by nothing
     * before it (SPEC Liveness 3 as D115 restates it): a record below the named position
     * leaves the effect held with the gap still open, and the record after it, once
     * delivered, releases the effect without the named position ever being fed.
     */
    @Test
    void aGapBelowAReceivedRecordIsSettledByThatReceiptAndNothingBeforeIt() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 0, "B", Map.of(C1, 2L)));
        engine.onReceive(plain(C1, 1, "A1"));
        engine.markDelivered(C1, 1);
        Deliverability.Verdict verdict = engine.headVerdict(C2).orElseThrow();
        assertTrue(verdict instanceof Deliverability.Held, "c1@1 settles nothing at or above 2: " + verdict);
        assertEquals(java.util.OptionalLong.of(1),
                ((Deliverability.Held) verdict).blockers().get(0).settledPosition());
        assertTrue(engine.nextDeliverable().isEmpty(), "nothing but a record at or past c1@2 releases the effect");

        engine.onReceive(plain(C1, 3, "A3"));
        engine.markDelivered(C1, 3);
        Optional<DeliverableMessage> next = engine.nextDeliverable();
        assertTrue(next.isPresent(), "receipt of c1@3 settles 2 as never yielding a message");
        assertEquals("B", new String(next.get().value()));
    }

    /**
     * The host's start position raises coverage to just below it (SPEC Host obligation 2,
     * Structural 12): a cause below the start position is satisfied without the message
     * ever being received, a feed below it is a replay dropped rather than a contradiction
     * refused, coverage the store already holds is never lowered, and a start position of
     * zero, or one for a channel not received, changes nothing (D115).
     */
    @Test
    void aStartPositionCoversEverythingBelowItWithinTheSessionFloor() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine started = new ProcessEngine("p", BOTH, store, ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES,
                Map.of(C1, 5L, C2, 0L, new ChannelId(new UUID(9, 3), 0), 9L));
        assertEquals(java.util.OptionalLong.of(4), started.fedUpTo(C1), "coverage sits just below the start");
        assertTrue(started.fedUpTo(C2).isEmpty(), "a start position of zero covers nothing");
        assertEquals(Causes.none(), started.frontierSnapshot(), "a start position is coverage, not a cause");
        started.onReceive(caused(C2, 0, "B", Map.of(C1, 4L)));
        assertTrue(started.nextDeliverable().isPresent(), "a cause below the start position is satisfied");
        started.markDelivered(C2, 0);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, started.onReceive(plain(C1, 2, "old")),
                "a feed below the start position is the host re-feeding a committed past: a replay, dropped");
        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, started.onReceive(plain(C1, 5, "A5")),
                "the start position itself is the first position fed");
        started.markDelivered(C1, 5);
        started.flushHolds();
        store.commit();

        ProcessEngine lower = new ProcessEngine("p", BOTH, store, ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES,
                Map.of(C1, 3L));
        assertEquals(java.util.OptionalLong.of(5), lower.fedUpTo(C1),
                "a start position below the store's coverage never lowers it");
    }

    /** Frontier merges receipt delivery and stamps emissions. */
    @Test
    void frontierMergesReceiptDeliveryAndStampsEmissions() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 7, "B", Map.of(C1, 3L)));
        engine.onReceive(plain(C1, 2, "A"));
        engine.markDelivered(C1, 2);
        Causes stamped = CausesCodec.decode(engine.causesHeaderForEmission());
        assertEquals(Causes.of(Map.of(C1, 3L)), stamped,
                "emissions must express causes learned from held metadata and delivered positions, compressed");
    }

    /** Refeed below session floor is dropped and within session fails. */
    @Test
    void refeedBelowSessionFloorIsDroppedAndWithinSessionFails() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 5, "M"));
        engine.markDelivered(C1, 5);
        assertThrows(ParsleyFailClosedException.class, () -> engine.onReceive(plain(C1, 5, "M")),
                "within one execution a covered position must never be fed again");

        engine.flushHolds();
        store.commit();
        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, restarted.onReceive(plain(C1, 5, "M")),
                "across executions a re-fed committed position is a duplicate, silently dropped");
    }

    /** In execution feed regression fails closed even below the session floor. */
    @Test
    void inExecutionFeedRegressionFailsClosedEvenBelowTheSessionFloor() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 5, "M"));
        engine.markDelivered(C1, 5);
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, restarted.onReceive(plain(C1, 4, "M4")));
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onReceive(plain(C1, 2, "M2")),
                "an in-execution feed regression below the session floor must fail closed, not drop silently");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, e.reason());
    }

    /** A received channel reported recreated at initialisation fails closed. */
    @Test
    void recreatedReceivedChannelReportFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "M"));
        engine.markDelivered(C1, 0);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onIdentityReport(new IdentityReport(Set.of(), Set.of(C1))),
                "a received topic recreated under its name means the feed path can no longer be trusted");
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason());
    }

    /** Recreated frontier channel is pruned immediately. */
    @Test
    void recreatedFrontierChannelIsPrunedImmediately() throws Exception {
        ChannelId foreign = new ChannelId(new UUID(9, 3), 0);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 0, "M", Map.of(foreign, 7L)));
        engine.markDelivered(C1, 0);
        assertEquals(Causes.of(Map.of(C1, 0L, foreign, 7L)),
                CausesCodec.decode(engine.causesHeaderForEmission()));

        engine.onIdentityReport(new IdentityReport(Set.of(), Set.of(foreign)));
        assertEquals(Causes.of(Map.of(C1, 0L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a recreated topic's old incarnation can no longer matter (SPEC Structural 13)");
    }

    /** Metadata beyond the budget fails closed on receipt. */
    @Test
    void metadataBeyondTheBudgetFailsClosedOnReceipt() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 64);
        java.util.TreeMap<ChannelId, Long> big = new java.util.TreeMap<>();
        for (int i = 0; i < 10; i++) {
            big.put(new ChannelId(new UUID(20, i), 0), 1L);
        }
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 0, "M", big)),
                "metadata beyond the budget must fail closed with parsley's diagnosis, not ride toward the wall");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason());
    }

    /**
     * The maintained frontier size agrees with the encoded header, byte for byte, across
     * the shapes that exercise every term of the arithmetic. The agreement is load-bearing:
     * the merge-site budget gate reads the counter where the emission gate measures real
     * bytes, and drift between them would let one refuse what the other allows (D98). Each
     * leg exists because a mutation trial showed its absence stays green: growth, a
     * mid-group prune and restore catch a counter update deleted at any of the three
     * mutation sites; the position-raising re-merge catches an unconditional add
     * double-counting; the 130-partition group catches a dropped partition-count
     * varint-width delta at the 127→128 boundary; the climb to 128 distinct topics catches
     * a hardcoded topic-count width; and the topic-emptying prune at that boundary catches
     * an emptied topic lingering in the per-topic counts.
     */
    @Test
    void frontierBytesAgreesWithTheEncodedHeader() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(), "empty frontier");

        java.util.TreeMap<ChannelId, Long> causes = new java.util.TreeMap<>();
        for (int partition = 0; partition < 3; partition++) {
            causes.put(new ChannelId(new UUID(40, 1), partition), 5L);
        }
        causes.put(new ChannelId(new UUID(40, 2), 300), 9L);
        engine.onReceive(caused(C1, 0, "A", causes));
        engine.markDelivered(C1, 0);
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "after growth through receipt and delivery");

        engine.onReceive(caused(C1, 1, "B", Map.of(new ChannelId(new UUID(40, 1), 0), 50L)));
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "after a position-raising re-merge of a tracked channel, which must not re-count it");

        engine.onIdentityReport(new IdentityReport(Set.of(new ChannelId(new UUID(40, 1), 1)), Set.of()));
        assertEquals(4, engine.frontierSize(), "staging: the dead channel must actually leave the frontier");
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "after pruning a mid-group partition");

        // One topic wide enough to push its partition count from one varint byte to two,
        // and enough distinct topics to do the same to the topic count: 3 in the frontier
        // already, plus this group and 124 singles makes exactly 128.
        java.util.TreeMap<ChannelId, Long> wide = new java.util.TreeMap<>();
        for (int partition = 0; partition < 130; partition++) {
            wide.put(new ChannelId(new UUID(41, 1), partition), 1L);
        }
        for (int topic = 1; topic <= 124; topic++) {
            wide.put(new ChannelId(new UUID(42, topic), 0), 1L);
        }
        engine.onReceive(caused(C2, 0, "C", wide));
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "with a 130-partition group and 128 distinct topics, both count varints two bytes wide");

        engine.onIdentityReport(new IdentityReport(Set.of(new ChannelId(new UUID(40, 2), 300)), Set.of()));
        assertEquals(257, engine.frontierSize(),
                "staging: the emptied topic's only channel must actually leave the frontier");
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "after a topic-emptying prune back across the topic-count width boundary");

        engine.flushHolds();
        store.commit();
        ProcessEngine restored = new ProcessEngine("p", BOTH, store);
        assertEquals(restored.causesHeaderForEmission().length, restored.frontierBytes(), "after restore");
    }

    /** Frontier growth beyond the budget fails closed. */
    @Test
    void frontierGrowthBeyondTheBudgetFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 70);

        engine.onReceive(caused(C1, 0, "A", Map.of(new ChannelId(new UUID(21, 1), 0), 1L)));
        engine.markDelivered(C1, 0);
        assertTrue(engine.causesHeaderForEmission().length <= 70, "still within budget");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 1, "B", Map.of(new ChannelId(new UUID(21, 2), 0), 1L))),
                "a frontier grown past the budget must fail closed before the substrate's wall");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason());
        assertTrue(engine.frontierSize() >= 3, "the frontier size is observable (SPEC Operational 5)");
        assertTrue(engine.frontierBytes() > 70, "the encoded size is observable (SPEC Operational 5)");
    }

    /** Unknown store format version fails closed. */
    @Test
    void unknownStoreFormatVersionFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(new byte[] {'v'}, new byte[] {99});
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "ordering state written by an unknown build must never be guessed at");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason());
    }

    /** Corrupt held blob fails closed at restore. */
    @Test
    void corruptHeldBlobFailsClosedAtRestore() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 3, "H", Map.of(C2, 9L)));
        engine.flushHolds();
        store.commit();
        store.put(StoreCodec.heldKey(C1, 3), new byte[] {1, 2, 3});
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "a held message whose persisted body cannot be decoded must stop the process, not be skipped");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason());
    }

    /** Held messages survive restart with bodies intact. */
    @Test
    void heldMessagesSurviveRestartWithBodiesIntact() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 1, "held", Map.of(C1, 8L)));
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store, ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES,
                Map.of(C1, 9L));
        assertEquals(1, restarted.heldCount(C2));
        DeliverableMessage message = restarted.nextDeliverable().orElseThrow();
        assertEquals("held", new String(message.value()));
        assertEquals("held", new String(message.key()));
    }

    /** Duplicate causes headers are undecodable. */
    @Test
    void duplicateCausesHeadersAreUndecodable() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        byte[] header = CausesCodec.encode(Causes.none());
        ReceivedMessage twoHeaders = new ReceivedMessage(C1, 0, 0, null, "v".getBytes(), List.of(
                new HeaderKV(CausesCodec.HEADER_KEY, header),
                new HeaderKV("app.other", new byte[] {1}),
                new HeaderKV(CausesCodec.HEADER_KEY, header)));
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> engine.onReceive(twoHeaders));
        assertEquals(ParsleyFailClosedException.Reason.UNDECODABLE_METADATA, e.reason());
        assertEquals(0, engine.heldCountTotal(), "an undecodable message is never accepted");
    }

    /** Joining channel does not reenter delivered causal past. */
    @Test
    void joiningChannelDoesNotReenterDeliveredCausalPast() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        Map<ChannelId, String> onlyC2 = Map.of(C2, "c2");
        ProcessEngine first = new ProcessEngine("p", onlyC2, store);
        first.onReceive(caused(C2, 0, "B", Map.of(C1, 3L)));
        assertTrue(first.nextDeliverable().isPresent());
        first.markDelivered(C2, 0);
        first.flushHolds();
        store.commit();

        ProcessEngine second = new ProcessEngine("p", BOTH, store);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED,
                second.onReceive(plain(C1, 2, "A")),
                "positions at or below the delivered causal past must not be delivered after their effects");
        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, second.onReceive(plain(C1, 4, "A4")),
                "positions above the delivered causal past deliver normally");
    }

    /** Recreated topic under same name is refused. */
    @Test
    void recreatedTopicUnderSameNameIsRefused() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine first = new ProcessEngine("p", Map.of(C1, "orders"), store);
        first.onReceive(plain(C1, 0, "M"));
        first.markDelivered(C1, 0);
        first.flushHolds();
        store.commit();

        ChannelId recreated = new ChannelId(new UUID(9, 99), 0);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(recreated, "orders"), store));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason());
    }

    /** Causes of a join clamp dropped message still bind sends. */
    @Test
    void causesOfAJoinClampDroppedMessageStillBindSends() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        Map<ChannelId, String> onlyC2 = Map.of(C2, "c2");
        ProcessEngine first = new ProcessEngine("p", onlyC2, store);
        first.onReceive(caused(C2, 0, "B", Map.of(C1, 3L)));
        first.markDelivered(C2, 0);
        first.flushHolds();
        store.commit();

        ChannelId elsewhere = new ChannelId(new UUID(9, 77), 0);
        ProcessEngine second = new ProcessEngine("p", BOTH, store);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED,
                second.onReceive(caused(C1, 2, "A", Map.of(elsewhere, 9L))));
        Causes stamped = CausesCodec.decode(second.causesHeaderForEmission());
        assertEquals(9L, stamped.byChannel().get(elsewhere),
                "causes learned from a dropped-but-received message must be expressed on sends");
    }

    /**
     * Feed on a dead channel fails closed even after restart — and with the diagnosis D77
     * assigned it: the dead-channel re-feed keeps {@code OUT_OF_ORDER_FEED} (D77 carved
     * {@code COVERED_POSITION_FED} out for report/feed contradictions and left feed-order
     * breaches, this one included, behind), and the message names the channel as recorded no
     * longer existing (D21's end-of-channel sentinel) rather than accusing a generic order
     * breach. Catches the reason being swapped or the dead-channel diagnosis being garbled,
     * which the previous type-only assertThrows stayed green through.
     */
    @Test
    void feedOnADeadChannelFailsClosedEvenAfterRestart() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "A"));
        engine.markDelivered(C1, 0);
        engine.onIdentityReport(new IdentityReport(Set.of(C1), Set.of()));
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onReceive(plain(C1, 1, "ghost")),
                "a channel recorded as dead can never legitimately feed again; this must not be silently dropped");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, e.reason(),
                "the dead-channel re-feed is a feed-order breach, the half D77 left with OUT_OF_ORDER_FEED");
        assertTrue(e.getMessage().contains("recorded as no longer existing"),
                "the diagnosis names the dead-channel condition, not a generic order breach, got: " + e.getMessage());
    }

    /** Unknown store format fails closed. */
    @Test
    void unknownStoreFormatFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(new byte[] {'v'}, new byte[] {99});
        assertThrows(ParsleyFailClosedException.class, () -> new ProcessEngine("p", BOTH, store));
    }

    /**
     * The one discarding Structural 13 permits after D115: a cause on a channel whose topic
     * no longer exists leaves the frontier at the identity report, and a cause on a live
     * channel is kept whatever the report says about others — there is no retention input
     * left to prune by.
     */
    @Test
    void identityReportPrunesCausesOnDeadChannelsAndNothingElse() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        ChannelId foreign = new ChannelId(new UUID(9, 9), 0);
        ChannelId foreign2 = new ChannelId(new UUID(9, 10), 0);
        engine.onReceive(caused(C1, 0, "A", Map.of(foreign, 4L, foreign2, 2L)));
        engine.markDelivered(C1, 0);
        assertEquals(Causes.of(Map.of(foreign, 4L, foreign2, 2L, C1, 0L)),
                CausesCodec.decode(engine.causesHeaderForEmission()));

        engine.onIdentityReport(new IdentityReport(Set.of(foreign2), Set.of()));
        assertEquals(Causes.of(Map.of(foreign, 4L, C1, 0L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a cause on a channel that no longer exists can no longer matter; every other cause stays");

        engine.onIdentityReport(IdentityReport.NONE);
        assertEquals(Causes.of(Map.of(foreign, 4L, C1, 0L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a report naming nothing prunes nothing");

        engine.flushHolds();
        store.commit();
        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(Causes.of(Map.of(foreign, 4L, C1, 0L)), CausesCodec.decode(restarted.causesHeaderForEmission()),
                "the prune reached the store: a restart does not restore the dead channel's cause");
    }

    /** Joining channel whose old messages aged out starts cleanly from its pre committed position. */
    @Test
    void joiningChannelWhoseOldMessagesAgedOutStartsCleanlyFromItsPreCommittedPosition() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine first = new ProcessEngine("p", Map.of(C2, "c2"), store);
        first.onReceive(caused(C2, 0, "B", Map.of(C1, 5L)));
        first.markDelivered(C2, 0);
        first.flushHolds();
        store.commit();

        ProcessEngine second = new ProcessEngine("p", BOTH, store, ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES,
                Map.of(C1, 100L));
        assertEquals(java.util.OptionalLong.of(99), second.fedUpTo(C1),
                "the joined channel's coverage is its start position less one, above the stale clamp");
    }

    /** Dead received channel with held messages refuses rather than settling. */
    @Test
    void deadReceivedChannelWithHeldMessagesRefusesRatherThanSettling() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 2, "H", Map.of(C2, 3L)));
        engine.flushHolds();
        store.commit();

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onIdentityReport(new IdentityReport(Set.of(C1), Set.of())));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        ParsleyFailClosedException again = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onIdentityReport(new IdentityReport(Set.of(C1), Set.of())));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, again.reason());
    }

    /** Dead received channel with nothing held settles remaining positions. */
    @Test
    void deadReceivedChannelWithNothingHeldSettlesRemainingPositions() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 4, "B", Map.of(C1, 6L)));

        engine.onIdentityReport(new IdentityReport(Set.of(C1), Set.of()));
        DeliverableMessage b = engine.nextDeliverable().orElseThrow();
        assertEquals(C2, b.channel());
        engine.markDelivered(C2, 4);
        assertEquals(0, engine.heldCountTotal());
    }

    /**
     * A restored frontier naming the reserved zero topic id is untrustworthy state: it can
     * only have entered through a forged causes header absorbed before wire-format
     * constraint 5 refused it at receipt, and no substrate query can ever answer for it,
     * so restoring it would re-express and re-persist the ghost on every emission forever
     * (D88). Stored state that cannot be trusted is a reason to stop.
     */
    @Test
    void restoredFrontierNamingTheZeroTopicIdFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        new ProcessEngine("p", BOTH, store);
        store.put(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, new ChannelId(new UUID(0, 0), 0)),
                StoreCodec.encodeLong(7));

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "state carrying the reserved zero id must refuse, not resume and re-express it");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason(),
                "the refusal names the untrusted-state condition");
    }

    /**
     * Catches the undeclared-channel guard vanishing from {@code onReceive}: without it a
     * message from a channel outside the declared received set is absorbed silently — its
     * causes merge into the frontier and its body enters the hold-back buffer — instead of
     * being refused before anything is taken. The refusal is a breach of how the host drives
     * the engine (SPEC Host obligation 1: the feed is the declared channels, in order), not a
     * protocol fail-closed stop, so what is pinned is its {@code IllegalArgumentException}
     * type, its naming of the condition, and that the refused message left no residue in
     * frontier, holds or coverage.
     */
    @Test
    void receiptOnAnUndeclaredChannelIsRefusedAndAbsorbsNothing() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", Map.of(C1, "c1"), store);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> engine.onReceive(caused(C2, 0, "M", Map.of(C1, 7L))),
                "a message on a channel this process never declared must refuse loudly, not be absorbed");
        assertTrue(e.getMessage().contains("received on undeclared channel"),
                "the refusal names the undeclared-channel condition, got: " + e.getMessage());
        assertEquals(0, engine.heldCountTotal(),
                "a refused undeclared-channel message must not enter the hold-back buffer");
        assertEquals(Causes.none(), engine.frontierSnapshot(),
                "the refused message's causes must not leak into the frontier");
        assertTrue(engine.fedUpTo(C2).isEmpty(),
                "a refused undeclared-channel message must not advance coverage for its channel");
    }

    /**
     * Catches the head guard vanishing from {@code markDelivered} in its empty shape: a
     * channel holding nothing — never fed, or already drained by the delivery just recorded —
     * has no head, and without the guard the equality probe dereferences a null head (an
     * undiagnosed NullPointerException) instead of the {@code IllegalStateException} naming
     * the hold-back-buffer head contract (the buffer and its head rule: D5). The
     * double-delivery of one position is the classic host regression the second leg refuses.
     */
    @Test
    void markDeliveredWithNothingHeldOnTheChannelRefusesAsNotTheHead() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "A"));

        IllegalStateException neverFed = assertThrows(IllegalStateException.class,
                () -> engine.markDelivered(C2, 0),
                "markDelivered on a channel holding nothing has no head to match and must refuse");
        assertTrue(neverFed.getMessage().contains("is not the head of the hold-back buffer"),
                "the refusal names the hold-back head contract, got: " + neverFed.getMessage());

        engine.onReceive(plain(C2, 0, "B"));
        engine.markDelivered(C2, 0);
        IllegalStateException redelivered = assertThrows(IllegalStateException.class,
                () -> engine.markDelivered(C2, 0),
                "a second markDelivered of the same position finds the buffer empty and must refuse, not repeat");
        assertTrue(redelivered.getMessage().contains("is not the head of the hold-back buffer"),
                "the double-delivery refusal names the same contract, got: " + redelivered.getMessage());

        assertEquals(1, engine.heldCount(C1), "the refusals must not disturb another channel's holds");
    }

    /**
     * Catches the head guard vanishing from {@code markDelivered} in its silent shape: with
     * the buffer non-empty, a non-head position falls through to the {@code removeIf}
     * mid-buffer removal — delivery recorded for a message that never passed the head rule,
     * the frontier advanced past its unblocked predecessors, per-channel order (SPEC Safety 3)
     * broken with no exception anywhere. D67 recorded the adjacent equivalent-mutant shape at
     * the old L482; this pins the non-equivalent one: the guard must refuse and leave the
     * buffered holds and the frontier exactly as they were.
     */
    @Test
    void markDeliveredAtANonHeadPositionRefusesAndLeavesHoldsAndFrontierUntouched() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "A0"));
        engine.onReceive(plain(C1, 1, "A1"));
        engine.onReceive(plain(C1, 2, "A2"));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> engine.markDelivered(C1, 1),
                "markDelivered at a held but non-head position must refuse, never remove mid-buffer");
        assertTrue(e.getMessage().contains("is not the head of the hold-back buffer"),
                "the refusal names the hold-back head contract, got: " + e.getMessage());
        assertEquals(3, engine.heldCount(C1),
                "a refused non-head delivery must leave every buffered hold in place");
        assertEquals(Causes.none(), engine.frontierSnapshot(),
                "a refused non-head delivery must not advance the frontier");
        DeliverableMessage head = engine.nextDeliverable().orElseThrow(
                () -> new AssertionError("the untouched buffer must still offer its true head"));
        assertEquals(0, head.position(), "the head of the hold-back buffer is still position 0");
    }

    /**
     * Catches the emission budget check in {@code causesHeaderForEmission} being deleted. The
     * only route to it is a frontier restored by the constructor, which deliberately carries
     * no budget check of its own: receipt (the per-message gate) and merge (the growth gate)
     * both police the budget as the frontier grows, so a frontier can stand beyond the budget
     * only by being restored from state committed under a larger one. Without the emission
     * check that restored frontier is encoded and handed back oversized, riding toward the
     * broker's record-size wall with no parsley diagnosis — exactly what D52's third
     * enforcement point exists to stop.
     */
    @Test
    void aFrontierRestoredPastAShrunkenBudgetFailsClosedAtEmissionNotAtRestore() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine generous = new ProcessEngine("p", BOTH, store);
        java.util.TreeMap<ChannelId, Long> wide = new java.util.TreeMap<>();
        for (int i = 0; i < 10; i++) {
            wide.put(new ChannelId(new UUID(30, i + 1), 0), 1L);
        }
        generous.onReceive(caused(C1, 0, "M", wide));
        generous.markDelivered(C1, 0);
        generous.flushHolds();
        store.commit();

        // Neither the restore path nor the identity report checks the budget, so both must
        // pass here and the stop below is attributable to the emission check alone.
        ProcessEngine constricted = new ProcessEngine("p", BOTH, store, 64);
        constricted.onIdentityReport(IdentityReport.NONE);
        assertEquals(11, constricted.frontierSize(), "staging: the wide frontier was restored intact");
        assertTrue(constricted.frontierBytes() > 64,
                "staging: the restored frontier already exceeds the shrunken budget");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                constricted::causesHeaderForEmission,
                "expressing a frontier beyond the budget must fail closed, not hand back an oversized header");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason(),
                "the stop carries the budget diagnosis (D52)");
        assertTrue(e.getMessage().contains("expressing the causal frontier"),
                "the diagnosis names the emission site, not the receipt or merge gates, got: " + e.getMessage());
    }

    /**
     * Discriminates the per-message header-length gate from the merged-frontier growth gate,
     * which share {@code METADATA_BUDGET_EXCEEDED}:
     * {@code #metadataBeyondTheBudgetFailsClosedOnReceipt} uses fresh channels, so deleting
     * the per-message gate lets the growth gate fire with the same reason and that test stays
     * green. A canonically-encoded header whose channels already sit in a within-budget
     * frontier can never itself exceed the budget — encoded size is monotone under
     * subsetting: dropping a pair drops its bytes, and dropping a group's last pair drops
     * its topic header too — so this test's oversized header is the canonical encoding of
     * exactly the frontier's channels at exactly their frontier positions, padded past the
     * budget: the
     * growth gate cannot fire (nothing merges, and even a merge would leave the frontier's
     * size unchanged and within budget), and only the per-message gate, which judges raw
     * length before any decode, can produce the budget diagnosis (D52's receipt enforcement
     * point). Deleting that gate alone turns this refusal into UNDECODABLE_METADATA and this
     * test red while the fresh-channel test stays green.
     */
    @Test
    void anOversizedHeaderNamingOnlyFrontierChannelsIsRefusedByThePerMessageGate() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 100);
        ChannelId f1 = new ChannelId(new UUID(31, 1), 0);
        ChannelId f2 = new ChannelId(new UUID(31, 2), 0);
        engine.onReceive(caused(C1, 0, "A", Map.of(f1, 5L, f2, 9L)));
        assertEquals(Causes.of(Map.of(f1, 5L, f2, 9L)), engine.frontierSnapshot(),
                "staging: both channels sit in the frontier, and the frontier is within budget");

        byte[] oversized = java.util.Arrays.copyOf(
                CausesCodec.encode(Causes.of(Map.of(f1, 5L, f2, 9L))), 101);
        ReceivedMessage message = new ReceivedMessage(C1, 1, 1, "B".getBytes(), "B".getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, oversized)));

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(message),
                "a header beyond the budget must be refused on raw length before any decode");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason(),
                "an oversized header's diagnosis is the budget, not undecodability (D52)");
        assertTrue(e.getMessage().contains("carries 101 bytes of causal metadata"),
                "the diagnosis states the per-message gate's own measurement, got: " + e.getMessage());
        assertEquals(1, engine.heldCountTotal(), "only the staging message may be held");
        assertEquals(Causes.of(Map.of(f1, 5L, f2, 9L)), engine.frontierSnapshot(),
                "the refused message must not have advanced the frontier");
    }

    /**
     * Only the head of each channel's hold-back buffer keeps its decoded form once flushed:
     * the delivery decision reads the head and nothing else, so everything behind it lives
     * in the store until it reaches the head (D102). A restart keeps nothing decoded until
     * a head is first offered to the decision.
     */
    @Test
    void onlyTheHeadOfEachBufferKeepsItsDecodedFormOnceFlushed() {
        ChannelId c3 = new ChannelId(new UUID(9, 3), 0);
        Map<ChannelId, String> three = Map.of(C1, "c1", C2, "c2", c3, "c3");
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", three, store);
        for (int i = 0; i < 5; i++) {
            engine.onReceive(caused(C2, i, "b" + i, Map.of(c3, 100L)));
        }
        for (int i = 0; i < 3; i++) {
            engine.onReceive(caused(C1, i, "a" + i, Map.of(c3, 200L)));
        }
        assertEquals(8, engine.decodedHoldCount(), "an unflushed hold is still in memory");

        engine.flushHolds();
        assertEquals(2, engine.decodedHoldCount(),
                "after a flush only the head of each buffer keeps its decoded form");

        store.commit();
        ProcessEngine restarted = new ProcessEngine("p", three, store);
        assertEquals(0, restarted.decodedHoldCount(), "a restart restores skeletons, not decoded messages");
        assertTrue(restarted.nextDeliverable().isEmpty(), "both heads are blocked");
        assertEquals(2, restarted.decodedHoldCount(),
                "offering the heads to the decision decodes exactly the heads");

        restarted.onReceive(plain(c3, 100, "settles-c2"));
        assertEquals(c3, restarted.nextDeliverable().orElseThrow().channel(), "the c3 record delivers first");
        restarted.markDelivered(c3, 100);
        Optional<DeliverableMessage> next = restarted.nextDeliverable();
        assertTrue(next.isPresent(), "C2's head is released once c3 settles past its cause");
        assertEquals(C2, next.get().channel(), "the head of C2 must be offered next");
        restarted.markDelivered(C2, 0);
        assertTrue(restarted.nextDeliverable().isPresent(), "the next hold on C2 becomes the head");
        assertEquals(2, restarted.decodedHoldCount(),
                "a hold decodes on reaching the head, and the delivered one is gone");
    }

    /**
     * A hold reloaded from the store — after a flush and a restart — reaches logic with the
     * key, value, headers and causes it arrived with, and its causes still enter the
     * delivered causal past, which a later-joining channel is clamped above (D31, D102).
     */
    @Test
    void aHoldReloadedFromTheStoreReachesLogicWithItsCausesIntact() {
        ChannelId foreign = new ChannelId(new UUID(9, 3), 0);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        ReceivedMessage received = caused(C2, 4, "B", Map.of(C1, 3L, foreign, 9L));
        engine.onReceive(received);
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store, ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES,
                Map.of(C1, 4L));
        Optional<DeliverableMessage> next = restarted.nextDeliverable();
        assertTrue(next.isPresent(), "the cause lies below the start position, so the reloaded hold is deliverable");
        DeliverableMessage message = next.get();
        assertArrayEquals(received.key(), message.key(), "key reloaded byte for byte");
        assertArrayEquals(received.value(), message.value(), "value reloaded byte for byte");
        assertEquals(received.headers().size(), message.headers().size(), "headers reloaded");
        assertArrayEquals(received.headers().get(0).value(), message.headers().get(0).value(),
                "the causes header reloaded byte for byte");
        assertEquals(Causes.of(Map.of(C1, 3L, foreign, 9L)), message.causes(), "causes reloaded");
        restarted.markDelivered(C2, 4);
        restarted.flushHolds();
        store.commit();

        ProcessEngine joined = new ProcessEngine("p", Map.of(C1, "c1", C2, "c2", foreign, "f"), store);
        assertEquals(9L, joined.fedUpTo(foreign).orElseThrow(),
                "the reloaded causes entered the delivered past, which clamps the joining channel");
    }

    /**
     * A hold delivered in the step that received it is never written to the store: it left
     * its buffer before the flush, and the flush must skip it rather than resurrect it as a
     * held message a restart would deliver again (D102).
     */
    @Test
    void aHoldDeliveredBeforeItsFirstFlushIsNeverWrittenToTheStore() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 2, "A"));
        engine.markDelivered(C1, 2);
        engine.flushHolds();
        java.util.List<Long> heldPositions = new java.util.ArrayList<>();
        store.scanPrefix(StoreCodec.heldPrefix(C1), (key, value) -> heldPositions.add(StoreCodec.positionOfHeldKey(key)));
        assertEquals(java.util.List.of(), heldPositions, "a delivered hold must not be persisted by a later flush");
        store.commit();
        assertEquals(0, new ProcessEngine("p", BOTH, store).heldCountTotal(), "nothing is restored as held");
    }

    /**
     * A hold whose store entry has vanished refuses rather than delivering an empty message
     * or skipping it: the buffer and the store contradict each other, which is ordering
     * state that cannot be trusted (D102).
     */
    @Test
    void aHeldMessageMissingFromTheStoreRefusesRatherThanDeliveringNothing() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 0, "B", Map.of(C1, 3L)));
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        store.delete(StoreCodec.heldKey(C2, 0));
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, restarted::nextDeliverable,
                "a hold absent from the store must stop the process");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason(),
                "a hold whose blob is absent from the store is refused as an unknown state format");
        assertTrue(e.getMessage().contains("absent from the store"), e.getMessage());
    }

    /**
     * The encoded frontier is computed once per change and handed out as a copy: two
     * emissions in one step share the encoding, an emission's bytes are the caller's to
     * alter, and every frontier mutation site — merge on receipt, merge on delivery, prune
     * on an identity report — produces a fresh encoding (D102).
     */
    @Test
    void theEmissionHeaderIsReusedUntilTheFrontierChangesAndHandedOutAsACopy() throws Exception {
        ChannelId foreign = new ChannelId(new UUID(9, 3), 0);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 7, "B", Map.of(C1, 3L, foreign, 5L)));
        byte[] first = engine.causesHeaderForEmission();
        byte[] second = engine.causesHeaderForEmission();
        assertArrayEquals(first, second, "an unchanged frontier encodes identically");
        assertNotSame(first, second, "each emission receives its own array");
        first[first.length - 1] ^= 0x7F;
        assertArrayEquals(second, engine.causesHeaderForEmission(), "altering a handed-out copy leaves the engine's encoding intact");

        engine.onReceive(caused(C2, 8, "C", Map.of(foreign, 6L)));
        assertEquals(Causes.of(Map.of(C1, 3L, foreign, 6L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a receipt that raises a cause re-encodes");

        engine.onReceive(plain(C1, 3, "A"));
        engine.markDelivered(C1, 3);
        assertEquals(Causes.of(Map.of(C1, 3L, foreign, 6L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a delivery at the position already expressed changes nothing");
        engine.onReceive(plain(C1, 4, "A2"));
        engine.markDelivered(C1, 4);
        assertEquals(Causes.of(Map.of(C1, 4L, foreign, 6L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a delivery past the expressed position re-encodes");

        engine.onIdentityReport(new IdentityReport(Set.of(foreign), Set.of()));
        assertEquals(Causes.of(Map.of(C1, 4L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a prune re-encodes without the pruned channel");
        assertEquals(engine.frontierBytes(), engine.causesHeaderForEmission().length,
                "the incremental width agrees with the cached encoding after every mutation");
    }

    /**
     * Flushing after every receipt stays cheap while the buffer deepens: a flush writes the
     * holds taken in since the previous flush and never scans the buffer (D102). This is
     * the suite's one wall-clock bound, chosen with a wide margin: 50,000 receipts each
     * followed by a flush complete in well under a second here, where a flush that scanned
     * every hold would spend on the order of twenty seconds in the scan alone.
     */
    @Test
    void flushingAfterEveryReceiptStaysCheapWhileTheBufferDeepens() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        byte[] header = CausesCodec.encode(Causes.of(Map.of(C1, 1_000_000L)));
        List<HeaderKV> headers = List.of(new HeaderKV(CausesCodec.HEADER_KEY, header));
        long started = System.nanoTime();
        for (int i = 0; i < 50_000; i++) {
            engine.onReceive(new ReceivedMessage(C2, i, i, null, null, headers));
            engine.flushHolds();
        }
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertEquals(50_000, engine.heldCount(C2), "every receipt is held behind the unsatisfied cause");
        assertTrue(elapsedMillis < 5_000,
                "50,000 receipt-and-flush cycles took " + elapsedMillis + " ms; a flush must not scan the buffer");
    }

    /**
     * The in-execution feed-order check binds at equality, not only on regression. The same
     * position fed twice within one execution is the shape a recreated topic's records take
     * when they arrive under the old channel's identity, and it must fail loudly as a
     * feed-order breach on both sides of the session floor: above the floor a check weakened
     * to strict-less-than would fall through to the covered-position branch and misname the
     * condition (SPEC Operational 6); below the floor it would drop the second feed silently
     * as a sanctioned replay (D67 gap 3).
     */
    @Test
    void feedingTheSamePositionTwiceInOneExecutionFailsClosedAsOutOfOrderOnBothSidesOfTheSessionFloor() {
        MemoryOrderingStore fresh = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, fresh);
        engine.onReceive(plain(C1, 5, "M"));
        ParsleyFailClosedException above = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(plain(C1, 5, "M")),
                "the same position fed twice above the session floor must fail closed");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, above.reason(),
                "a same-position re-feed is a feed-order breach, not a report/feed contradiction");

        MemoryOrderingStore committed = new MemoryOrderingStore();
        ProcessEngine first = new ProcessEngine("p", BOTH, committed);
        first.onReceive(plain(C1, 5, "M"));
        first.markDelivered(C1, 5);
        first.flushHolds();
        committed.commit();
        ProcessEngine restarted = new ProcessEngine("p", BOTH, committed);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, restarted.onReceive(plain(C1, 3, "M3")),
                "staging: the first re-feed below the session floor is a sanctioned replay");
        ParsleyFailClosedException below = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onReceive(plain(C1, 3, "M3")),
                "the same position fed twice below the session floor must fail closed, not drop silently");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, below.reason(),
                "the second feed of one position is a feed-order breach whatever the session floor says");
    }

    /**
     * A delivered-past entry pruned with its dead channel must not resurface as a join clamp.
     * Were it kept, a channel of that identity joining the received set would have its
     * fed-up-to advanced to the stale past, leaving a coverage record for a channel this
     * process never read, and the bootstrap would resume the channel from it (D115).
     * Coverage is the fed-up-to record alone, and a joined-never-read channel must leave
     * none.
     */
    @Test
    void aPrunedDeliveredPastEntryDoesNotBecomeCoverageWhenItsChannelJoins() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine first = new ProcessEngine("p", Map.of(C1, "c1"), store);
        first.onReceive(caused(C1, 0, "A", Map.of(C2, 5L)));
        first.markDelivered(C1, 0);
        first.onIdentityReport(new IdentityReport(Set.of(C2), Set.of()));
        first.flushHolds();
        store.commit();

        ProcessEngine joined = new ProcessEngine("p", BOTH, store);
        assertEquals(java.util.OptionalLong.empty(), joined.fedUpTo(C2),
                "a channel whose delivered past was pruned must join with no clamp");
        Map<byte[], byte[]> image = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        store.scanPrefix(new byte[0], image::put);
        assertEquals(Map.of(C1, 0L), OrderingStateInspector.coveredPositions(image),
                "the joined channel must leave no fed-up-to record: the bootstrap would otherwise resume"
                        + " it from coverage this process never had");
    }

    /**
     * The incrementally maintained frontier width must track the encoded header when a prune
     * shrinks a topic group back across the varint-width boundary: removing partitions from a
     * group of 129 through 128 to 127 narrows the partition-count varint from two bytes to
     * one, and {@link #frontierBytesAgreesWithTheEncodedHeader} only ever crosses that
     * boundary upwards. A delta taken from the wrong side of the count leaves frontierBytes
     * one byte off the header the process emits, and the O(1) budget gate then refuses one
     * byte early or late, a drift that compounds with every crossing.
     */
    @Test
    void frontierBytesTracksTheEncodedHeaderWhenAPruneShrinksAGroupAcrossTheVarintWidthBoundary() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        UUID wideTopic = new UUID(43, 1);
        java.util.TreeMap<ChannelId, Long> wide = new java.util.TreeMap<>();
        for (int partition = 0; partition < 129; partition++) {
            wide.put(new ChannelId(wideTopic, partition), 1L);
        }
        engine.onReceive(caused(C1, 0, "A", wide));
        assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                "with a 129-partition group, the partition count two varint bytes wide");

        for (int remaining = 128; remaining >= 126; remaining--) {
            engine.onIdentityReport(new IdentityReport(Set.of(new ChannelId(wideTopic, remaining)), Set.of()));
            assertEquals(remaining, engine.frontierSize(),
                    "staging: the pruned partition must actually leave the frontier");
            assertEquals(engine.causesHeaderForEmission().length, engine.frontierBytes(),
                    "after shrinking the group to " + remaining + " partitions");
        }
    }
}
