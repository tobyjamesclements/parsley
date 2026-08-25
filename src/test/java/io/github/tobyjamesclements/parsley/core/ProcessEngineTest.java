package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static io.github.tobyjamesclements.parsley.core.EngineTestFactory.plain;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    /** Holds until facts settle the cause. */
    @Test
    void holdsUntilFactsSettleTheCause() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 0, "B", Map.of(C1, 3L)));
        assertTrue(engine.nextDeliverable().isEmpty());

        engine.onFacts(new PositionFacts(Map.of(C1, 4L), Map.of(), Set.of()));
        Optional<DeliverableMessage> next = engine.nextDeliverable();
        assertTrue(next.isPresent());
        assertEquals("B", new String(next.get().value()));
        engine.markDelivered(C2, 0);
        assertEquals(0, engine.heldCountTotal());
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

    /**
     * A feed at a position a read-position report covered is not a feed-order violation —
     * in-execution order is checked separately — and carries its own reason: the report and
     * the feed contradict each other, which is either a false report or this execution
     * observing a successor's committed progress after being superseded.
     */
    @Test
    void feedAtAReportCoveredPositionFailsClosedAsCoveredPositionFed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 2, "A"));
        engine.markDelivered(C1, 2);
        engine.onFacts(new PositionFacts(Map.of(C1, 10L), Map.of(), Set.of()));

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(plain(C1, 7, "M")),
                "a feed contradicting a read-position report must fail closed");
        assertEquals(ParsleyFailClosedException.Reason.COVERED_POSITION_FED, e.reason(),
                "the condition is a report/feed contradiction, not a host feed-order breach");
    }

    /** Recreated received channel fact fails closed. */
    @Test
    void recreatedReceivedChannelFactFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "M"));
        engine.markDelivered(C1, 0);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1), Set.of(C1))),
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

        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(), Set.of(foreign)));
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

    /** Frontier growth beyond the budget fails closed. */
    @Test
    void frontierGrowthBeyondTheBudgetFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 80);

        engine.onReceive(caused(C1, 0, "A", Map.of(new ChannelId(new UUID(21, 1), 0), 1L)));
        engine.markDelivered(C1, 0);
        assertTrue(engine.causesHeaderForEmission().length <= 80, "still within budget");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 1, "B", Map.of(new ChannelId(new UUID(21, 2), 0), 1L))),
                "a frontier grown past the budget must fail closed before the substrate's wall");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason());
        assertTrue(engine.frontierSize() >= 3, "the frontier size is observable (SPEC Operational 5)");
        assertTrue(engine.frontierBytes() > 80, "the encoded size is observable (SPEC Operational 5)");
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

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(1, restarted.heldCount(C2));
        restarted.onFacts(new PositionFacts(Map.of(C1, 9L), Map.of(), Set.of()));
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

    /** Truncation covered by the same facts batch does not fail closed. */
    @Test
    void truncationCoveredByTheSameFactsBatchDoesNotFailClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 5, "M"));
        engine.markDelivered(C1, 5);

        engine.onFacts(new PositionFacts(Map.of(C1, 11L), Map.of(C1, 11L), Set.of()));
        assertEquals(java.util.OptionalLong.of(10), engine.fedUpTo(C1));

        assertThrows(ParsleyFailClosedException.class, () ->
                        engine.onFacts(new PositionFacts(Map.of(C1, 11L), Map.of(C1, 20L), Set.of())),
                "positions the report does not cover must still fail closed when discarded");
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
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1)));
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

    /** Facts prune causes below log start and on dead channels. */
    @Test
    void factsPruneCausesBelowLogStartAndOnDeadChannels() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        ChannelId foreign = new ChannelId(new UUID(9, 9), 0);
        engine.onReceive(caused(C1, 0, "A", Map.of(foreign, 4L)));
        engine.markDelivered(C1, 0);
        assertEquals(Causes.of(Map.of(foreign, 4L, C1, 0L)),
                CausesCodec.decode(engine.causesHeaderForEmission()));

        engine.onFacts(new PositionFacts(Map.of(), Map.of(foreign, 5L), Set.of()));
        assertEquals(Causes.of(Map.of(C1, 0L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a cause below its channel's earliest retained position can no longer matter");

        ChannelId foreign2 = new ChannelId(new UUID(9, 10), 0);
        engine.onReceive(caused(C1, 1, "B", Map.of(foreign2, 2L)));
        engine.markDelivered(C1, 1);
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(foreign2)));
        assertEquals(Causes.of(Map.of(C1, 1L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a cause on a channel that no longer exists can no longer matter");
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

        ProcessEngine second = new ProcessEngine("p", BOTH, store);
        second.onFacts(new PositionFacts(Map.of(C1, 100L), Map.of(C1, 100L), Set.of()));
        assertEquals(java.util.OptionalLong.of(99), second.fedUpTo(C1));
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
                () -> engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1))));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        ParsleyFailClosedException again = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1))));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, again.reason());
    }

    /** Dead received channel with nothing held settles remaining positions. */
    @Test
    void deadReceivedChannelWithNothingHeldSettlesRemainingPositions() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 4, "B", Map.of(C1, 6L)));

        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1)));
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

        // Neither the restore path nor the facts round checks the budget, so both must pass
        // here and the stop below is attributable to the emission check alone.
        ProcessEngine constricted = new ProcessEngine("p", BOTH, store, 64);
        constricted.onFacts(PositionFacts.EMPTY);
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
     * frontier can never itself exceed the budget — the encoded size is affine in the entry
     * count — so this test's oversized header is the canonical encoding of exactly the
     * frontier's channels at exactly their frontier positions, padded past the budget: the
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
}
