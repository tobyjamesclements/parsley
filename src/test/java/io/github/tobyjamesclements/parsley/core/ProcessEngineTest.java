package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Engine unit tests over an in-memory transactional store; end-to-end behaviour lives in the simulation tests. */
class ProcessEngineTest {

    private static final ChannelId C1 = new ChannelId(new UUID(9, 1), 0);
    private static final ChannelId C2 = new ChannelId(new UUID(9, 2), 0);
    private static final Map<ChannelId, String> BOTH = Map.of(C1, "c1", C2, "c2");

    private static ReceivedMessage plain(ChannelId channel, long position, String uid) {
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(), List.of());
    }

    private static ReceivedMessage caused(ChannelId channel, long position, String uid, Map<ChannelId, Long> causes) {
        byte[] header = CausesCodec.encode(Causes.of(causes));
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)));
    }

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

    @Test
    void frontierMergesReceiptDeliveryAndStampsEmissions() throws Exception {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 7, "B", Map.of(C1, 3L)));   // held: cause unknown; learned via receipt
        engine.onReceive(plain(C1, 2, "A"));
        engine.markDelivered(C1, 2);                             // delivering A merges (C1, 2); learned (C1, 3) wins
        Causes stamped = CausesCodec.decode(engine.causesHeaderForEmission());
        assertEquals(Causes.of(Map.of(C1, 3L)), stamped,
                "emissions must express causes learned from held metadata and delivered positions, compressed");
    }

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

    @Test
    void inExecutionFeedRegressionFailsClosedEvenBelowTheSessionFloor() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 5, "M"));
        engine.markDelivered(C1, 5);
        engine.flushHolds();
        store.commit();

        // The next execution replays the committed past in order — legitimate, dropped — but a feed that then goes
        // backwards contradicts Host obligation 1 within the execution, floor or no floor: the observed shape of a
        // recreated topic's records arriving under the old channel's identity. It must stop loudly, never drop.
        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, restarted.onReceive(plain(C1, 4, "M4")));
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onReceive(plain(C1, 2, "M2")),
                "an in-execution feed regression below the session floor must fail closed, not drop silently");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, e.reason());
    }

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

    @Test
    void recreatedFrontierChannelIsPrunedImmediately() throws Exception {
        ChannelId foreign = new ChannelId(new UUID(9, 3), 0);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 0, "M", Map.of(foreign, 7L)));
        engine.markDelivered(C1, 0);
        assertEquals(Causes.of(Map.of(C1, 0L, foreign, 7L)),
                CausesCodec.decode(engine.causesHeaderForEmission()));

        // Recreation is affirmative evidence the old topic is gone — the name denotes another id, and ids are
        // never reused — so the frontier pair prunes at once, with no dead-verdict debounce.
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(), Set.of(foreign)));
        assertEquals(Causes.of(Map.of(C1, 0L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a recreated topic's old incarnation can no longer matter (SPEC Structural 13)");
    }

    @Test
    void metadataBeyondTheBudgetFailsClosedOnReceipt() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 64);
        java.util.TreeMap<ChannelId, Long> big = new java.util.TreeMap<>();
        for (int i = 0; i < 10; i++) {
            big.put(new ChannelId(new UUID(20, i), 0), 1L); // 10 entries ≈ 285 encoded bytes, over the 64-byte budget
        }
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 0, "M", big)),
                "metadata beyond the budget must fail closed with parsley's diagnosis, not ride toward the wall");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason());
    }

    @Test
    void frontierGrowthBeyondTheBudgetFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, 80);
        // Two frontier entries encode to 5 + 2×28 = 61 bytes (within the budget); a third makes 89, over it.
        engine.onReceive(caused(C1, 0, "A", Map.of(new ChannelId(new UUID(21, 1), 0), 1L)));
        engine.markDelivered(C1, 0);
        assertTrue(engine.causesHeaderForEmission().length <= 80, "still within budget");
        // The frontier is a union across everything received, so the budget is enforced at the merge that
        // crosses it (SPEC Operational 4): a process that only receives — for which the emission-site check
        // never runs — must still stop attributably before its persisted frontier balloons (D52).
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 1, "B", Map.of(new ChannelId(new UUID(21, 2), 0), 1L))),
                "a frontier grown past the budget must fail closed before the substrate's wall");
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason());
        assertTrue(engine.frontierSize() >= 3, "the frontier size is observable (SPEC Operational 5)");
        assertTrue(engine.frontierBytes() > 80, "the encoded size is observable (SPEC Operational 5)");
    }

    @Test
    void unknownStoreFormatVersionFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(new byte[] {'v'}, new byte[] {99});
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "ordering state written by an unknown build must never be guessed at");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason());
    }

    @Test
    void corruptHeldBlobFailsClosedAtRestore() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 3, "H", Map.of(C2, 9L)));
        engine.flushHolds();
        store.commit();
        store.put(StoreCodec.heldKey(C1, 3), new byte[] {1, 2, 3}); // truncated blob
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "a held message whose persisted body cannot be decoded must stop the process, not be skipped");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, e.reason());
    }

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

    @Test
    void duplicateCausesHeadersAreUndecodable() {
        // docs/METADATA.md: more than one parsley.causes header is undecodable — it must fail closed, never resolve
        // to whichever header happens to come last.
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

    @Test
    void joiningChannelDoesNotReenterDeliveredCausalPast() {
        // SPEC Structural 16 / Safety 1 across the lifetime: a message delivered while its cause's channel was
        // outside the received set commits the process to a causal past including that cause; when the channel
        // joins, the cause must be treated as settled, not delivered after its effect.
        MemoryOrderingStore store = new MemoryOrderingStore();
        Map<ChannelId, String> onlyC2 = Map.of(C2, "c2");
        ProcessEngine first = new ProcessEngine("p", onlyC2, store);
        first.onReceive(caused(C2, 0, "B", Map.of(C1, 3L))); // dep on C1 vacuous: C1 not received
        assertTrue(first.nextDeliverable().isPresent());
        first.markDelivered(C2, 0);                          // B delivered; causal past now covers C1..3
        first.flushHolds();
        store.commit();

        ProcessEngine second = new ProcessEngine("p", BOTH, store); // C1 joins the received set
        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED,
                second.onReceive(plain(C1, 2, "A")),
                "positions at or below the delivered causal past must not be delivered after their effects");
        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, second.onReceive(plain(C1, 4, "A4")),
                "positions above the delivered causal past deliver normally");
    }

    @Test
    void recreatedTopicUnderSameNameIsRefused() {
        // SPEC Assumption 2 / Safety 8: the group's read positions are keyed by topic name; if the name now
        // resolves to a different channel, resuming would adopt a dead incarnation's positions mid-log.
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

    @Test
    void truncationCoveredByTheSameFactsBatchDoesNotFailClosed() {
        // Retention may pass over a never-yielding run the host's read position already covered; when the report
        // and the new log start arrive in one facts batch, the report must be applied first or the process would
        // fail closed spuriously — and permanently, since restarts replay the same facts.
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 5, "M"));
        engine.markDelivered(C1, 5);

        engine.onFacts(new PositionFacts(Map.of(C1, 11L), Map.of(C1, 11L), Set.of())); // covered: no throw
        assertEquals(java.util.OptionalLong.of(10), engine.fedUpTo(C1));

        assertThrows(ParsleyFailClosedException.class, () ->
                        engine.onFacts(new PositionFacts(Map.of(C1, 11L), Map.of(C1, 20L), Set.of())),
                "positions the report does not cover must still fail closed when discarded");
    }

    @Test
    void causesOfAJoinClampDroppedMessageStillBindSends() throws Exception {
        // A message dropped because a joining channel must not re-enter delivered past was still received; its
        // metadata carries causes of every subsequent send (SPEC Structural 15).
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

    @Test
    void feedOnADeadChannelFailsClosedEvenAfterRestart() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "A"));
        engine.markDelivered(C1, 0);
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1))); // C1's topic no longer exists
        engine.flushHolds();
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertThrows(ParsleyFailClosedException.class, () -> restarted.onReceive(plain(C1, 1, "ghost")),
                "a channel recorded as dead can never legitimately feed again; this must not be silently dropped");
    }

    @Test
    void unknownStoreFormatFailsClosed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(new byte[] {'v'}, new byte[] {99});
        assertThrows(ParsleyFailClosedException.class, () -> new ProcessEngine("p", BOTH, store));
    }

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

        // The dead-channel half of the rule: a cause on a channel that no longer exists is discarded too.
        ChannelId foreign2 = new ChannelId(new UUID(9, 10), 0);
        engine.onReceive(caused(C1, 1, "B", Map.of(foreign2, 2L)));
        engine.markDelivered(C1, 1);
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(foreign2)));
        assertEquals(Causes.of(Map.of(C1, 1L)), CausesCodec.decode(engine.causesHeaderForEmission()),
                "a cause on a channel that no longer exists can no longer matter");
    }

    @Test
    void joiningChannelWhoseOldMessagesAgedOutStartsCleanlyFromItsPreCommittedPosition() {
        // Structural 16 join meets Safety 8: the runtime pre-commits the joining channel's initial position, and
        // that report (earliest = current log start) must prevent a spurious discarded-positions verdict even
        // though the delivered causal past clamp sits far below the log start.
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine first = new ProcessEngine("p", Map.of(C2, "c2"), store);
        first.onReceive(caused(C2, 0, "B", Map.of(C1, 5L)));
        first.markDelivered(C2, 0); // delivered past on C1 = 5
        first.flushHolds();
        store.commit();

        ProcessEngine second = new ProcessEngine("p", BOTH, store); // C1 joins; its log now starts at 100
        second.onFacts(new PositionFacts(Map.of(C1, 100L), Map.of(C1, 100L), Set.of()));
        assertEquals(java.util.OptionalLong.of(99), second.fedUpTo(C1));
    }

    @Test
    void deadReceivedChannelWithHeldMessagesRefusesRatherThanSettling() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 2, "H", Map.of(C2, 3L)));   // held on C1: nothing known of C2 yet
        engine.flushHolds();
        store.commit();

        // C1's topic is deleted while H is still held from it. Senders that delivered from C1 may already have
        // discarded its causes from their metadata (SPEC Structural 13 permits exactly that), so H's place in
        // causal order can no longer be preserved locally: the engine must refuse, not settle and deliver past H
        // (SPEC Safety 9; ASSESSMENT 1.4).
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1))));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());

        // The refusal is a deliberate one: the hold and the verdict are both durable, so it recurs on restart.
        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        ParsleyFailClosedException again = assertThrows(ParsleyFailClosedException.class,
                () -> restarted.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1))));
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, again.reason());
    }

    @Test
    void deadReceivedChannelWithNothingHeldSettlesRemainingPositions() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C2, 4, "B", Map.of(C1, 6L)));   // B waits on C1@6; nothing held from C1 itself

        // With nothing held from it, the dead channel only settles: every remaining position will never yield a
        // message this process receives (D21), so B's dependency resolves.
        engine.onFacts(new PositionFacts(Map.of(), Map.of(), Set.of(C1)));
        DeliverableMessage b = engine.nextDeliverable().orElseThrow();
        assertEquals(C2, b.channel());
        engine.markDelivered(C2, 4);
        assertEquals(0, engine.heldCountTotal());
    }
}
