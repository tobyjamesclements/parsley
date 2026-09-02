package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static io.github.tobyjamesclements.parsley.core.EngineTestFactory.plain;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boundary pins over the pure core from a line-by-line review (D106): the equal-position
 * re-feed of a held message, holds spanning flush boundaries, the {@code nextRead = 0}
 * coverage floor, the inclusive budget boundary, byte-exact emission headers after every
 * frontier mutation kind, and the reserved maximum position refused before it can reach
 * the fed-to-end sentinel (D105).
 */
class EngineBoundaryTest {
    private static final ChannelId C1 = new ChannelId(new UUID(9, 1), 0);
    private static final ChannelId C2 = new ChannelId(new UUID(9, 2), 0);
    private static final Map<ChannelId, String> BOTH = Map.of(C1, "c1", C2, "c2");

    private static ReceivedMessage caused(ChannelId channel, long position, String uid, Map<ChannelId, Long> causes) {
        byte[] header = CausesCodec.encode(Causes.of(causes));
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)));
    }

    /**
     * D67 gap 3: the {@code position <= fedBefore} boundary in {@code onReceive}. Feeding the
     * same position twice within one execution while the first copy is still held (not
     * delivered) must refuse as a feed-order breach. Weakened to {@code <}, the second feed
     * falls through to the covered-position branch and refuses with
     * {@code COVERED_POSITION_FED} instead, blaming a read-position report that never
     * existed (SPEC Operational 6); the previous type-only assertThrows could not see that.
     */
    @Test
    void equalPositionRefeedOfAHeldNotDeliveredMessageFailsClosedAsOutOfOrderFeed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(caused(C1, 5, "H", Map.of(C2, 3L)));
        assertTrue(engine.nextDeliverable().isEmpty(), "staging: the message is held, not delivered");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(caused(C1, 5, "H", Map.of(C2, 3L))),
                "the same position fed twice in one execution must fail closed");
        assertEquals(ParsleyFailClosedException.Reason.OUT_OF_ORDER_FEED, e.reason(),
                "an equal-position re-feed is a feed-order breach, not a report/feed contradiction");
        assertTrue(e.getMessage().contains("already fed position 5"),
                "the diagnosis names the in-execution feed, got: " + e.getMessage());
        assertEquals(1, engine.heldCount(C1), "the refused re-feed must not enter the buffer");
    }

    /**
     * Holds accumulate across several flushes before a restart: a flush persists exactly the
     * holds received since the previous flush, a hold delivered before any flush never
     * touches the store (D17), and a restart restores every survivor in position order with
     * its body intact. This is the invariant any flush that stops scanning the whole buffer
     * must keep: unpersisted holds are a suffix of each channel's buffer.
     */
    @Test
    void holdsSpanningFlushBoundariesArePersistedOnceAndRestoredInOrderWithBodiesIntact() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onReceive(plain(C1, 0, "delivered-before-flush"));
        engine.markDelivered(C1, 0);
        for (long position = 0; position < 3; position++) {
            engine.onReceive(caused(C2, position, "h" + position, Map.of(C1, 100L)));
        }
        engine.flushHolds();
        assertNull(store.get(StoreCodec.heldKey(C1, 0)),
                "a message delivered in the step it arrived must never be written as a hold (D17)");
        for (long position = 0; position < 3; position++) {
            assertTrue(store.get(StoreCodec.heldKey(C2, position)) != null, "flushed hold " + position);
        }
        for (long position = 3; position < 5; position++) {
            engine.onReceive(caused(C2, position, "h" + position, Map.of(C1, 100L)));
        }
        assertNull(store.get(StoreCodec.heldKey(C2, 3)), "not yet flushed");
        engine.flushHolds();
        assertTrue(store.get(StoreCodec.heldKey(C2, 4)) != null, "second flush persists the new suffix");
        store.commit();

        ProcessEngine restarted = new ProcessEngine("p", BOTH, store);
        assertEquals(5, restarted.heldCount(C2), "every flushed hold is restored");
        restarted.onFacts(new PositionFacts(Map.of(C1, 101L), Map.of(), Set.of()));
        for (long position = 0; position < 5; position++) {
            DeliverableMessage next = restarted.nextDeliverable().orElseThrow(
                    () -> new AssertionError("restored hold must become deliverable"));
            assertEquals(position, next.position(), "restored holds deliver in position order");
            assertEquals("h" + position, new String(next.value()), "restored body intact");
            assertEquals(Causes.of(Map.of(C1, 100L)), next.causes(), "restored causes intact");
            restarted.markDelivered(C2, position);
        }
        assertEquals(0, restarted.heldCountTotal(), "every restored hold must have been delivered");
    }

    /**
     * A read-position report of {@code nextRead = 0} covers nothing: coverage is {@code -1},
     * a cause at position 0 still blocks, and the record at position 0 is accepted rather than
     * dropped as covered. A sign slip at this boundary would either release a cause at 0
     * unsatisfied or drop the channel's first record.
     */
    @Test
    void committedNextReadOfZeroCoversNothingAndBlocksACauseAtPositionZero() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        engine.onFacts(new PositionFacts(Map.of(C1, 0L), Map.of(C1, 0L), Set.of()));
        assertEquals(OptionalLong.of(-1), engine.fedUpTo(C1), "nextRead 0 covers nothing");

        engine.onReceive(caused(C2, 0, "B", Map.of(C1, 0L)));
        assertTrue(engine.nextDeliverable().isEmpty(), "a cause at position 0 is not covered by -1");

        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, engine.onReceive(plain(C1, 0, "A")),
                "position 0 must not be dropped as covered by a -1 coverage");
        DeliverableMessage a = engine.nextDeliverable().orElseThrow();
        assertEquals(C1, a.channel(), "the sole deliverable must be the held message on C1");
        engine.markDelivered(C1, 0);
        DeliverableMessage b = engine.nextDeliverable().orElseThrow();
        assertEquals(C2, b.channel(), "the effect delivers once position 0 is delivered");
    }

    /**
     * Every budget gate is inclusive at exactly the budget (strict {@code >}, D98's cost
     * note): a header whose length equals the budget is accepted, a frontier whose encoded
     * width equals the budget is expressed, and one byte more refuses at each gate.
     */
    @Test
    void budgetGatesAreInclusiveAtExactlyTheBudget() {
        ChannelId f1 = new ChannelId(new UUID(31, 1), 0);
        ChannelId f2 = new ChannelId(new UUID(31, 2), 0);
        byte[] exact = CausesCodec.encode(Causes.of(Map.of(f1, 5L, f2, 9L)));
        int budget = exact.length;

        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store, budget);
        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, engine.onReceive(new ReceivedMessage(
                C1, 0, 0, "A".getBytes(), "A".getBytes(), List.of(new HeaderKV(CausesCodec.HEADER_KEY, exact)))),
                "a header of exactly the budget is accepted");
        assertEquals(budget, engine.frontierBytes(), "the frontier now sits exactly at the budget");
        assertEquals(budget, engine.causesHeaderForEmission().length, "and is still expressible");

        ProcessEngine tighter = new ProcessEngine("q", BOTH, new MemoryOrderingStore(), budget - 1);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> tighter.onReceive(new ReceivedMessage(C1, 0, 0, "A".getBytes(), "A".getBytes(),
                        List.of(new HeaderKV(CausesCodec.HEADER_KEY, exact)))));
        assertEquals(ParsleyFailClosedException.Reason.METADATA_BUDGET_EXCEEDED, e.reason(),
                "one byte past the budget must refuse with the budget's own reason");
    }

    /**
     * The emission header equals a fresh canonical encoding of the frontier, byte for byte,
     * after each mutation kind: a position-only raise of a tracked channel (which changes no
     * size, so a size-keyed check cannot see it), a delivery merge, a new channel, a prune,
     * and a restore. This is the invariant any cached or incrementally patched header must
     * keep; {@code ProcessEngineTest#frontierBytesAgreesWithTheEncodedHeader} compares
     * lengths only.
     */
    @Test
    void emissionHeaderIsByteExactAfterEveryFrontierMutationKind() {
        ChannelId x = new ChannelId(new UUID(50, 1), 3);
        ChannelId y = new ChannelId(new UUID(50, 1), 7);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", BOTH, store);
        assertArrayEquals(CausesCodec.encode(Causes.none()), engine.causesHeaderForEmission(), "empty");

        engine.onReceive(caused(C1, 0, "A", Map.of(x, 5L)));
        assertArrayEquals(CausesCodec.encode(engine.frontierSnapshot()), engine.causesHeaderForEmission(),
                "after a new channel");
        byte[] first = engine.causesHeaderForEmission();

        engine.onReceive(caused(C1, 1, "B", Map.of(x, 9L)));
        byte[] afterRaise = engine.causesHeaderForEmission();
        assertArrayEquals(CausesCodec.encode(engine.frontierSnapshot()), afterRaise,
                "after a position-only raise of a tracked channel");
        assertFalse(java.util.Arrays.equals(first, afterRaise), "the raise must change the bytes");
        assertEquals(first.length, afterRaise.length, "staging: the raise changes no size");

        engine.markDelivered(C1, 0);
        assertArrayEquals(CausesCodec.encode(engine.frontierSnapshot()), engine.causesHeaderForEmission(),
                "after a delivery merge");
        engine.markDelivered(C1, 1);
        assertArrayEquals(CausesCodec.encode(engine.frontierSnapshot()), engine.causesHeaderForEmission(),
                "after a position-only delivery merge");

        engine.onReceive(caused(C1, 2, "C", Map.of(y, 1L)));
        engine.onFacts(new PositionFacts(Map.of(), Map.of(x, 10L), Set.of()));
        assertEquals(Causes.of(Map.of(C1, 1L, y, 1L)), engine.frontierSnapshot(), "staging: x pruned");
        assertArrayEquals(CausesCodec.encode(engine.frontierSnapshot()), engine.causesHeaderForEmission(),
                "after a prune");

        engine.flushHolds();
        store.commit();
        ProcessEngine restored = new ProcessEngine("p", BOTH, store);
        assertArrayEquals(CausesCodec.encode(restored.frontierSnapshot()), restored.causesHeaderForEmission(),
                "after a restore");
        restored.onReceive(caused(C2, 0, "D", Map.of(y, 4L)));
        assertArrayEquals(CausesCodec.encode(restored.frontierSnapshot()), restored.causesHeaderForEmission(),
                "after a position-only raise on a restored frontier");
    }

    /**
     * A header position of {@code Long.MAX_VALUE} is the engine's own fed-to-end sentinel.
     * Absorbed from a header and delivered while its channel is outside the received set, it
     * would enter the delivered past; when that channel later joined, the D31 clamp would
     * copy it into {@code fedUpTo}, where it reads as "this channel no longer exists", and
     * every feed on the live channel would be refused as a dead-channel breach, recurring on
     * every restart. Receipt therefore refuses the pair as undecodable (wire-format
     * constraint 7, D105), and a restore refuses state that absorbed one before the refusal
     * existed.
     */
    @Test
    void aHeaderPositionAtLongMaxValueIsRefusedBeforeItCanReachTheFedToEndSentinel() throws Exception {
        java.nio.ByteBuffer forged = java.nio.ByteBuffer.allocate(1 + 1 + 16 + 1 + 1 + 8);
        forged.put(CausesCodec.FORMAT_VERSION).put((byte) 1);
        forged.putLong(C1.topicId().getMostSignificantBits()).putLong(C1.topicId().getLeastSignificantBits());
        forged.put((byte) 1).put((byte) 0).putLong(Long.MAX_VALUE);
        MemoryOrderingStore store = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", Map.of(C2, "c2"), store);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> engine.onReceive(new ReceivedMessage(C2, 0, 0, "h".getBytes(), "h".getBytes(),
                        List.of(new HeaderKV(CausesCodec.HEADER_KEY, forged.array())))),
                "a position no channel can assign must be refused at receipt");
        assertEquals(ParsleyFailClosedException.Reason.UNDECODABLE_METADATA, e.reason(),
                "a forged header is refused as undecodable metadata");
        assertTrue(e.getMessage().contains("beyond any position a channel can assign"), e.getMessage());
        assertTrue(engine.frontierSnapshot().isEmpty(), "the refused pair must not enter the frontier");

        assertThrows(IllegalArgumentException.class, () -> Causes.of(Map.of(C1, Long.MAX_VALUE)),
                "the value object refuses it too, so no encoder can spell it");

        // State written before the refusal existed: a delivered-past row at the sentinel.
        store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        store.put(StoreCodec.channelKey(StoreCodec.TAG_DELIVERED_PAST, C1), StoreCodec.encodeLong(Long.MAX_VALUE));
        ParsleyFailClosedException restore = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "a restored delivered past at the sentinel must refuse rather than mark a live channel deleted");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, restore.reason(),
                "untrusted ordering state is refused as an unknown state format");
        assertTrue(restore.getMessage().contains("delivered past names position"), restore.getMessage());
    }

    /**
     * A negative position in a restored frontier row is refused at restore, before it can
     * be re-expressed. Receipt has always refused negative positions, so such a row can only
     * be corrupt store state; and since the emission path encodes the engine's frontier map
     * directly (D102), restore is the one point between the store and the wire where the
     * value is checked — left in place, every emission would carry a header downstream
     * readers refuse as undecodable, blaming the sender's metadata for this process's state.
     */
    @Test
    void aNegativeRestoredFrontierRowIsRefusedBeforeItCanBeReExpressed() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        store.put(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, C1), StoreCodec.encodeLong(-1L));
        ParsleyFailClosedException restore = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", BOTH, store),
                "a negative restored frontier position must refuse rather than reach the wire");
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, restore.reason(),
                "corrupt ordering state is refused as an unknown state format");
        assertTrue(restore.getMessage().contains("frontier names position -1"), restore.getMessage());
        assertTrue(restore.getMessage().contains("corrupt ordering state"), restore.getMessage());
    }
}
