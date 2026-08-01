package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Directed protocol tests for shapes the randomized scenarios under-sample: a run of
 * consumer-skipped offsets that opens behind a held head, an echoed self sequence claim on a
 * channel that is no longer a declared sink, the boundaries of stamp normalisation and of the
 * truncation sweep, and the scope changes a restart performs exactly once. Every one is a
 * deterministic construction, so it pins the behaviour on every run rather than on the seeds
 * that happen to reach it.
 */
class CausalNodeDirectedTest {

    private static final UUID T_C = UUID.nameUUIDFromBytes("dtc".getBytes());
    private static final UUID T_D = UUID.nameUUIDFromBytes("dtd".getBytes());
    private static final UUID T_E = UUID.nameUUIDFromBytes("dte".getBytes());
    private static final UUID T_S = UUID.nameUUIDFromBytes("dts".getBytes());
    private static final UUID T_F = UUID.nameUUIDFromBytes("dtf".getBytes());
    private static final Channel C = new Channel(T_C, 0);
    private static final Channel D = new Channel(T_D, 0);
    private static final Channel E = new Channel(T_E, 0);
    /** A declared sink, so a stamp can be taken. Never consumed. */
    private static final Channel SINK = new Channel(T_S, 0);
    /** Neither consumed nor a sink: only ever reached as carried custody. */
    private static final Channel FOREIGN = new Channel(T_F, 0);
    private static final UUID SELF = UUID.nameUUIDFromBytes("dself".getBytes());
    private static final UUID UPSTREAM = UUID.nameUUIDFromBytes("dupstream".getBytes());
    private static final UUID STRANGER = UUID.nameUUIDFromBytes("dstranger".getBytes());

    /** Plain in-memory store, with no staging. Every write is immediately durable. */
    private static final class MapStore implements StateStore {
        final TreeMap<String, byte[]> map = new TreeMap<>();

        @Override
        public void put(String key, byte[] value) {
            map.put(key, value.clone());
        }

        @Override
        public byte[] get(String key) {
            byte[] v = map.get(key);
            return v == null ? null : v.clone();
        }

        @Override
        public void delete(String key) {
            map.remove(key);
        }

        @Override
        public void forEachPrefix(String prefix, BiConsumer<String, byte[]> consumer) {
            map.subMap(prefix, prefix + Character.MAX_VALUE)
                    .forEach((k, v) -> consumer.accept(k, v.clone()));
        }
    }

    /** End offsets for declared sink topics only, exactly as the admin-backed seam answers. */
    private static final class Offsets implements BrokerOffsets {
        final Map<Channel, Long> ends = new HashMap<>();

        @Override
        public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
            Map<Channel, Long> out = new HashMap<>();
            ends.forEach((c, e) -> {
                if (sinkTopics.contains(c.topicId())) out.put(c, e);
            });
            return out;
        }

        @Override
        public EarliestOffsets earliestOffsets(Set<Channel> channels) {
            return new EarliestOffsets(Map.of(), Set.of());
        }
    }

    private static InboundRecord rec(Channel c, long offset, VectorClock clock) {
        return new InboundRecord(c, offset, clock, null, -1, new byte[] {1}, new byte[] {2}, 1000L);
    }

    /** The same record carrying a sender tag, so its delivery marks a sequence. */
    private static InboundRecord tagged(Channel c, long offset, VectorClock clock, UUID sender,
                                        long seq) {
        return new InboundRecord(c, offset, clock, sender, seq, new byte[] {1}, new byte[] {2}, 1000L);
    }

    private static NodeConfig config(Set<Channel> consumed, Set<UUID> sinks) {
        return new NodeConfig("n", SELF, consumed, sinks, 0);
    }

    private static VectorClock seqClaim(Channel c, UUID sender, long seq) {
        VectorClock k = new VectorClock();
        k.advanceSeq(c, sender, seq);
        return k;
    }

    /**
     * A run of consumer-skipped offsets (transaction markers, aborted records) that opens
     * behind a held head must still fold into the frontier. Every business record below the
     * head has been delivered, so the rest of the prefix was skipped, by the same known-clean
     * argument the empty-queue bridge rests on. Left unbridged, a claim naming an offset inside
     * the gap wedges permanently: nothing else can ever advance the frontier through it,
     * because a position advance is refused while the queue is non-empty.
     */
    @Test
    void aSkippedRunBehindAHeldHeadFoldsIntoTheFrontier() {
        CausalNode n = new CausalNode(
                new NodeConfig("n", SELF, Set.of(C, D), Set.of(), 0), new MapStore(), new Offsets());

        // c@5 arrives first: the queue is empty, so the seed folds the prefix below it. It
        // holds, because the d@0 it depends on has not arrived.
        assertTrue(n.onRecord(rec(C, 5, VectorClock.of(D, 0))).isEmpty(),
                "c@5 must hold until its d@0 cause is delivered");

        // c@9 arrives behind the held head; 6..8 are markers a read_committed consumer never
        // returns. It depends on d@1, so it holds in turn.
        assertTrue(n.onRecord(rec(C, 9, VectorClock.of(D, 1))).isEmpty(),
                "c@9 must hold behind the head and its own d@1 cause");

        // d@0 delivers and releases c@5, leaving c@9 as the head above the skipped run.
        assertEquals(2, n.onRecord(rec(D, 0, new VectorClock())).size(),
                "d@0 must deliver and cascade the release of c@5");

        // d@1 claims c@7 — a marker offset inside the run. An end-offset seed over c mints
        // exactly this claim, so the gate must already count it as covered.
        assertEquals(2, n.onRecord(rec(D, 1, VectorClock.of(C, 7))).size(),
                "d@1 must deliver against the bridged frontier and release c@9");
        assertEquals(Map.of(), n.headOffsets(),
                "every channel must drain: nothing may still gate on a skipped offset");
    }

    /**
     * A record in custody can carry a sequence claim this node itself minted, on a channel that
     * is no longer one of its declared sinks. A stage's own tick record echoes its claims
     * back, and so does any cycle whose intermediate hop does not consume the claimed sink and
     * therefore cannot normalise the claim. The init end-offset seed does not cover a dropped
     * sink, so the upgrade target must come from the carried ancestry the rescope heal wrote.
     */
    @Test
    void anEchoedSelfClaimOnADroppedSinkStillStamps() {
        MapStore store = new MapStore();
        Offsets offsets = new Offsets();
        offsets.ends.put(C, 10L);   // the sink that the second incarnation drops
        offsets.ends.put(D, 3L);    // the self-loop channel carrying the echo back

        CausalNode first = new CausalNode(
                new NodeConfig("s", SELF, Set.of(D), Set.of(T_C, T_D), 0), store, offsets);
        first.prepareSend(C);
        SendStamp loop = first.prepareSend(D);
        assertEquals(1, loop.clock().sequenceEntryCount(),
                "the loop record must carry this node's own unresolved claim on C");
        first.onRecord(new InboundRecord(D, 20L, loop.clock(), loop.senderId(), loop.senderSeq(),
                new byte[] {1}, new byte[] {2}, 1000L));

        CausalNode restarted = new CausalNode(
                new NodeConfig("s", SELF, Set.of(D), Set.of(T_D), 0), store, offsets);
        VectorClock stamp = restarted.prepareSend(D).clock();
        assertEquals(0, stamp.sequenceEntryCount(),
                "the echoed self claim must resolve, not stay unresolvable in sequence space");
        assertEquals(9, stamp.get(C),
                "the dropped sink stays claimed to its end-offset bound, so nothing is lost");
    }

    /**
     * The same echo on a sink whose topic no longer exists: the rescope heal cannot resolve it
     * and no end-offset seed covers it, so the claim is unclaimable by anyone and drops rather
     * than failing the stamp.
     */
    @Test
    void anEchoedSelfClaimOnADestroyedSinkIsDropped() {
        MapStore store = new MapStore();
        Offsets offsets = new Offsets();
        offsets.ends.put(C, 10L);
        offsets.ends.put(D, 3L);

        CausalNode first = new CausalNode(
                new NodeConfig("s", SELF, Set.of(D), Set.of(T_C, T_D), 0), store, offsets);
        first.prepareSend(C);
        SendStamp loop = first.prepareSend(D);
        first.onRecord(new InboundRecord(D, 20L, loop.clock(), loop.senderId(), loop.senderSeq(),
                new byte[] {1}, new byte[] {2}, 1000L));

        offsets.ends.remove(C); // the topic is gone: definitive absence, not a lookup failure
        CausalNode restarted = new CausalNode(
                new NodeConfig("s", SELF, Set.of(D), Set.of(T_D), 0), store, offsets);
        VectorClock stamp = restarted.prepareSend(D).clock();
        assertEquals(0, stamp.sequenceEntryCount(),
                "an unresolvable claim on a destroyed topic must drop, not fail the stamp");
    }

    // ---------------------------------------------------------------- the gate's frontier

    /**
     * The frontier must never be folded past a record still held. A record arriving behind a
     * held head says nothing about the head: the offsets below the arriving record include the
     * head's own, which is by definition undelivered. Folded in, the frontier would report the
     * head as delivered and release whatever waits on it, an effect ahead of its cause, which
     * is the one thing the gate exists to prevent.
     */
    @Test
    void aRecordArrivingBehindAHeldHeadDoesNotBridgePastIt() {
        CausalNode n = new CausalNode(
                config(Set.of(C, D, E), Set.of()), new MapStore(), new Offsets());

        assertTrue(n.onRecord(rec(C, 0, VectorClock.of(D, 5))).isEmpty(),
                "c@0 must hold until its d@5 cause is delivered");
        assertTrue(n.onRecord(rec(C, 1, null)).isEmpty(),
                "c@1 must queue behind the held head rather than deliver past it");

        assertTrue(n.onRecord(rec(E, 0, VectorClock.of(C, 0))).isEmpty(),
                "e@0 claims c@0, which is still held: the frontier must not have been folded"
                        + " past the head, so the claim is unmet and e@0 waits");
    }

    /**
     * A consumer position advance taken while records are held is remembered, and folds the
     * moment the queue drains. The queue is non-empty, so nothing can fold now, since every
     * offset below the head is either delivered or the head itself. But the host reports each advance
     * once, and a trailing run of markers has no other witness. Dropped, that run is never
     * bridged, and a claim naming an offset inside it waits forever.
     */
    @Test
    void aPositionAdvanceTakenWhileHoldingFoldsOnceTheQueueDrains() {
        CausalNode n = new CausalNode(config(Set.of(C, D), Set.of()), new MapStore(), new Offsets());

        assertTrue(n.onRecord(rec(C, 0, VectorClock.of(D, 0))).isEmpty(),
                "c@0 must hold until its d@0 cause is delivered");
        assertTrue(n.positionAdvance(C, 10).isEmpty(),
                "a position advance releases nothing while the head is still gated");

        assertEquals(2, n.onRecord(rec(D, 0, null)).size(),
                "d@0 must deliver and cascade the release of c@0");
        assertEquals(1, n.onRecord(rec(D, 1, VectorClock.of(C, 5))).size(),
                "c@5 sits inside the run that position advance covered, so once the queue"
                        + " drained the claim on it must already be satisfied");
    }

    /**
     * Delivering a restored held record advances the frontier. Nothing else can reach that
     * offset: the consumer position this incarnation knows starts empty, so the trailing-run
     * fold has no watermark to bridge with, and the record's own delivery is the only fact
     * establishing that the offset is covered.
     */
    @Test
    void deliveringARestoredHeldRecordAdvancesTheFrontier() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, new Offsets());
        assertTrue(first.onRecord(rec(C, 5, VectorClock.of(D, 0))).isEmpty(),
                "c@5 must hold until its d@0 cause is delivered");

        CausalNode restarted = new CausalNode(config(Set.of(C, D), Set.of()), store, new Offsets());
        assertEquals(2, restarted.onRecord(rec(D, 0, null)).size(),
                "d@0 must deliver and cascade the restored c@5");
        assertEquals(1, restarted.onRecord(rec(D, 1, VectorClock.of(C, 5))).size(),
                "the restored record's own delivery must have advanced the frontier to c@5");
    }

    // ---------------------------------------------------------------- stamp normalisation

    /**
     * A sequence claim at exactly the sequence this node has delivered is resolved, not carried
     * on: the delivered record is the claimed one. Left in sequence space it travels to every
     * downstream consumer, each of which must then resolve it against a sender it may never
     * have seen, which is the late-joiner window the offset form closes.
     */
    @Test
    void normalizationResolvesAClaimAtExactlyTheDeliveredSequence() {
        CausalNode n = new CausalNode(
                config(Set.of(C, D), Set.of(T_S)), new MapStore(), new Offsets());
        assertEquals(1, n.onRecord(tagged(C, 0, null, UPSTREAM, 5)).size(),
                "an unstamped record delivers and marks the sender at sequence 5");
        assertEquals(1, n.onRecord(rec(D, 0, seqClaim(C, UPSTREAM, 5))).size(),
                "a claim at the delivered sequence is met, so the record delivers");

        VectorClock stamp = n.prepareSend(SINK).clock();
        assertEquals(VectorClock.NOTHING, stamp.getSeq(new VectorClock.SeqKey(C, UPSTREAM)),
                "a claim this node can vouch for must be rewritten into offset space");
        assertEquals(0, stamp.sequenceEntryCount(),
                "nothing unresolved may remain in the stamp: " + stamp);
    }

    /**
     * A sequence claim this node cannot vouch for is carried on exactly as it stands. Rewriting
     * it to the offset of some earlier record from the same sender would claim strictly less
     * than the stamp was handed, since the claimed record sits at a higher offset, and a
     * sender this node has never delivered offers no offset to rewrite to at all.
     */
    @Test
    void normalizationNeverUnderClaimsAnUnresolvedSequenceClaim() {
        CausalNode n = new CausalNode(
                config(Set.of(C, D), Set.of(T_S)), new MapStore(), new Offsets());
        assertEquals(1, n.onRecord(tagged(C, 0, null, UPSTREAM, 5)).size(),
                "an unstamped record delivers and marks the sender at sequence 5");

        assertTrue(n.onRecord(rec(D, 0, seqClaim(C, UPSTREAM, 9))).isEmpty(),
                "a claim past the delivered sequence is unmet, so the record holds");
        assertEquals(9, n.prepareSend(SINK).clock().getSeq(new VectorClock.SeqKey(C, UPSTREAM)),
                "the claim names sequence 9; rewriting it to the offset of sequence 5 would"
                        + " claim a strictly earlier record");

        assertTrue(n.onRecord(rec(D, 1, seqClaim(C, STRANGER, 3))).isEmpty(),
                "a claim naming a sender never delivered here is unmet, so the record holds");
        assertEquals(3, n.prepareSend(SINK).clock().getSeq(new VectorClock.SeqKey(C, STRANGER)),
                "an unseen sender's claim must travel on untouched, not be dropped and not"
                        + " fail the stamp");
    }

    // ---------------------------------------------------------------- the truncation sweep

    /**
     * A truncation sweep over logs retention has not touched changes nothing. Log start zero
     * means nothing has been deleted, so there is no stability bound to apply. This is the
     * sweep's ordinary case, not an edge one: it runs on a timer against every channel a stamp
     * names, and most of them have never had a record deleted.
     */
    @Test
    void aTruncationSweepOverUndeletedLogsChangesNothing() {
        CausalNode n = new CausalNode(
                config(Set.of(C), Set.of(T_S)), new MapStore(), new Offsets());
        assertEquals(1, n.onRecord(rec(C, 0, VectorClock.of(FOREIGN, 5))).size(),
                "the record delivers, taking custody of foreign@5");

        n.truncateToLogStarts(Map.of(C, 0L, FOREIGN, 0L), Set.of());

        assertEquals(5, n.prepareSend(SINK).clock().get(FOREIGN),
                "no record has been deleted anywhere, so no claim may be truncated");
    }

    /**
     * A topic confirmed destroyed has its claims truncated entirely. A recreated topic is a
     * different channel, so no consumer anywhere can ever deliver the records those claims
     * name. Carried forever they would only grow the clock, and no gate would ever be
     * satisfied by them.
     */
    @Test
    void aDestroyedTopicsClaimsTruncateEntirely() {
        CausalNode n = new CausalNode(
                config(Set.of(C), Set.of(T_S)), new MapStore(), new Offsets());
        assertEquals(1, n.onRecord(rec(C, 0, VectorClock.of(FOREIGN, 5))).size(),
                "the record delivers, taking custody of foreign@5");

        n.truncateToLogStarts(Map.of(), Set.of(FOREIGN));

        assertEquals(VectorClock.NOTHING, n.prepareSend(SINK).clock().get(FOREIGN),
                "a destroyed topic's claims are unclaimable by anyone and must be dropped");
    }

    /**
     * Truncation is durable. The advertised clocks it prunes are read back at the next start,
     * so a truncation that is not written down is undone by the first restart, and the clock
     * regrows to exactly what the sweep last removed.
     */
    @Test
    void aTruncatedChannelClockStaysTruncatedAcrossARestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C), Set.of(T_S)), store, new Offsets());
        assertEquals(1, first.onRecord(rec(C, 0, VectorClock.of(FOREIGN, 41))).size(),
                "the record delivers, taking custody of foreign@41");
        first.truncate(VectorClock.of(FOREIGN, 41));
        assertEquals(VectorClock.NOTHING, first.prepareSend(SINK).clock().get(FOREIGN),
                "the claim sits at the stability bound and must leave the live clock");

        CausalNode restarted = new CausalNode(config(Set.of(C), Set.of(T_S)), store, new Offsets());
        assertEquals(VectorClock.NOTHING, restarted.prepareSend(SINK).clock().get(FOREIGN),
                "and must not come back at the next start");
    }

    /**
     * The truncation driver is told about channels only carried ancestry names. It queries log
     * starts for exactly the channels {@code stampChannels()} reports, so a channel left out of
     * that set carries a claim that can never be truncated. Carried ancestry, which the
     * frontier and the advertised clocks no longer cover, is precisely where a dropped input's
     * past lives.
     */
    @Test
    void carriedAncestryChannelsAreReportedToTheTruncationDriver() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, new Offsets());
        assertEquals(1, first.onRecord(rec(C, 0, null)).size(), "an unstamped record delivers");
        assertEquals(1, first.onRecord(rec(D, 0, null)).size(), "an unstamped record delivers");

        CausalNode restarted = new CausalNode(config(Set.of(D), Set.of()), store, new Offsets());
        assertEquals(Set.of(C), restarted.stampChannels(),
                "the dropped input survives only in carried ancestry, and the sweep must still"
                        + " be told to ask for its log start");
    }

    // ---------------------------------------------------------------- scope changes

    /**
     * A dropped input's delivered past is carried, and stays carried. Frontier offset zero is a
     * real claim, the first record of a channel, and the advertised clock the dropped input
     * contributed names causes on channels nothing else claims. Both must survive the restart
     * that drops the input and every restart after it, because the rescope that heals them
     * sees the input exactly once. The next start's recorded scope no longer mentions it.
     */
    @Test
    void aDroppedInputsPastIsCarriedAndStaysCarried() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of(T_S)), store, new Offsets());
        assertEquals(1, first.onRecord(rec(C, 0, VectorClock.of(FOREIGN, 7))).size(),
                "the record delivers, taking custody of foreign@7");

        CausalNode shrunk = new CausalNode(config(Set.of(D), Set.of(T_S)), store, new Offsets());
        VectorClock afterShrink = shrunk.prepareSend(SINK).clock();
        assertEquals(0, afterShrink.get(C),
                "the dropped input's frontier must be carried; offset zero is a claim like any"
                        + " other");
        assertEquals(7, afterShrink.get(FOREIGN),
                "the dropped input's advertised clock must be carried: nothing else claims"
                        + " foreign@7");

        CausalNode again = new CausalNode(config(Set.of(D), Set.of(T_S)), store, new Offsets());
        VectorClock afterSecondStart = again.prepareSend(SINK).clock();
        assertEquals(0, afterSecondStart.get(C),
                "the heal runs once, so a carried claim it does not write down is lost at the"
                        + " next start");
        assertEquals(7, afterSecondStart.get(FOREIGN),
                "the heal runs once, so a carried claim it does not write down is lost at the"
                        + " next start");
    }

    /**
     * A former sink's healed claim is written down. Dropping a sink leaves the node's own past
     * outputs there claimed by nothing else, with no frontier, since the node never consumed
     * it, and no end-offset seed, since it is no longer declared. So the rescope heal folds its
     * end offsets into carried ancestry. That heal sees the sink exactly once. By the next start
     * the recorded scope no longer lists it, so a heal that is not persisted is simply lost.
     */
    @Test
    void aHealedFormerSinksClaimSurvivesASecondStart() {
        MapStore store = new MapStore();
        Offsets offsets = new Offsets();
        offsets.ends.put(C, 10L);
        offsets.ends.put(D, 3L);
        new CausalNode(config(Set.of(D), Set.of(T_C, T_D)), store, offsets);

        CausalNode healed = new CausalNode(config(Set.of(D), Set.of(T_D)), store, offsets);
        assertEquals(9, healed.prepareSend(D).clock().get(C),
                "the dropped sink's last appended offset must be healed into carried ancestry");

        CausalNode again = new CausalNode(config(Set.of(D), Set.of(T_D)), store, offsets);
        assertEquals(9, again.prepareSend(D).clock().get(C),
                "and must still be claimed at the next start, which no longer sees the sink as"
                        + " former and so cannot heal it again");
    }

    /**
     * A dropped input leaves no state behind. Its frontier, advertised clock, queue metadata,
     * and delivered-sequence marks are all keyed by a channel nothing will feed again. Left in
     * the store they are read back by every later start and replicated by the changelog
     * forever, so the state of a long-lived application only ever grows.
     */
    @Test
    void aDroppedInputLeavesNoStateBehind() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, new Offsets());
        assertEquals(1,
                first.onRecord(tagged(C, 0, VectorClock.of(FOREIGN, 7), UPSTREAM, 3)).size(),
                "the record delivers, writing this channel's frontier, advertised clock, queue"
                        + " metadata and delivered-sequence mark");
        assertTrue(store.map.keySet().stream().anyMatch(k -> k.contains(C.key())),
                "the input must have state to leave behind for this test to mean anything");

        new CausalNode(config(Set.of(D), Set.of()), store, new Offsets());

        assertEquals(List.of(),
                store.map.keySet().stream().filter(k -> k.contains(C.key())).toList(),
                "every key of the removed input must be deleted");
    }

    /**
     * A sink dropped before anything was ever written to it costs nothing to heal. The heal
     * folds the former sink's end offsets into carried ancestry so the node's own past outputs
     * stay claimed. An empty topic has no appended offset to claim, and asking for one would
     * fail a start that has nothing to recover.
     */
    @Test
    void droppingASinkThatWasNeverWrittenToDoesNotFailInit() {
        MapStore store = new MapStore();
        Offsets offsets = new Offsets();
        offsets.ends.put(C, 0L); // declared and created, but never produced to
        new CausalNode(config(Set.of(D), Set.of(T_C, T_D)), store, offsets);

        CausalNode restarted = new CausalNode(config(Set.of(D), Set.of(T_D)), store, offsets);

        assertEquals(VectorClock.NOTHING, restarted.prepareSend(D).clock().get(C),
                "an empty former sink has no appended offset, so it claims nothing");
    }

    /**
     * An input grown into scope is seeded at what the node already knows, even when that
     * knowledge is offset zero. Here the grown channel is one of the node's own declared sinks,
     * so the only thing that knows about it is the init end-offset seed. Unseeded, the node
     * refetches and re-delivers its own output, and with the frontier left unseeded a claim on
     * that output, which every one of the node's own later stamps carries, waits forever.
     */
    @Test
    void aGrownInputIsSeededAtItsOwnOutputsEndOffset() {
        MapStore store = new MapStore();
        Offsets offsets = new Offsets();
        offsets.ends.put(C, 1L); // one record, this node's own, at offset 0
        new CausalNode(config(Set.of(D), Set.of(T_C)), store, offsets);

        CausalNode grown = new CausalNode(config(Set.of(C, D), Set.of(T_C)), store, offsets);

        assertEquals(Map.of(C, 1L), grown.resumePositions(),
                "the grown input must resume one past what the node already knows, so its own"
                        + " output is not delivered back to it");
        assertEquals(1, grown.onRecord(rec(D, 0, VectorClock.of(C, 0))).size(),
                "and the seeded frontier must already satisfy a claim on that same offset");
    }

    // ------------------------------------------------------------------ stable iteration order
    //
    // NodeConfig normalises its channel sets with Set.copyOf, and the JDK's immutable sets
    // randomise iteration order per JVM from a salt seeded at class-init. Iterating them
    // directly would make cross-channel delivery order and the scope record's bytes differ
    // between runs of the same tree, which would make a seeded simulator run depend on the JVM
    // as well as its seed. Both tests below pin a fixed order. Neither can fail deterministically
    // under the bug they guard — the bug is the absence of an order — so each uses enough
    // channels that an accidental pass is unlikely.

    /** Channels with hand-picked ids, so the expected sorted order is readable off the literals. */
    private static Channel ordered(int id) {
        return new Channel(new UUID(id, 0), 0);
    }

    /**
     * The scope record lists channels in a stable order. It is rewritten at every init and read
     * back by the next one, so a salt-dependent order means the same logical scope serialises
     * to different bytes on different JVMs. That is changelog churn at best, and an unpinnable
     * record in {@link WireFormatTest} at worst. Five channels, so an unordered set passes by luck once in
     * a hundred and twenty runs.
     */
    @Test
    void theScopeRecordListsItsChannelsInAStableSortedOrder() {
        MapStore store = new MapStore();
        Set<Channel> consumed = Set.of(ordered(5), ordered(3), ordered(1), ordered(4), ordered(2));
        new CausalNode(config(consumed, Set.of()), store, new Offsets());

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(store.map.get("scope"));
        int count = buf.getInt();
        assertEquals(5, count, "every consumed channel must appear in the scope record");
        List<Channel> written = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            written.add(new Channel(new UUID(buf.getLong(), buf.getLong()), buf.getInt()));
        }
        assertEquals(written.stream().sorted().toList(), written,
                "the scope record must serialise its channels in a fixed order, or the same"
                        + " scope writes different bytes on different JVMs: " + written);
    }

    /**
     * One cascade delivers across channels in a stable order. The order itself is arbitrary,
     * since nothing in the causal model prefers one concurrent channel over another, but it
     * must be the same on every JVM, because the host runs user logic per delivery in list order, so in
     * the simulator this order reaches the broker as send order and from there into every
     * offset downstream. Salt-dependent here means a seed no longer reproduces its run.
     */
    @Test
    void oneCascadeDeliversAcrossChannelsInAStableOrder() {
        Channel first = ordered(1);
        Channel second = ordered(2);
        Channel third = ordered(3);
        CausalNode node = new CausalNode(
                config(Set.of(first, second, third), Set.of()), new MapStore(), new Offsets());

        // Both hold on the same absent cause, so one arrival releases them together.
        assertTrue(node.onRecord(rec(third, 0, VectorClock.of(first, 0))).isEmpty(),
                "third must hold until first@0 is delivered");
        assertTrue(node.onRecord(rec(second, 0, VectorClock.of(first, 0))).isEmpty(),
                "second must hold until first@0 is delivered");

        List<Delivery> out = node.onRecord(rec(first, 0, new VectorClock()));

        assertEquals(List.of(first, second, third), out.stream().map(Delivery::channel).toList(),
                "the cause and both released records must arrive in a fixed channel order");
    }
}
