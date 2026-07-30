package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Directed gate and stamp tests for shapes the randomized scenarios under-sample: a run of
 * consumer-skipped offsets that opens behind a held head, and an echoed self sequence claim on
 * a channel that is no longer a declared sink. Both are deterministic constructions, so they
 * pin the behaviour on every run rather than on the seeds that happen to reach it.
 */
class CausalNodeDirectedTest {

    private static final UUID T_C = UUID.nameUUIDFromBytes("dtc".getBytes());
    private static final UUID T_D = UUID.nameUUIDFromBytes("dtd".getBytes());
    private static final Channel C = new Channel(T_C, 0);
    private static final Channel D = new Channel(T_D, 0);
    private static final UUID SELF = UUID.nameUUIDFromBytes("dself".getBytes());

    /** Plain in-memory store: no staging — every write is immediately durable. */
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

    /**
     * A run of consumer-skipped offsets (transaction markers, aborted records) that opens
     * behind a held head must still fold into the frontier. Every business record below the
     * head has been delivered, so the rest of the prefix was skipped — the same known-clean
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
     * is no longer one of its declared sinks — a stage's own tick record echoes its claims
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
}
