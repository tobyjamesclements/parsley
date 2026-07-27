package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Directed persistence tests: everything a restart must restore, checked deterministically
 * against a shared store across two node incarnations. The simulator exercises these paths
 * under load; these tests pin each persisted fact individually (they exist to kill the
 * mutants the simulator's randomized schedules let slip).
 */
class CausalNodePersistenceTest {

    private static final UUID TOPIC_1 = UUID.nameUUIDFromBytes("pt1".getBytes());
    private static final UUID TOPIC_2 = UUID.nameUUIDFromBytes("pt2".getBytes());
    private static final UUID SINK = UUID.nameUUIDFromBytes("psink".getBytes());
    private static final Channel C1 = new Channel(TOPIC_1, 0);
    private static final Channel C2 = new Channel(TOPIC_2, 0);
    private static final Channel SINK_0 = new Channel(SINK, 0);
    private static final UUID UPSTREAM = UUID.nameUUIDFromBytes("upstream".getBytes());

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
            map.subMap(prefix, prefix + Character.MAX_VALUE).forEach((k, v) -> consumer.accept(k, v.clone()));
        }
    }

    private static final SendTracker NO_ACKS = new SendTracker() {
        @Override
        public List<Ack> drainAcks() {
            return List.of();
        }

        @Override
        public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
            return Map.of();
        }

        @Override
        public EarliestOffsets earliestOffsets(Set<Channel> channels) {
            return new EarliestOffsets(Map.of(), Set.of());
        }
    };

    private static NodeConfig config() {
        return new NodeConfig("persist-test", UUID.nameUUIDFromBytes("self".getBytes()),
                Set.of(C1, C2), Set.of(SINK), 0, 64);
    }

    private static InboundRecord record(Channel c, long offset, Clock clock, UUID sender, long seq,
                                        byte[] key, byte[] value) {
        return new InboundRecord(c, offset, clock, sender, seq, key, value, 1000);
    }

    /** Send sequences must survive a restart: a reused sequence would double-tag records. */
    @Test
    void sendSequencesSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_ACKS);
        assertEquals(0, first.prepareSend(SINK_0).senderSeq());
        assertEquals(1, first.prepareSend(SINK_0).senderSeq());

        CausalNode restarted = new CausalNode(config(), store, NO_ACKS);
        assertEquals(2, restarted.prepareSend(SINK_0).senderSeq(),
                "a restart must continue the send sequence, never reuse one");
    }

    /** Delivered-sequence marks must survive a restart: they resolve later sequence claims. */
    @Test
    void deliveredSequencesSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_ACKS);
        List<Delivery> delivered = first.onRecord(record(C1, 0, null, UPSTREAM, 5, null, null));
        assertEquals(1, delivered.size(), "an unstamped record delivers immediately");

        CausalNode restarted = new CausalNode(config(), store, NO_ACKS);
        Clock resolved = new Clock();
        resolved.advanceSeq(C1, UPSTREAM, 5);
        assertEquals(1, restarted.onRecord(record(C2, 0, resolved, null, -1, null, null)).size(),
                "a claim at the delivered sequence must be resolved from restored state");

        Clock unresolved = new Clock();
        unresolved.advanceSeq(C1, UPSTREAM, 6);
        assertEquals(0, restarted.onRecord(record(C2, 1, unresolved, null, -1, null, null)).size(),
                "a claim past the delivered sequence must hold");
    }

    /** The frontier must survive a restart: a lost frontier re-delivers on replay. */
    @Test
    void frontierSurvivesRestartAndDropsReplays() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_ACKS);
        assertEquals(1, first.onRecord(record(C1, 0, null, null, -1, null, null)).size());

        CausalNode restarted = new CausalNode(config(), store, NO_ACKS);
        assertTrue(restarted.onRecord(record(C1, 0, null, null, -1, null, null)).isEmpty(),
                "a replayed offset at or below the restored frontier must be dropped");
    }

    /**
     * A held record survives a restart byte-for-byte, including a null key, an empty value,
     * and its sender tag — the tag must still advance the delivered sequence on release.
     */
    @Test
    void heldRecordsSurviveRestartVerbatim() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_ACKS);
        Clock deps = Clock.of(C1, 0);
        assertTrue(first.onRecord(record(C2, 0, deps, UPSTREAM, 7, null, new byte[0])).isEmpty(),
                "the record must hold until its C1 cause is delivered");

        CausalNode restarted = new CausalNode(config(), store, NO_ACKS);
        List<Delivery> released = restarted.onRecord(record(C1, 0, null, null, -1, "k".getBytes(), "v".getBytes()));
        assertEquals(2, released.size(), "delivering the cause must cascade the restored held record");
        Delivery held = released.get(1);
        assertNull(held.key(), "a null key must restore as null, not empty");
        assertArrayEquals(new byte[0], held.value(), "an empty value must restore as empty, not null");

        Clock resolved = new Clock();
        resolved.advanceSeq(C2, UPSTREAM, 7);
        assertEquals(1, restarted.onRecord(record(C1, 1, resolved, null, -1, null, null)).size(),
                "the restored record's sender tag must have advanced the delivered sequence");
    }

    /** The advertised channel clocks must survive a restart: custody lost is claims lost. */
    @Test
    void channelClocksSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_ACKS);
        Channel foreign = new Channel(UUID.nameUUIDFromBytes("foreign".getBytes()), 0);
        Clock custody = Clock.of(foreign, 41);
        first.onRecord(record(C1, 0, custody, null, -1, null, null));

        CausalNode restarted = new CausalNode(config(), store, NO_ACKS);
        assertEquals(41, restarted.prepareSend(SINK_0).clock().get(foreign),
                "custody folded before the restart must still be claimed after it");
    }
}
