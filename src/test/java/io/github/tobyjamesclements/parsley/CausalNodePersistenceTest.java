package io.github.tobyjamesclements.parsley;

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
 * under load. These tests pin each persisted fact individually, to kill the mutants the
 * simulator's randomized schedules let slip.
 */
class CausalNodePersistenceTest {

    private static final UUID TOPIC_1 = UUID.nameUUIDFromBytes("pt1".getBytes());
    private static final UUID TOPIC_2 = UUID.nameUUIDFromBytes("pt2".getBytes());
    private static final UUID SINK = UUID.nameUUIDFromBytes("psink".getBytes());
    private static final Channel C1 = new Channel(TOPIC_1, 0);
    private static final Channel C2 = new Channel(TOPIC_2, 0);
    private static final Channel SINK_0 = new Channel(SINK, 0);
    private static final UUID UPSTREAM = UUID.nameUUIDFromBytes("upstream".getBytes());

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
            map.subMap(prefix, prefix + Character.MAX_VALUE).forEach((k, v) -> consumer.accept(k, v.clone()));
        }
    }

    private static final BrokerOffsets NO_OFFSETS = new BrokerOffsets() {
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
                Set.of(C1, C2), Set.of(SINK), 0);
    }

    private static InboundRecord record(Channel c, long offset, VectorClock clock, UUID sender, long seq,
                                        byte[] key, byte[] value) {
        return new InboundRecord(c, offset, clock, sender, seq, key, value, 1000);
    }

    /** Send sequences must survive a restart: a reused sequence would double-tag records. */
    @Test
    void sendSequencesSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        assertEquals(0, first.prepareSend(SINK_0).senderSeq());
        assertEquals(1, first.prepareSend(SINK_0).senderSeq());

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        assertEquals(2, restarted.prepareSend(SINK_0).senderSeq(),
                "a restart must continue the send sequence, never reuse one");
    }

    /** Delivered-sequence marks must survive a restart: they resolve later sequence claims. */
    @Test
    void deliveredSequencesSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        List<Delivery> delivered = first.onRecord(record(C1, 0, null, UPSTREAM, 5, null, null));
        assertEquals(1, delivered.size(), "an unstamped record delivers immediately");

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        VectorClock resolved = new VectorClock();
        resolved.advanceSeq(C1, UPSTREAM, 5);
        assertEquals(1, restarted.onRecord(record(C2, 0, resolved, null, -1, null, null)).size(),
                "a claim at the delivered sequence must be resolved from restored state");

        VectorClock unresolved = new VectorClock();
        unresolved.advanceSeq(C1, UPSTREAM, 6);
        assertEquals(0, restarted.onRecord(record(C2, 1, unresolved, null, -1, null, null)).size(),
                "a claim past the delivered sequence must hold");
    }

    /** The frontier must survive a restart: a lost frontier re-delivers on replay. */
    @Test
    void frontierSurvivesRestartAndDropsReplays() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        assertEquals(1, first.onRecord(record(C1, 0, null, null, -1, null, null)).size());

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        assertTrue(restarted.onRecord(record(C1, 0, null, null, -1, null, null)).isEmpty(),
                "a replayed offset at or below the restored frontier must be dropped");
    }

    /**
     * A held record survives a restart byte-for-byte, including a null key, an empty value,
     * and its sender tag, which must still advance the delivered sequence on release.
     */
    @Test
    void heldRecordsSurviveRestartVerbatim() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        VectorClock deps = VectorClock.of(C1, 0);
        assertTrue(first.onRecord(record(C2, 0, deps, UPSTREAM, 7, null, new byte[0])).isEmpty(),
                "the record must hold until its C1 cause is delivered");

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        List<Delivery> released = restarted.onRecord(record(C1, 0, null, null, -1, "k".getBytes(), "v".getBytes()));
        assertEquals(2, released.size(), "delivering the cause must cascade the restored held record");
        Delivery held = released.get(1);
        assertNull(held.key(), "a null key must restore as null, not empty");
        assertArrayEquals(new byte[0], held.value(), "an empty value must restore as empty, not null");

        VectorClock resolved = new VectorClock();
        resolved.advanceSeq(C2, UPSTREAM, 7);
        assertEquals(1, restarted.onRecord(record(C1, 1, resolved, null, -1, null, null)).size(),
                "the restored record's sender tag must have advanced the delivered sequence");
    }

    /**
     * The payload of a held record survives the restore path unchanged: key bytes, value
     * bytes, and timestamp. The oracle cannot see this, because it judges deliveries by
     * coordinate, so a restore that returned the right record with the wrong bytes reads as
     * correct everywhere in the simulator. This is the assertion that says the bytes handed to user
     * logic after a restart are the bytes that were fetched.
     */
    @Test
    void heldRecordPayloadSurvivesRestartUnchanged() {
        MapStore store = new MapStore();
        byte[] key = "order-42".getBytes();
        byte[] value = new byte[256];
        for (int i = 0; i < value.length; i++) value[i] = (byte) i; // every byte value once
        long timestamp = 1_700_000_000_123L;

        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        assertTrue(first.onRecord(new InboundRecord(C2, 0, VectorClock.of(C1, 0), UPSTREAM, 7,
                        key, value, timestamp)).isEmpty(),
                "the record must hold until its C1 cause is delivered");

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        List<Delivery> released = restarted.onRecord(record(C1, 0, null, null, -1, null, null));
        assertEquals(2, released.size(), "delivering the cause must cascade the restored record");
        Delivery held = released.get(1);
        assertArrayEquals(key, held.key(), "the held record's key bytes must survive the store");
        assertArrayEquals(value, held.value(), "the held record's value bytes must survive the store");
        assertEquals(timestamp, held.timestamp(),
                "the held record's timestamp must survive the store: it is the message time"
                        + " user logic sees, and the default timestamp of its emissions");
        assertEquals(0, held.offset(), "the held record's own coordinate must survive the store");
    }

    /**
     * The envelope's optional fields round-trip in every combination the layout distinguishes:
     * an untagged sender, a null key beside a non-empty value, an empty key beside a null
     * value, and an empty dependency clock. Each pairs a presence flag with a length field in
     * the held-record layout, and each is a place where null and empty can silently swap.
     */
    @Test
    void heldRecordEnvelopeRoundTripsEveryOptionalField() {
        record Case(String name, byte[] key, byte[] value, UUID sender, long seq, VectorClock clock) {}
        List<Case> cases = List.of(
                new Case("untagged sender", "k".getBytes(), "v".getBytes(), null, -1, VectorClock.of(C1, 0)),
                new Case("null key, real value", null, "v".getBytes(), UPSTREAM, 3, VectorClock.of(C1, 0)),
                new Case("empty key, null value", new byte[0], null, UPSTREAM, 0, VectorClock.of(C1, 0)),
                new Case("empty clock held behind a head", "k".getBytes(), "v".getBytes(), UPSTREAM, 1,
                        new VectorClock()));

        for (Case c : cases) {
            MapStore store = new MapStore();
            CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
            // A blocked head at C2@0 keeps the second record queued whatever its own clock is,
            // so the empty-clock case exercises the queue rather than delivering on arrival.
            assertTrue(first.onRecord(record(C2, 0, VectorClock.of(C1, 0), null, -1, null, null)).isEmpty(),
                    c.name() + ": the head must hold");
            assertTrue(first.onRecord(new InboundRecord(C2, 1, c.clock(), c.sender(), c.seq(),
                    c.key(), c.value(), 4242L)).isEmpty(), c.name() + ": the record must queue");

            CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
            List<Delivery> released = restarted.onRecord(record(C1, 0, null, null, -1, null, null));
            assertEquals(3, released.size(), c.name() + ": the cause must release both records");
            Delivery back = released.get(2);
            assertArrayEquals(c.key(), back.key(), c.name() + ": key must round-trip");
            assertArrayEquals(c.value(), back.value(), c.name() + ": value must round-trip");
            assertEquals(4242L, back.timestamp(), c.name() + ": timestamp must round-trip");
        }
    }

    /** The advertised channel clocks must survive a restart: custody lost is claims lost. */
    @Test
    void channelClocksSurviveRestart() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(), store, NO_OFFSETS);
        Channel foreign = new Channel(UUID.nameUUIDFromBytes("foreign".getBytes()), 0);
        VectorClock custody = VectorClock.of(foreign, 41);
        first.onRecord(record(C1, 0, custody, null, -1, null, null));

        CausalNode restarted = new CausalNode(config(), store, NO_OFFSETS);
        assertEquals(41, restarted.prepareSend(SINK_0).clock().get(foreign),
                "custody folded before the restart must still be claimed after it");
    }
}
