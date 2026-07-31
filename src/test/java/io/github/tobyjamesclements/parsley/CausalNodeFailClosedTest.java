package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The protocol's fail-closed guards. Each one refuses a situation whose alternative is silent
 * loss or a silent under-claim, and each is stated as a promise in the docs — "blocks or
 * fails, never guesses" (docs/guide/expectations.md#what-parsley-promises). A guard nothing
 * exercises is a guard nobody knows still fires: the simulator only ever drives hosts that
 * behave, so these constructions are deterministic and directed.
 *
 * <p>Every assertion checks the message as well as the type. These failures surface to an
 * operator mid-incident, and the remedy they name is the whole value of failing loudly.
 */
class CausalNodeFailClosedTest {

    private static final UUID TOPIC_C = UUID.nameUUIDFromBytes("fc-c".getBytes());
    private static final UUID TOPIC_D = UUID.nameUUIDFromBytes("fc-d".getBytes());
    private static final UUID TOPIC_SINK = UUID.nameUUIDFromBytes("fc-sink".getBytes());
    private static final UUID TOPIC_OTHER = UUID.nameUUIDFromBytes("fc-other".getBytes());
    private static final Channel C = new Channel(TOPIC_C, 0);
    private static final Channel D = new Channel(TOPIC_D, 0);
    private static final Channel SINK = new Channel(TOPIC_SINK, 0);
    private static final Channel UNDECLARED = new Channel(TOPIC_OTHER, 0);
    private static final UUID SELF = UUID.nameUUIDFromBytes("fc-self".getBytes());

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

    private static NodeConfig config(Set<Channel> consumed, Set<UUID> sinks) {
        return new NodeConfig("fail-closed", SELF, consumed, sinks, 0);
    }

    private static InboundRecord record(Channel c, long offset, VectorClock clock) {
        return new InboundRecord(c, offset, clock, null, -1, new byte[] {1}, new byte[] {2}, 1000L);
    }

    /**
     * A record from a channel the node does not consume has no frontier to advance and no
     * queue to join; delivering it would order nothing and claim nothing. The host has
     * mis-wired its sources, which is a fault, not an input.
     */
    @Test
    void aRecordOnAnUnconsumedChannelFailsClosed() {
        CausalNode node = new CausalNode(config(Set.of(C), Set.of()), new MapStore(), NO_OFFSETS);

        IllegalStateException fed = assertThrows(IllegalStateException.class,
                () -> node.onRecord(record(D, 0, null)),
                "a record on an unconsumed channel must fail, not be silently accepted");
        assertTrue(fed.getMessage().contains("unconsumed channel"),
                "the failure must name the mis-wiring, got: " + fed.getMessage());

        IllegalStateException advanced = assertThrows(IllegalStateException.class,
                () -> node.positionAdvance(D, 5),
                "a position advance on an unconsumed channel must fail too");
        assertTrue(advanced.getMessage().contains("unconsumed channel"),
                "the failure must name the mis-wiring, got: " + advanced.getMessage());
    }

    /**
     * A send to a topic that was never declared a sink is refused at the stamping door. The
     * init end-offset seed only covers declared sinks, so a restart would not claim what this
     * incarnation sent there — the node's own outputs would silently lose their order.
     */
    @Test
    void aSendToAnUndeclaredSinkFailsClosed() {
        CausalNode node = new CausalNode(
                config(Set.of(C), Set.of(TOPIC_SINK)), new MapStore(), NO_OFFSETS);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> node.prepareSend(UNDECLARED),
                "stamping for an undeclared sink must fail: its offsets are never seeded");
        assertTrue(e.getMessage().contains("undeclared sink topic"),
                "the failure must name the undeclared sink, got: " + e.getMessage());
    }

    /**
     * A stamp is per destination channel, because a sequence claim names one. A caller that
     * has not partitioned yet cannot be given a stamp that means anything.
     */
    @Test
    void aSendWithNoDestinationFailsClosed() {
        CausalNode node = new CausalNode(
                config(Set.of(C), Set.of(TOPIC_SINK)), new MapStore(), NO_OFFSETS);

        NullPointerException e = assertThrows(NullPointerException.class,
                () -> node.prepareSend(null),
                "a stamp with no destination channel must fail");
        assertTrue(e.getMessage().contains("partitions before stamping"),
                "the failure must name the host duty it enforces, got: " + e.getMessage());
    }

    /**
     * An input dropped from a stage's declaration while records are still held on it: the
     * held records can be neither delivered (nothing will ever feed that channel again) nor
     * discarded (they are committed, and a cause already claims them). Init refuses, naming
     * both ways out — docs/design/state.md#scope-changes.
     */
    @Test
    void aHeldRecordOnARemovedInputFailsInit() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, NO_OFFSETS);
        assertTrue(first.onRecord(record(D, 0, VectorClock.of(C, 3))).isEmpty(),
                "the record must hold: its C@3 cause has not arrived");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new CausalNode(config(Set.of(C), Set.of()), store, NO_OFFSETS),
                "dropping an input with records still held on it must fail init");
        assertTrue(e.getMessage().contains("held records on removed input"),
                "the failure must say what went wrong, got: " + e.getMessage());
        assertTrue(e.getMessage().contains(TOPIC_D.toString().substring(0, 8) + ":0"),
                "the failure must name the removed input's channel in the same short form the"
                        + " task's log lines use, so an operator can match the two up, got: "
                        + e.getMessage());
        assertTrue(e.getMessage().contains("redeclare the input")
                        && e.getMessage().contains("full reset"),
                "the failure must name both remedies, got: " + e.getMessage());
    }

    /**
     * A hold queue whose entries do not match its own indices is a corrupt restore, not an
     * empty one. Continuing would drop the missing records silently — they are committed,
     * their offsets are past, and nothing would ever fetch them again.
     */
    @Test
    void aHoldQueueMissingAnEntryFailsRestore() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, NO_OFFSETS);
        // One delivery first, so the queue's head index is not zero: the restore walks the
        // half-open window [head, tail), and a window starting at zero cannot tell a wrong
        // bound from a right one.
        assertEquals(1, first.onRecord(record(D, 0, null)).size(), "an unstamped record delivers");
        assertTrue(first.onRecord(record(D, 1, VectorClock.of(C, 3))).isEmpty(), "must hold");
        assertTrue(first.onRecord(record(D, 2, VectorClock.of(C, 3))).isEmpty(), "must hold");

        String lost = store.map.keySet().stream()
                .filter(k -> k.contains("/e/")).reduce((a, b) -> b).orElseThrow();
        store.map.remove(lost); // the second entry vanishes; the queue's indices still claim it

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new CausalNode(config(Set.of(C, D), Set.of()), store, NO_OFFSETS),
                "a hold queue restoring fewer entries than its indices claim must fail");
        assertTrue(e.getMessage().contains("restored 1 of 2 entries"),
                "the failure must name exactly what is missing — the count is head-to-tail,"
                        + " not tail alone, got: " + e.getMessage());
    }

    /** The complement: an intact queue whose head index is not zero restores in full. */
    @Test
    void aPartlyDrainedHoldQueueRestoresEveryRemainingEntry() {
        MapStore store = new MapStore();
        CausalNode first = new CausalNode(config(Set.of(C, D), Set.of()), store, NO_OFFSETS);
        assertEquals(1, first.onRecord(record(D, 0, null)).size(), "an unstamped record delivers");
        assertTrue(first.onRecord(record(D, 1, VectorClock.of(C, 3))).isEmpty(), "must hold");
        assertTrue(first.onRecord(record(D, 2, VectorClock.of(C, 3))).isEmpty(), "must hold");

        CausalNode restarted = new CausalNode(config(Set.of(C, D), Set.of()), store, NO_OFFSETS);
        assertEquals(3, restarted.onRecord(record(C, 3, null)).size(),
                "the cause must release both restored records, and neither may be dropped by an"
                        + " off-by-one in the restore window");
    }
}
