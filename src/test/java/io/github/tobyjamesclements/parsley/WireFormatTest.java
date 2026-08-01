package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compatibility surface, pinned to literal bytes and literal names.
 *
 * <p>Every other serialization test round-trips through this same implementation, so a
 * coordinated change to both sides — a version bump, a reordered field, a renamed header —
 * passes them all while breaking interoperability with every other Parsley application. These
 * assertions are deliberately redundant with {@code docs/reference/wire-format.md}: the
 * document is the contract, and this test is what makes changing the contract a decision rather
 * than an accident.
 *
 * <p>If one of these fails, the fix is almost never to update the expected bytes. It is to
 * restore the layout — or, if the change is intended, to bump the version, update the document,
 * and treat every deployed application as needing migration.
 */
class WireFormatTest {

    private static final HexFormat HEX = HexFormat.of();

    /** Fixed identities, so every expected byte below is readable straight off the literal. */
    private static final UUID TOPIC = new UUID(0x0102030405060708L, 0x090A0B0C0D0E0F10L);
    private static final UUID SENDER = new UUID(0x1112131415161718L, 0x191A1B1C1D1E1F20L);
    private static final Channel CHANNEL = new Channel(TOPIC, 7);

    /** Version 1, one offset entry (channel at watermark 42), one sequence entry (sender at 9). */
    private static final String CLOCK_HEX =
            "01"                                    // version
            + "00000001"                            // offset entry count
            + "0102030405060708" + "090A0B0C0D0E0F10"  // topic id, msb then lsb
            + "00000007"                            // partition
            + "000000000000002A"                    // offset watermark 42
            + "00000001"                            // sequence entry count
            + "0102030405060708" + "090A0B0C0D0E0F10"  // channel topic id
            + "00000007"                            // channel partition
            + "1112131415161718" + "191A1B1C1D1E1F20"  // sender id, msb then lsb
            + "0000000000000009";                   // sequence watermark 9

    private static VectorClock goldenClock() {
        VectorClock k = new VectorClock();
        k.advanceTo(CHANNEL, 42);
        k.advanceSeq(CHANNEL, SENDER, 9);
        return k;
    }

    /** The serialized clock is exactly the documented layout, byte for byte. */
    @Test
    void clockSerializesToTheDocumentedBytes() {
        assertEquals(CLOCK_HEX, HEX.formatHex(goldenClock().serialize()).toUpperCase(),
                "the clock's byte layout is the cross-application contract; see"
                        + " docs/reference/wire-format.md");
    }

    /** Those same bytes decode back to the same clock, so the reader is pinned too. */
    @Test
    void documentedBytesDeserializeToTheSameClock() {
        VectorClock decoded = VectorClock.deserialize(HEX.parseHex(CLOCK_HEX));
        assertEquals(goldenClock(), decoded,
                "bytes this implementation did not produce must still decode as documented");
        assertEquals(42, decoded.get(CHANNEL), "the offset watermark must survive the wire");
        assertEquals(9, decoded.getSeq(new VectorClock.SeqKey(CHANNEL, SENDER)),
                "the sequence watermark must survive the wire");
    }

    /** The version byte is 1: changing it makes every peer reject this application's records. */
    @Test
    void clockVersionByteIsOne() {
        assertEquals(1, goldenClock().serialize()[0],
                "the wire version is 1; a bump breaks every deployed peer and must be deliberate");
    }

    /** An empty clock is nine bytes: version plus two zero counts, not an absent header. */
    @Test
    void emptyClockSerializesToVersionAndTwoZeroCounts() {
        assertEquals("010000000000000000", HEX.formatHex(new VectorClock().serialize()).toUpperCase(),
                "an empty clock still has both section counts on the wire");
    }

    /** The header names are part of the contract; other applications match on these literals. */
    @Test
    void headerNamesAreTheDocumentedLiterals() {
        assertEquals("vc", CausalHeaders.CLOCK, "the clock header name is part of the wire format");
        assertEquals("vc-sender", CausalHeaders.SENDER, "the sender header name is part of the wire format");
        assertEquals("vc-seq", CausalHeaders.SEQ, "the sequence header name is part of the wire format");
    }

    /** A stamp writes exactly three headers, each with the documented name and byte layout. */
    @Test
    void stampWritesTheDocumentedHeaderBytes() {
        var headers = new RecordHeaders();
        CausalHeaders.write(headers, new SendStamp(goldenClock(), SENDER, 9));

        assertEquals(CLOCK_HEX, HEX.formatHex(headers.lastHeader("vc").value()).toUpperCase(),
                "the vc header carries the serialized clock verbatim");
        assertEquals("1112131415161718191A1B1C1D1E1F20",
                HEX.formatHex(headers.lastHeader("vc-sender").value()).toUpperCase(),
                "vc-sender is 16 bytes: UUID most-significant then least-significant");
        assertEquals("0000000000000009",
                HEX.formatHex(headers.lastHeader("vc-seq").value()).toUpperCase(),
                "vc-seq is 8 bytes, big-endian");
    }

    /**
     * The names Parsley puts on a cluster, pinned. They are a compatibility surface twice
     * over: the tick topic is created by hand before start, so renaming it leaves an
     * application consuming a topic nobody made, and the store names are what Kafka Streams
     * builds the changelog topics and state directories from, so renaming those orphans the
     * committed state of every deployed instance.
     *
     * <p>The shape is Kafka Streams' own, {@code <application.id>-<name>-<kind>}: an operator
     * listing a cluster's topics in name order sees one application's Parsley and Streams
     * topics as a single block. A stray prefix on any of these scatters them again, which is
     * what this pins against.
     */
    @Test
    void ownedNamesFollowTheKafkaStreamsConvention() {
        Topic<String, String> orders = Topic.of("orders", Codec.utf8(), Codec.utf8());
        Stage stage = Stage.named("balances")
                .state(Codec.int64(), () -> 0L)
                .on(orders, (Long n, Message<String, String> m) -> Step.of(n + 1))
                .ticks(Duration.ofSeconds(1), (Long n, Tick t) -> Step.of(n))
                .build();

        assertEquals("payments-balances-ticks", stage.tickTopicName("payments"),
                "the tick topic is qualified by the application id, like every internal topic"
                        + " Kafka Streams creates");
        // Streams wraps the store name as <application.id>-<store>-changelog, so a
        // stage-relative store name lands as payments-balances-state-changelog on the cluster.
        assertEquals("balances-state", stage.protocolStoreName(),
                "the protocol store is named for the stage alone");
        assertEquals("balances-fold", stage.foldStoreName(),
                "the fold store is named for the stage alone");

        assertTrue(Parsley.named("payments", stage).testTopology().describe().toString()
                        .contains("payments-balances-ticks"),
                "and the assembled topology really sources and sinks that name");
    }

    // ---------------------------------------------------------------- state-store layout
    //
    // The store is the other half of what this library writes, and it travels between
    // processes the same way the headers do: a running application's committed state, and its
    // changelog, are read back by the next version of the same application. A renamed key
    // prefix or a reordered envelope field is silently incompatible with everything already
    // on disk — the frontier reads as absent and every record above it is delivered twice.
    // Every other test of these layouts writes and reads through one implementation, so a
    // coordinated change passes them all; these assertions are what make the change a
    // decision. They mirror docs/reference/wire-format.md#state-store-keys.

    /** A second consumed channel, so a record can be gated by an unmet claim on it. */
    private static final UUID CAUSE_TOPIC = new UUID(0x2122232425262728L, 0x292A2B2C2D2E2F30L);
    private static final Channel CAUSE = new Channel(CAUSE_TOPIC, 1);
    private static final UUID SINK_TOPIC = new UUID(0x3132333435363738L, 0x393A3B3C3D3E3F40L);
    private static final Channel SINK = new Channel(SINK_TOPIC, 0);
    private static final Channel FOREIGN =
            new Channel(new UUID(0x4142434445464748L, 0x494A4B4C4D4E4F50L), 3);

    /** The documented text form of a channel in a store key: {@code <uuid>:<partition>}. */
    private static final String CHANNEL_KEY = "01020304-0506-0708-090a-0b0c0d0e0f10:7";

    /**
     * One held record: offset 1, sender-tagged, a one-entry dependency clock, a two-byte key
     * and a one-byte value — every field of docs/reference/wire-format.md#held-record.
     */
    private static final String HELD_HEX =
            "0000000000000001"                      // offset 1
            + "1122334455667788"                    // timestamp
            + "01"                                  // tagged sender
            + "1112131415161718" + "191A1B1C1D1E1F20"  // sender id, msb then lsb
            + "000000000000000A"                    // sender sequence 10
            + "00000025"                            // clock length, 37 bytes
            + "01"                                  // clock: version
            + "00000001"                            //        one offset entry
            + "2122232425262728" + "292A2B2C2D2E2F30"  //        cause topic id
            + "00000001"                            //        cause partition
            + "0000000000000005"                    //        watermark 5
            + "00000000"                            //        no sequence entries
            + "00000002" + "0A0B"                   // key length, key
            + "00000001" + "0C";                    // value length, value

    /** Plain in-memory store: no staging, and it keeps the raw bytes the protocol wrote. */
    private static final class RecordingStore implements StateStore {
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

    /**
     * Drives a node through one of every persisted fact: a delivery (frontier, advertised
     * clock, delivered sequence), a record left held (queue meta and entry), a send (send
     * sequence), and a truncation sweep (carried ancestry).
     */
    private static RecordingStore storeAfterOneOfEverything() {
        RecordingStore store = new RecordingStore();
        CausalNode node = new CausalNode(
                new NodeConfig("wire", SENDER, Set.of(CHANNEL, CAUSE), Set.of(SINK_TOPIC), 7),
                store, NO_OFFSETS);
        // Delivers: its only claim names a channel this node does not consume.
        node.onRecord(new InboundRecord(CHANNEL, 0, VectorClock.of(FOREIGN, 4), SENDER, 9,
                new byte[] {1}, new byte[] {2}, 100L));
        // Holds: claims CAUSE@5, which has not arrived.
        node.onRecord(new InboundRecord(CHANNEL, 1, VectorClock.of(CAUSE, 5), SENDER, 10,
                new byte[] {0x0A, 0x0B}, new byte[] {0x0C}, 0x1122334455667788L));
        node.prepareSend(SINK);
        node.truncate(new VectorClock());
        return store;
    }

    /** Every state-store key is exactly the documented literal, and there are no others. */
    @Test
    void stateStoreKeysAreTheDocumentedLiterals() {
        assertEquals(Set.of(
                        "scope",
                        "ca",
                        "f/" + CHANNEL_KEY,
                        "cc/" + CHANNEL_KEY,
                        "q/" + CHANNEL_KEY + "/m",
                        "q/" + CHANNEL_KEY + "/e/0000000000000001",
                        "sq/31323334-3536-3738-393a-3b3c3d3e3f40:0",
                        "ds/" + CHANNEL_KEY + "/11121314-1516-1718-191a-1b1c1d1e1f20"),
                storeAfterOneOfEverything().map.keySet(),
                "the state-store key layout is what lets the next version of an application"
                        + " read the state this one committed; see"
                        + " docs/reference/wire-format.md#state-store-keys");
    }

    /** A channel's text form in a key is the topic UUID, a colon, and the partition. */
    @Test
    void channelKeyIsUuidColonPartition() {
        assertEquals(CHANNEL_KEY, CHANNEL.key(), "the channel key format is part of every"
                + " per-channel store key");
        assertEquals(CHANNEL, Channel.fromKey(CHANNEL.key()), "the key must parse back");
    }

    /**
     * The queue index is sixteen lower-case hex digits, zero-padded, so the prefix scan that
     * restores a queue walks entries in numeric order.
     */
    @Test
    void queueEntryIndexIsZeroPaddedHex() {
        String key = storeAfterOneOfEverything().map.keySet().stream()
                .filter(k -> k.contains("/e/")).findFirst().orElseThrow();
        String index = key.substring(key.lastIndexOf('/') + 1);
        assertEquals(16, index.length(), "the index is sixteen digits so lexicographic order"
                + " is numeric order");
        assertEquals(index, index.toLowerCase(java.util.Locale.ROOT), "the index is lower case");
        assertEquals(1L, Long.parseLong(index, 16), "the second enqueue takes index 1");
    }

    /** A held record persists in exactly the documented envelope, byte for byte. */
    @Test
    void heldRecordSerializesToTheDocumentedBytes() {
        byte[] held = storeAfterOneOfEverything().map.get("q/" + CHANNEL_KEY + "/e/0000000000000001");
        assertEquals(HELD_HEX, HEX.formatHex(held).toUpperCase(),
                "the held-record envelope is what a restart reads back; see"
                        + " docs/reference/wire-format.md#held-record");
    }

    /**
     * A delivered record's queue entry is removed from the store. Held records are bounded by
     * what is actually waiting, not by everything the node has ever received: left behind,
     * every entry would accumulate in the store and its changelog forever.
     */
    @Test
    void deliveredQueueEntriesLeaveTheStore() {
        var keys = storeAfterOneOfEverything().map.keySet();
        assertFalse(keys.contains("q/" + CHANNEL_KEY + "/e/0000000000000000"),
                "the delivered record's queue entry must be deleted, not merely skipped on"
                        + " restore: it is the only thing bounding the store's growth");
        assertTrue(keys.contains("q/" + CHANNEL_KEY + "/e/0000000000000001"),
                "the record still held must remain");
    }
}
