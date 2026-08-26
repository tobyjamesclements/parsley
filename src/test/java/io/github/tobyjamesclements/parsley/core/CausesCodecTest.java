package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the frozen wire format of the causal frontier: entries grouped by topic,
 * structural fields as minimal varints, positions fixed-width (D98).
 *
 * <p>Encoding is canonical, and decoding rejects rather than salvages. A golden vector
 * assembled by hand from the specification alone guards against the implementation and the
 * document drifting together.
 */
class CausesCodecTest {
    private static final ChannelId CH_A = new ChannelId(new UUID(1, 1), 0);
    private static final ChannelId CH_B = new ChannelId(new UUID(1, 2), 3);

    private static ByteBuffer header(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(CausesCodec.FORMAT_VERSION);
        return buffer;
    }

    /** Round trips empty and canonical. */
    @Test
    void roundTripsEmptyAndCanonical() throws Exception {
        byte[] encoded = CausesCodec.encode(Causes.none());
        assertEquals(Causes.none(), CausesCodec.decode(encoded));
        assertArrayEquals(encoded, CausesCodec.encode(CausesCodec.decode(encoded)));
    }

    /** An empty frontier is the version byte and a zero topic count, nothing else. */
    @Test
    void emptyFrontierIsTwoBytes() throws Exception {
        byte[] encoded = CausesCodec.encode(Causes.none());
        assertArrayEquals(new byte[] {2, 0}, encoded);
        assertEquals(Causes.none(), CausesCodec.decode(encoded));
    }

    /** Round trips multiple channels, including two partitions sharing one topic group. */
    @Test
    void roundTripsMultipleChannels() throws Exception {
        Causes causes = Causes.of(Map.of(CH_A, 41L, CH_B, 7L, new ChannelId(new UUID(1, 1), 6), 3L));
        byte[] encoded = CausesCodec.encode(causes);
        assertEquals(causes, CausesCodec.decode(encoded));
        assertArrayEquals(encoded, CausesCodec.encode(CausesCodec.decode(encoded)), "encoding must be canonical");
    }

    /**
     * Matches the frozen golden bytes. Assembled by hand from the document alone, so the
     * implementation and wire-format.md cannot drift together. The two-partition first
     * group is the point of the grammar — one topic id over two pairs — and the high-bit
     * second topic pins the unsigned group order.
     */
    @Test
    void matchesTheFrozenGoldenBytes() throws Exception {
        UUID low = new UUID(0x0102030405060708L, 0x090A0B0C0D0E0F10L);
        UUID highBit = new UUID(0xF102030405060708L, 0x090A0B0C0D0E0F10L);
        Causes causes = Causes.of(Map.of(
                new ChannelId(low, 2), 41L,
                new ChannelId(low, 5), 7L,
                new ChannelId(highBit, 0), 9L));

        ByteBuffer golden = ByteBuffer.allocate(1 + 1 + 16 + 1 + 2 * 9 + 16 + 1 + 9);
        golden.put((byte) 2).put((byte) 2);
        golden.putLong(0x0102030405060708L).putLong(0x090A0B0C0D0E0F10L).put((byte) 2);
        golden.put((byte) 2).putLong(41);
        golden.put((byte) 5).putLong(7);
        golden.putLong(0xF102030405060708L).putLong(0x090A0B0C0D0E0F10L).put((byte) 1);
        golden.put((byte) 0).putLong(9);

        assertArrayEquals(golden.array(), CausesCodec.encode(causes));
        assertEquals(causes, CausesCodec.decode(golden.array()));
    }

    /**
     * Pins the varint spelling itself: partition 300 is {@code 0xAC 0x02} — seven payload
     * bits per byte, lowest bits first, the high bit set on every byte but the last — as
     * wire-format.md defines it. A big-endian or padded spelling fails the array equality.
     */
    @Test
    void varintSpellsMultiByteValuesLowBitsFirst() throws Exception {
        Causes causes = Causes.of(Map.of(new ChannelId(new UUID(1, 1), 300), 7L));
        ByteBuffer expected = header(1 + 1 + 16 + 1 + 2 + 8);
        expected.put((byte) 1);
        expected.putLong(1).putLong(1).put((byte) 1);
        expected.put((byte) 0xAC).put((byte) 0x02).putLong(7);
        assertArrayEquals(expected.array(), CausesCodec.encode(causes));
        assertEquals(causes, CausesCodec.decode(expected.array()));
    }

    /** Channel order is the unsigned order of the encoding. */
    @Test
    void channelOrderIsTheUnsignedOrderOfTheEncoding() {
        ChannelId highBit = new ChannelId(new UUID(0x8000000000000000L, 0), 0);
        ChannelId low = new ChannelId(new UUID(1, 0), 0);

        org.junit.jupiter.api.Assertions.assertTrue(low.compareTo(highBit) < 0);
        org.junit.jupiter.api.Assertions.assertTrue(
                java.util.Arrays.compareUnsigned(low.toBytes(), highBit.toBytes()) < 0);
    }

    /**
     * Rejects unknown version. Version byte 1 named a pre-release flat grammar no released
     * message ever carried; it is retired and refused like any unknown byte, pinned here
     * beside the first unassigned value (3) and a far one (9) so neither a widened accept
     * set nor a salvaging default can stay green.
     */
    @Test
    void rejectsUnknownVersion() {
        byte[] encoded = CausesCodec.encode(Causes.none());
        for (byte version : new byte[] {1, 3, 9}) {
            encoded[0] = version;
            assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(encoded),
                    "version byte " + version + " must be undecodable");
        }
    }

    /** Rejects null header value. */
    @Test
    void rejectsNullHeaderValue() {
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(null));
    }

    /**
     * Rejects truncation and trailing bytes. Truncation inside a position, inside a topic
     * id, inside a varint and below the version byte all classify as the truncated-header
     * diagnosis — never a raw {@code BufferUnderflowException} escaping decode into the
     * receive path unnamed (Safety 7; D3's strict decode; D8's fail-closed stop) — and a
     * surplus byte after the last group is trailing, so the exact byte length stays part of
     * the grammar even though it is no longer computable from a count. Regression caught:
     * deleting the underflow catch clause lets the truncation probes throw raw.
     */
    @Test
    void rejectsTruncationAndTrailingBytes() {
        byte[] encoded = CausesCodec.encode(Causes.of(Map.of(CH_A, 41L, CH_B, 7L)));

        byte[] midPosition = java.util.Arrays.copyOf(encoded, encoded.length - 3);
        CausesCodec.UndecodableMetadataException insidePosition = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(midPosition));
        assertEquals("truncated causes header", insidePosition.getMessage(),
                "truncation inside a position must classify, not underflow raw");

        byte[] midTopicId = java.util.Arrays.copyOf(encoded, encoded.length - 20);
        CausesCodec.UndecodableMetadataException insideTopicId = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(midTopicId));
        assertEquals("truncated causes header", insideTopicId.getMessage(),
                "truncation inside a topic id must classify, not underflow raw");

        byte[] midVarint = {CausesCodec.FORMAT_VERSION, (byte) 0xAC};
        CausesCodec.UndecodableMetadataException insideVarint = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(midVarint));
        assertEquals("truncated causes header", insideVarint.getMessage(),
                "truncation inside a varint must classify, not underflow raw");

        CausesCodec.UndecodableMetadataException empty = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(new byte[0]),
                "a zero-byte header must classify as undecodable, not underflow raw");
        assertEquals("truncated causes header", empty.getMessage(),
                "the diagnosis must name the truncation");

        byte[] padded = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        CausesCodec.UndecodableMetadataException trailing = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(padded));
        assertTrue(trailing.getMessage().contains("trailing bytes"),
                () -> "the diagnosis must name the surplus: " + trailing.getMessage());
    }

    /**
     * Rejects a negative position, diagnosed per entry naming its channel. Backstopped by
     * {@code Causes.of}'s own refusal through the malformed-header wrapper (see
     * {@code PositionRefusalTest}), so this pin is message-level (D81): deleting the
     * decoder's guard still refuses through the backstop, and only the diagnosis assertion
     * goes red.
     */
    @Test
    void rejectsNegativePosition() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(-5);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
        assertTrue(thrown.getMessage().contains("negative position -5 on "),
                () -> "the diagnosis must be the decoder's own, naming position and channel: " + thrown.getMessage());
    }

    /**
     * Rejects the reserved zero topic ID (wire-format constraint 5, D83), checked once per
     * group. The substrate never assigns it to a channel, and once merged it would sit in
     * the frontier as an id no broker query can answer for — a well-framed forged header
     * must not be able to plant it.
     */
    @Test
    void rejectsZeroTopicId() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(0).putLong(0).put((byte) 1).put((byte) 0).putLong(7);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "an otherwise well-formed group naming the zero topic id must be undecodable");
        assertTrue(thrown.getMessage().contains("zero topic id at group"),
                () -> "the diagnosis must be the reserved-id refusal's own: " + thrown.getMessage());
    }

    /** Rejects topics out of order or duplicated: each topic appears at most once, ascending unsigned. */
    @Test
    void rejectsTopicsOutOfOrderOrDuplicate() {
        ByteBuffer descending = header(1 + 1 + 2 * 26);
        descending.put((byte) 2);
        descending.putLong(1).putLong(2).put((byte) 1).put((byte) 3).putLong(1);
        descending.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(2);
        CausesCodec.UndecodableMetadataException outOfOrder = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(descending.array()));
        assertTrue(outOfOrder.getMessage().contains("topics not strictly ascending"),
                () -> "the diagnosis must name the group order rule: " + outOfOrder.getMessage());

        ByteBuffer duplicate = header(1 + 1 + 2 * 26);
        duplicate.put((byte) 2);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 1).putLong(2);
        CausesCodec.UndecodableMetadataException duplicated = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(duplicate.array()));
        assertTrue(duplicated.getMessage().contains("topics not strictly ascending"),
                () -> "a repeated topic must fall to the same order rule: " + duplicated.getMessage());
    }

    /** Rejects partitions out of order or duplicated within their topic's group. */
    @Test
    void rejectsPartitionsOutOfOrderOrDuplicate() {
        ByteBuffer descending = header(1 + 1 + 16 + 1 + 2 * 9);
        descending.put((byte) 1);
        descending.putLong(1).putLong(1).put((byte) 2);
        descending.put((byte) 5).putLong(1);
        descending.put((byte) 2).putLong(2);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(descending.array()));
        assertTrue(thrown.getMessage().contains("partitions not strictly ascending"),
                () -> "the diagnosis must name the intra-group order rule: " + thrown.getMessage());

        ByteBuffer duplicate = header(1 + 1 + 16 + 1 + 2 * 9);
        duplicate.put((byte) 1);
        duplicate.putLong(1).putLong(1).put((byte) 2);
        duplicate.put((byte) 2).putLong(1);
        duplicate.put((byte) 2).putLong(2);
        CausesCodec.UndecodableMetadataException duplicated = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(duplicate.array()));
        assertTrue(duplicated.getMessage().contains("partitions not strictly ascending"),
                () -> "a repeated partition must fall to the same order rule: " + duplicated.getMessage());
    }

    /** Rejects a topic group naming zero partitions: an empty group can carry no cause. */
    @Test
    void rejectsZeroPartitionTopic() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 0);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
        assertTrue(thrown.getMessage().contains("zero partitions"),
                () -> "the diagnosis must name the empty group: " + thrown.getMessage());
    }

    /**
     * Rejects count miscounts: topic counts and partition counts must match the bytes
     * exactly. Each probe mutates one count byte of real encoder output — the file's
     * encode-then-mutate pattern — so the vectors cannot drift from the grammar and each
     * refusal is pinned to exactly that one delta. At these values every count is a
     * one-byte varint: the topic count at offset 1, the first group's partition count at
     * offset 18.
     */
    @Test
    void rejectsCountMiscounts() {
        byte[] oneTopic = CausesCodec.encode(Causes.of(Map.of(CH_A, 1L)));
        byte[] topicsOverstated = oneTopic.clone();
        topicsOverstated[1] = 2;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(topicsOverstated));

        byte[] twoTopics = CausesCodec.encode(Causes.of(Map.of(CH_A, 1L, CH_B, 2L)));
        byte[] topicsUnderstated = twoTopics.clone();
        topicsUnderstated[1] = 1;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(topicsUnderstated));

        byte[] partitionsOverstated = oneTopic.clone();
        partitionsOverstated[18] = 2;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(partitionsOverstated));

        byte[] twoPartitions = CausesCodec.encode(
                Causes.of(Map.of(CH_A, 1L, new ChannelId(CH_A.topicId(), 6), 2L)));
        byte[] partitionsUnderstated = twoPartitions.clone();
        partitionsUnderstated[18] = 1;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(partitionsUnderstated));
    }

    /**
     * Rejects non-minimal varints. A padded spelling — 1 as {@code 0x81 0x00}, 5 as
     * {@code 0x85 0x00} — decodes to the same value through a salvaging reader, so two byte
     * strings would mean one frontier and the document's byte-for-byte uniqueness promise
     * would silently break. Pinned on both varint positions: the topic count and a
     * partition id.
     */
    @Test
    void rejectsNonMinimalVarint() {
        ByteBuffer paddedTopicCount = header(1 + 2 + 26);
        paddedTopicCount.put((byte) 0x81).put((byte) 0x00);
        paddedTopicCount.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(paddedTopicCount.array()));
        assertTrue(thrown.getMessage().contains("non-minimal varint"),
                () -> "the diagnosis must name the padding: " + thrown.getMessage());

        ByteBuffer paddedPartition = header(1 + 1 + 16 + 1 + 2 + 8);
        paddedPartition.put((byte) 1);
        paddedPartition.putLong(1).putLong(1).put((byte) 1);
        paddedPartition.put((byte) 0x85).put((byte) 0x00).putLong(1);
        CausesCodec.UndecodableMetadataException padded = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(paddedPartition.array()));
        assertTrue(padded.getMessage().contains("non-minimal varint"),
                () -> "the diagnosis must name the padding, not decode partition 5: " + padded.getMessage());
    }

    /**
     * Refuses every vector in the shared malformation battery. The named tests above pin
     * each class with its own narrative and exact diagnoses; this sweep pins the codec to
     * {@link CausesMalformationVectors}, the catalogue the session companion's mirror
     * battery also runs, so a malformation class added there is enforced on both decoders
     * with no further test change (D99's mirror, made structural).
     */
    @Test
    void refusesEveryCataloguedMalformation() {
        for (CausesMalformationVectors.Vector vector : CausesMalformationVectors.all()) {
            CausesCodec.UndecodableMetadataException thrown = assertThrows(
                    CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(vector.bytes()),
                    () -> vector.family() + ": " + vector.label() + " must be undecodable");
            assertTrue(thrown.getMessage().contains(vector.diagnosisFragment()),
                    () -> vector.family() + ": " + vector.label() + " must diagnose \""
                            + vector.diagnosisFragment() + "\": " + thrown.getMessage());
        }
    }

    /**
     * Rejects varints past the non-negative int range. Java's shift discards bits past 31,
     * so a five-byte varint whose terminal byte carries only overflowed bits —
     * {@code 85 80 80 80 10} — would silently decode to the same value as {@code 05} in a
     * salvaging reader: aliasing the padding check cannot see. A terminal byte reaching the
     * sign bit ({@code 08}) and a sixth byte ({@code 80} continuing) are the same refusal.
     * Regression caught: dropping the terminal-byte guard admits the alias and turns only
     * the first probe green through a different diagnosis.
     */
    @Test
    void rejectsVarintBeyondTheNonNegativeIntRange() {
        byte[][] probes = {
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01},
        };
        for (byte[] probe : probes) {
            ByteBuffer buffer = header(1 + probe.length + 26);
            buffer.put(probe);
            buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
            CausesCodec.UndecodableMetadataException thrown = assertThrows(
                    CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                    "a varint past the int range must be undecodable");
            assertTrue(thrown.getMessage().contains("exceeds the non-negative int range"),
                    () -> "the diagnosis must name the overflow, not alias or misparse: " + thrown.getMessage());
        }
    }
}
