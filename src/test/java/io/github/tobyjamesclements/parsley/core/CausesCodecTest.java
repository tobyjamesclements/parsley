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
 * Establishes the frozen wire format of the causal frontier.
 *
 * <p>Encoding is canonical, and decoding rejects rather than salvages. A golden vector
 * assembled by hand from the specification alone guards against the implementation and the
 * document drifting together.
 */
class CausesCodecTest {
    private static final ChannelId CH_A = new ChannelId(new UUID(1, 1), 0);
    private static final ChannelId CH_B = new ChannelId(new UUID(1, 2), 3);

    /** Round trips empty and canonical. */
    @Test
    void roundTripsEmptyAndCanonical() throws Exception {
        byte[] encoded = CausesCodec.encode(Causes.none());
        assertEquals(Causes.none(), CausesCodec.decode(encoded));
        assertArrayEquals(encoded, CausesCodec.encode(CausesCodec.decode(encoded)));
    }

    /** Round trips multiple channels. */
    @Test
    void roundTripsMultipleChannels() throws Exception {
        Causes causes = Causes.of(Map.of(CH_A, 41L, CH_B, 7L));
        byte[] encoded = CausesCodec.encode(causes);
        assertEquals(causes, CausesCodec.decode(encoded));
        assertArrayEquals(encoded, CausesCodec.encode(CausesCodec.decode(encoded)), "encoding must be canonical");
    }

    /** Matches the frozen golden bytes. */
    @Test
    void matchesTheFrozenGoldenBytes() throws Exception {
        ChannelId low = new ChannelId(new UUID(0x0102030405060708L, 0x090A0B0C0D0E0F10L), 2);
        ChannelId highBit = new ChannelId(new UUID(0xF102030405060708L, 0x090A0B0C0D0E0F10L), 0);
        Causes causes = Causes.of(Map.of(low, 41L, highBit, 7L));

        ByteBuffer golden = ByteBuffer.allocate(1 + 4 + 2 * 28);
        golden.put((byte) 1).putInt(2);

        golden.putLong(0x0102030405060708L).putLong(0x090A0B0C0D0E0F10L).putInt(2).putLong(41);
        golden.putLong(0xF102030405060708L).putLong(0x090A0B0C0D0E0F10L).putInt(0).putLong(7);

        assertArrayEquals(golden.array(), CausesCodec.encode(causes));
        assertEquals(causes, CausesCodec.decode(golden.array()));
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
     * Rejects unknown version. With version 2 now the grouped grammar (D98), the first
     * unassigned byte is 3 — pinned alongside a far value so neither a widened accept set
     * nor a salvaging default can stay green.
     */
    @Test
    void rejectsUnknownVersion() {
        byte[] encoded = CausesCodec.encode(Causes.none());
        encoded[0] = 9;
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(encoded));
        encoded[0] = 3;
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(encoded));
    }

    /** Rejects truncation. */
    @Test
    void rejectsTruncation() {
        byte[] encoded = CausesCodec.encode(Causes.of(Map.of(CH_A, 41L)));
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 3);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(truncated));
    }

    /** Rejects trailing bytes. */
    @Test
    void rejectsTrailingBytes() {
        byte[] encoded = CausesCodec.encode(Causes.of(Map.of(CH_A, 41L)));
        byte[] padded = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(padded));
    }

    /** Rejects negative position. */
    @Test
    void rejectsNegativePosition() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 28);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(1);
        CH_A.writeTo(buffer);
        buffer.putLong(-5);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
    }

    /**
     * Rejects the reserved zero topic ID (wire-format constraint 5, D83). The substrate
     * never assigns it to a channel, and once merged it would sit in the frontier as an id
     * no broker query can answer for — a well-framed forged header must not be able to
     * plant it.
     */
    @Test
    void rejectsZeroTopicId() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 28);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(1);
        new ChannelId(new java.util.UUID(0, 0), 0).writeTo(buffer);
        buffer.putLong(7);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "an otherwise well-formed entry naming the zero topic id must be undecodable");
    }

    /** Rejects unsorted or duplicate channels. */
    @Test
    void rejectsUnsortedOrDuplicateChannels() {
        ByteBuffer unsorted = ByteBuffer.allocate(1 + 4 + 56);
        unsorted.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(2);
        CH_B.writeTo(unsorted);
        unsorted.putLong(1);
        CH_A.writeTo(unsorted);
        unsorted.putLong(2);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(unsorted.array()));

        ByteBuffer duplicate = ByteBuffer.allocate(1 + 4 + 56);
        duplicate.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(2);
        CH_A.writeTo(duplicate);
        duplicate.putLong(1);
        CH_A.writeTo(duplicate);
        duplicate.putLong(2);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(duplicate.array()));
    }

    /** Rejects null header value. */
    @Test
    void rejectsNullHeaderValue() {
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(null));
    }

    /** Rejects negative count. */
    @Test
    void rejectsNegativeCount() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(-1);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
    }

    /**
     * A header too short for the read the grammar promises next must surface as the
     * classified {@code UndecodableMetadataException} with the "truncated causes header"
     * diagnosis — never as a raw {@code BufferUnderflowException} escaping decode into the
     * receive path unnamed (Safety 7; D3's strict decode; D8's fail-closed stop). The
     * count/entry length check refuses any longer truncation arithmetically before an entry
     * read can underflow, so the underflow catch's only live entrances are the version byte
     * and the count int — the sub-5-byte headers both probes exercise. Regression caught:
     * deleting the {@code BufferUnderflowException} catch clause lets both probes throw the
     * raw underflow and fails both {@code assertThrows}.
     */
    @Test
    void truncationBelowTheCountIsClassifiedNotARawUnderflow() {
        byte[] midCount = {CausesCodec.FLAT_FORMAT_VERSION, 0, 0};
        CausesCodec.UndecodableMetadataException insideCount = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(midCount),
                "a header ending inside its count int must classify as undecodable, not underflow raw");
        assertEquals("truncated causes header", insideCount.getMessage(),
                "the diagnosis must name the truncation");

        CausesCodec.UndecodableMetadataException empty = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(new byte[0]),
                "a zero-byte header must classify as undecodable, not underflow raw");
        assertEquals("truncated causes header", empty.getMessage(),
                "the diagnosis must name the truncation");
    }

    /**
     * An entry malformed below the codec's own checks — a negative partition, which only
     * {@link ChannelId}'s constructor refuses — must come back classified as
     * "malformed causes header" carrying the constructor's own refusal text, never as the
     * raw {@code IllegalArgumentException} escaping decode (Safety 7; D3; D81's principle
     * that a stop names its condition). Regression caught: deleting the
     * {@code IllegalArgumentException} catch clause lets the constructor's IAE escape raw
     * and fails the {@code assertThrows}.
     */
    @Test
    void malformedEntryBelowTheCodecsOwnChecksIsClassified() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 28);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(1);
        buffer.putLong(1).putLong(1).putInt(-3);
        buffer.putLong(7);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "a negative partition must classify as undecodable, not escape as the constructor's IAE");
        assertTrue(thrown.getMessage().startsWith("malformed causes header: "),
                () -> "the diagnosis must carry the malformed-header classification: " + thrown.getMessage());
        assertTrue(thrown.getMessage().contains("partition must be non-negative: -3"),
                () -> "the diagnosis must carry the constructor's own refusal: " + thrown.getMessage());
    }

    /**
     * A negative declared count must be diagnosed as exactly that. The refusal signature is
     * shared with the count/length mismatch check immediately below it (both throw
     * {@code UndecodableMetadataException}), so this pin is message-level (D81): deleting
     * the negative-count guard still refuses — as "cause count -1 does not match remaining
     * length 0" — and only this diagnosis assertion goes red.
     */
    @Test
    void negativeCountIsDiagnosedAsNegativeCauseCount() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(-1);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "a negative count must be undecodable");
        assertTrue(thrown.getMessage().contains("negative cause count -1"),
                () -> "the diagnosis must name the negative count itself, not a length mismatch: "
                        + thrown.getMessage());
    }

    /**
     * A negative position must be diagnosed by the codec's own per-entry check, naming the
     * position and its channel. The signature is shared with every other refusal in decode,
     * and the entry is backstopped by {@code Causes.of}'s validation through the
     * malformed-header wrapper (see {@code PositionRefusalTest}), so this pin is
     * message-level (D81): deleting the per-entry guard still refuses — wrapped as
     * "malformed causes header: position must be non-negative on ..." — and only this
     * diagnosis assertion goes red.
     */
    @Test
    void negativePositionIsDiagnosedPerEntryNamingItsChannel() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 28);
        buffer.put(CausesCodec.FLAT_FORMAT_VERSION).putInt(1);
        CH_A.writeTo(buffer);
        buffer.putLong(-5);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "a negative position must be undecodable");
        assertTrue(thrown.getMessage().contains("negative position -5 on " + CH_A),
                () -> "the diagnosis must be the per-entry check's own, not the Causes.of backstop's: "
                        + thrown.getMessage());
    }

    private static ByteBuffer grouped(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(CausesCodec.GROUPED_FORMAT_VERSION);
        return buffer;
    }

    /**
     * Matches the frozen version-2 golden bytes. Assembled by hand from the document alone,
     * like the version-1 vector, so the grouped implementation and wire-format.md cannot
     * drift together — and kept beside the version-1 golden, not instead of it, because both
     * grammars stay live on the wire through the migration (D98). The two-partition first
     * group is the point of the grammar: one topic id over two pairs.
     */
    @Test
    void matchesTheFrozenGroupedGoldenBytes() throws Exception {
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

        assertArrayEquals(golden.array(), CausesCodec.encodeGrouped(causes));
        assertEquals(causes, CausesCodec.decode(golden.array()));
    }

    /**
     * The two grammars express the same frontier, and the grouped encoding is canonical.
     * This is the phase-1 compatibility pin (D98): a reader shipped now provably accepts,
     * byte for byte, what the writer flip will produce.
     */
    @Test
    void groupedRoundTripsAndAgreesWithTheFlatGrammar() throws Exception {
        Causes causes = Causes.of(Map.of(CH_A, 41L, CH_B, 7L, new ChannelId(new UUID(1, 1), 6), 3L));
        byte[] encoded = CausesCodec.encodeGrouped(causes);
        assertEquals(causes, CausesCodec.decode(encoded));
        assertArrayEquals(encoded, CausesCodec.encodeGrouped(CausesCodec.decode(encoded)),
                "grouped encoding must be canonical");
        assertEquals(CausesCodec.decode(CausesCodec.encode(causes)), CausesCodec.decode(encoded),
                "both grammars must yield the same frontier");
    }

    /** An empty frontier in the grouped grammar is the version byte and a zero topic count. */
    @Test
    void groupedEmptyFrontierIsTwoBytes() throws Exception {
        byte[] encoded = CausesCodec.encodeGrouped(Causes.none());
        assertArrayEquals(new byte[] {2, 0}, encoded);
        assertEquals(Causes.none(), CausesCodec.decode(encoded));
    }

    /**
     * Pins the varint spelling itself: partition 300 is {@code 0xAC 0x02} — seven payload
     * bits per byte, lowest bits first, the high bit set on every byte but the last — as
     * wire-format.md defines it. A big-endian or padded spelling fails the array equality.
     */
    @Test
    void groupedVarintSpellsMultiByteValuesLowBitsFirst() throws Exception {
        Causes causes = Causes.of(Map.of(new ChannelId(new UUID(1, 1), 300), 7L));
        ByteBuffer expected = grouped(1 + 1 + 16 + 1 + 2 + 8);
        expected.put((byte) 1);
        expected.putLong(1).putLong(1).put((byte) 1);
        expected.put((byte) 0xAC).put((byte) 0x02).putLong(7);
        assertArrayEquals(expected.array(), CausesCodec.encodeGrouped(causes));
        assertEquals(causes, CausesCodec.decode(expected.array()));
    }

    /** Rejects grouped topics out of order or duplicated: each topic appears at most once, ascending. */
    @Test
    void rejectsGroupedTopicsOutOfOrderOrDuplicate() {
        ByteBuffer descending = grouped(1 + 1 + 2 * 26);
        descending.put((byte) 2);
        descending.putLong(1).putLong(2).put((byte) 1).put((byte) 3).putLong(1);
        descending.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(2);
        CausesCodec.UndecodableMetadataException outOfOrder = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(descending.array()));
        assertTrue(outOfOrder.getMessage().contains("topics not strictly ascending"),
                () -> "the diagnosis must name the group order rule: " + outOfOrder.getMessage());

        ByteBuffer duplicate = grouped(1 + 1 + 2 * 26);
        duplicate.put((byte) 2);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 1).putLong(2);
        CausesCodec.UndecodableMetadataException duplicated = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(duplicate.array()));
        assertTrue(duplicated.getMessage().contains("topics not strictly ascending"),
                () -> "a repeated topic must fall to the same order rule: " + duplicated.getMessage());
    }

    /** Rejects grouped partitions out of order or duplicated within their topic's group. */
    @Test
    void rejectsGroupedPartitionsOutOfOrderOrDuplicate() {
        ByteBuffer descending = grouped(1 + 1 + 16 + 1 + 2 * 9);
        descending.put((byte) 1);
        descending.putLong(1).putLong(1).put((byte) 2);
        descending.put((byte) 5).putLong(1);
        descending.put((byte) 2).putLong(2);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(descending.array()));
        assertTrue(thrown.getMessage().contains("partitions not strictly ascending"),
                () -> "the diagnosis must name the intra-group order rule: " + thrown.getMessage());

        ByteBuffer duplicate = grouped(1 + 1 + 16 + 1 + 2 * 9);
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
    void rejectsGroupedZeroPartitionTopic() {
        ByteBuffer buffer = grouped(1 + 1 + 16 + 1);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 0);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
        assertTrue(thrown.getMessage().contains("zero partitions"),
                () -> "the diagnosis must name the empty group: " + thrown.getMessage());
    }

    /**
     * Rejects the reserved zero topic ID in the grouped grammar — version 2's constraint 6,
     * the same refusal as the flat grammar's constraint 5 (D83), checked once per group.
     */
    @Test
    void rejectsGroupedZeroTopicId() {
        ByteBuffer buffer = grouped(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(0).putLong(0).put((byte) 1).put((byte) 0).putLong(7);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "an otherwise well-formed group naming the zero topic id must be undecodable");
        assertTrue(thrown.getMessage().contains("zero topic id at group"),
                () -> "the diagnosis must be the reserved-id refusal's own: " + thrown.getMessage());
    }

    /** Rejects a negative position in the grouped grammar, named per entry as in the flat one. */
    @Test
    void rejectsGroupedNegativePosition() {
        ByteBuffer buffer = grouped(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(-5);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()));
        assertTrue(thrown.getMessage().contains("negative position -5 on "),
                () -> "the diagnosis must name the position and its channel: " + thrown.getMessage());
    }

    /**
     * Rejects grouped truncation and trailing bytes. Truncation inside a position, inside a
     * topic id and inside a varint all classify as the truncated-header diagnosis, never a
     * raw underflow; a surplus byte after the last group is trailing, so the exact byte
     * length stays part of the grammar even though it is no longer computable from a count.
     */
    @Test
    void rejectsGroupedTruncationAndTrailingBytes() {
        byte[] encoded = CausesCodec.encodeGrouped(
                Causes.of(Map.of(CH_A, 41L, CH_B, 7L)));

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

        byte[] midVarint = {CausesCodec.GROUPED_FORMAT_VERSION, (byte) 0xAC};
        CausesCodec.UndecodableMetadataException insideVarint = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(midVarint));
        assertEquals("truncated causes header", insideVarint.getMessage(),
                "truncation inside a varint must classify, not underflow raw");

        byte[] padded = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        CausesCodec.UndecodableMetadataException trailing = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(padded));
        assertTrue(trailing.getMessage().contains("trailing bytes"),
                () -> "the diagnosis must name the surplus: " + trailing.getMessage());
    }

    /**
     * Rejects grouped count miscounts: topic counts and partition counts must match the
     * bytes exactly. Each probe mutates one count byte of real encoder output — the file's
     * encode-then-mutate pattern — so the vectors cannot drift from the grammar and each
     * refusal is pinned to exactly that one delta. At these values every count is a
     * one-byte varint: the topic count at offset 1, the first group's partition count at
     * offset 18.
     */
    @Test
    void rejectsGroupedCountMiscounts() {
        byte[] oneTopic = CausesCodec.encodeGrouped(Causes.of(Map.of(CH_A, 1L)));
        byte[] topicsOverstated = oneTopic.clone();
        topicsOverstated[1] = 2;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(topicsOverstated));

        byte[] twoTopics = CausesCodec.encodeGrouped(Causes.of(Map.of(CH_A, 1L, CH_B, 2L)));
        byte[] topicsUnderstated = twoTopics.clone();
        topicsUnderstated[1] = 1;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(topicsUnderstated));

        byte[] partitionsOverstated = oneTopic.clone();
        partitionsOverstated[18] = 2;
        assertThrows(CausesCodec.UndecodableMetadataException.class,
                () -> CausesCodec.decode(partitionsOverstated));

        byte[] twoPartitions = CausesCodec.encodeGrouped(
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
        ByteBuffer paddedTopicCount = grouped(1 + 2 + 26);
        paddedTopicCount.put((byte) 0x81).put((byte) 0x00);
        paddedTopicCount.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(paddedTopicCount.array()));
        assertTrue(thrown.getMessage().contains("non-minimal varint"),
                () -> "the diagnosis must name the padding: " + thrown.getMessage());

        ByteBuffer paddedPartition = grouped(1 + 1 + 16 + 1 + 2 + 8);
        paddedPartition.put((byte) 1);
        paddedPartition.putLong(1).putLong(1).put((byte) 1);
        paddedPartition.put((byte) 0x85).put((byte) 0x00).putLong(1);
        CausesCodec.UndecodableMetadataException padded = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(paddedPartition.array()));
        assertTrue(padded.getMessage().contains("non-minimal varint"),
                () -> "the diagnosis must name the padding, not decode partition 5: " + padded.getMessage());
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
            ByteBuffer buffer = grouped(1 + probe.length + 26);
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
