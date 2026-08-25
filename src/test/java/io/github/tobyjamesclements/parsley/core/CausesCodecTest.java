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

    /** Rejects unknown version. */
    @Test
    void rejectsUnknownVersion() {
        byte[] encoded = CausesCodec.encode(Causes.none());
        encoded[0] = 9;
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(1);
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(1);
        new ChannelId(new java.util.UUID(0, 0), 0).writeTo(buffer);
        buffer.putLong(7);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "an otherwise well-formed entry naming the zero topic id must be undecodable");
    }

    /** Rejects unsorted or duplicate channels. */
    @Test
    void rejectsUnsortedOrDuplicateChannels() {
        ByteBuffer unsorted = ByteBuffer.allocate(1 + 4 + 56);
        unsorted.put(CausesCodec.FORMAT_VERSION).putInt(2);
        CH_B.writeTo(unsorted);
        unsorted.putLong(1);
        CH_A.writeTo(unsorted);
        unsorted.putLong(2);
        assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(unsorted.array()));

        ByteBuffer duplicate = ByteBuffer.allocate(1 + 4 + 56);
        duplicate.put(CausesCodec.FORMAT_VERSION).putInt(2);
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(-1);
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
        byte[] midCount = {CausesCodec.FORMAT_VERSION, 0, 0};
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(1);
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(-1);
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
        buffer.put(CausesCodec.FORMAT_VERSION).putInt(1);
        CH_A.writeTo(buffer);
        buffer.putLong(-5);
        CausesCodec.UndecodableMetadataException thrown = assertThrows(
                CausesCodec.UndecodableMetadataException.class, () -> CausesCodec.decode(buffer.array()),
                "a negative position must be undecodable");
        assertTrue(thrown.getMessage().contains("negative position -5 on " + CH_A),
                () -> "the diagnosis must be the per-entry check's own, not the Causes.of backstop's: "
                        + thrown.getMessage());
    }
}
