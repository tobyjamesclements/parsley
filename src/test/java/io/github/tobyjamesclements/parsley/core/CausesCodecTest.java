package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
