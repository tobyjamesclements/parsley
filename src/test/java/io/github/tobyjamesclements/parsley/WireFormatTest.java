package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
