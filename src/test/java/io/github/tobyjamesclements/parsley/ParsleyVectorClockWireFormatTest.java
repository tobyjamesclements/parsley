package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden-bytes pin of the {@code parsley-causal-clock} wire format, byte for byte, against the
 * layout published in {@code docs/internals/wire-format.md}: big-endian throughout,
 * {@code [version:1 = 0x01][count:4]} then per entry
 * {@code [topicId MSB:8][topicId LSB:8][partition:4][offset:8]} — {@code 5 + 28 × n} bytes total.
 *
 * <p>Unlike the round-trip test in {@link ParsleyVectorClockTest}, which stays green under any
 * self-consistent refactor of the encoding, these assertions compare against hand-assembled byte
 * literals. External implementers are invited to encode this format independently, and records
 * carrying it are in flight during a rolling restart, so any layout change — field order, field
 * width, endianness, the version byte — must fail here. If it was intentional, it is a wire-format
 * break: bump the version byte and update {@code docs/internals/wire-format.md} together with this
 * test.
 *
 * <p>Entry <em>order</em> within a multi-entry clock is deliberately unspecified (the encoder walks
 * JDK immutable maps, whose iteration order is salted per JVM run), and the documented format
 * promises none — so the multi-entry pins assert the exact 5-byte header plus the exact
 * <em>set</em> of 28-byte entry slices, and decoding is asserted to accept entries in any order.
 */
class ParsleyVectorClockWireFormatTest {

    private static final int HEADER_LENGTH = 5;
    private static final int ENTRY_LENGTH = 28;

    /** Byte values chosen so every field of the encoded entry has a distinct, position-revealing pattern. */
    private static final Uuid C1_ID = new Uuid(0x0102030405060708L, 0x1112131415161718L);
    private static final Uuid C2_ID = new Uuid(0x4142434445464748L, 0x5152535455565758L);
    private static final int C1_PARTITION = 0x21222324;
    private static final long C1_OFFSET = 0x3132333435363738L;

    /** Hand-assembled encoding of a single-entry clock at {@code (C1_ID, C1_PARTITION, C1_OFFSET)}. */
    private static final byte[] SINGLE_ENTRY_BYTES = {
            0x01,                                            // version
            0x00, 0x00, 0x00, 0x01,                          // count = 1
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // topicId most-significant bits
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // topicId least-significant bits
            0x21, 0x22, 0x23, 0x24,                          // partition
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // offset
    };

    /**
     * An empty clock encodes to exactly the five header bytes: version {@code 0x01} and a zero
     * entry count. This is the doc's {@code 5 + 28 × n} size claim at {@code n = 0}.
     *
     * Asserts the encoded bytes equal the pinned five-byte literal.
     */
    @Test
    void emptyClockEncodesToPinnedHeaderBytes() {
        assertArrayEquals(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00},
                ParsleyVectorClock.empty().toBytes(),
                "an empty clock must encode as version 0x01 followed by a big-endian zero count");
    }

    /**
     * A single-entry clock encodes to the pinned 33-byte literal: header, then topicId MSB,
     * topicId LSB, partition, offset — each big-endian, in exactly that order. The entry values
     * use distinct byte patterns, so swapping any two fields, changing a field's width, or
     * flipping endianness produces a different array.
     *
     * Asserts the encoded bytes equal the hand-assembled literal.
     */
    @Test
    void singleEntryClockEncodesToPinnedBytes() {
        ParsleyVectorClock clock = ParsleyVectorClock.empty().observe(C1_ID, C1_PARTITION, C1_OFFSET);
        assertArrayEquals(SINGLE_ENTRY_BYTES, clock.toBytes(),
                "a single-entry clock must encode as [version][count=1][MSB:8][LSB:8][partition:4][offset:8], big-endian");
    }

    /**
     * A multi-entry clock encodes as the pinned header followed by one pinned 28-byte slice per
     * entry — two partitions of one topic and one partition of another, covering per-topic
     * fan-out and a beyond-32-bit offset. Entry order is unspecified (salted map iteration), so
     * the slices are compared as a set; everything inside each slice is still pinned exactly.
     *
     * Asserts the total length is {@code 5 + 28 × 3}, the header bytes are exact, and the set of
     * entry slices equals the hand-assembled expected set.
     */
    @Test
    void multiEntryClockEncodesEachEntryAsPinnedSlice() {
        ParsleyVectorClock clock = ParsleyVectorClock.empty()
                .observe(C1_ID, 0, 5L)
                .observe(C1_ID, 1, 7L)
                .observe(C2_ID, 9, 0x0000000100000000L);

        byte[] bytes = clock.toBytes();

        assertEquals(HEADER_LENGTH + 3 * ENTRY_LENGTH, bytes.length,
                "a three-entry clock must occupy exactly 5 + 28 × 3 bytes");
        assertArrayEquals(new byte[] {0x01, 0x00, 0x00, 0x00, 0x03},
                Arrays.copyOfRange(bytes, 0, HEADER_LENGTH),
                "the header must be version 0x01 followed by a big-endian count of 3");

        Set<String> expectedSlices = Set.of(
                hex(pinnedEntry(C1_ID, 0, 5L)),
                hex(pinnedEntry(C1_ID, 1, 7L)),
                hex(pinnedEntry(C2_ID, 9, 0x0000000100000000L)));
        assertEquals(expectedSlices, entrySlices(bytes),
                "each encoded entry must match its hand-assembled 28-byte slice (in any order)");
    }

    /**
     * The pinned single-entry literal decodes to the expected clock, independently of what
     * {@code toBytes()} produces — pinning the decode direction against literals a plain-client
     * implementation would emit.
     *
     * Asserts the decoded clock equals the expected clock and reports the pinned offset for the
     * pinned coordinate.
     */
    @Test
    void pinnedSingleEntryBytesDecodeToExpectedClock() {
        ParsleyVectorClock decoded = ParsleyVectorClock.fromBytes(SINGLE_ENTRY_BYTES);
        assertEquals(ParsleyVectorClock.empty().observe(C1_ID, C1_PARTITION, C1_OFFSET), decoded,
                "the pinned single-entry literal must decode to the clock it was assembled from");
        assertEquals(C1_OFFSET, decoded.offsetFor(C1_ID, C1_PARTITION),
                "the decoded clock must report the pinned offset at the pinned coordinate");
    }

    /**
     * Decoding accepts entries in any order: two hand-assembled literals holding the same two
     * entries in opposite orders decode to equal clocks. The documented format promises no entry
     * order, so an external encoder may emit entries however it likes.
     *
     * Asserts both orderings decode to the same clock.
     */
    @Test
    void decodingAcceptsEntriesInAnyOrder() {
        byte[] entryA = pinnedEntry(C1_ID, 0, 5L);
        byte[] entryB = pinnedEntry(C2_ID, 9, 7L);

        ParsleyVectorClock abOrder = ParsleyVectorClock.fromBytes(concat(new byte[] {0x01, 0x00, 0x00, 0x00, 0x02}, entryA, entryB));
        ParsleyVectorClock baOrder = ParsleyVectorClock.fromBytes(concat(new byte[] {0x01, 0x00, 0x00, 0x00, 0x02}, entryB, entryA));

        ParsleyVectorClock expected = ParsleyVectorClock.empty().observe(C1_ID, 0, 5L).observe(C2_ID, 9, 7L);
        assertEquals(expected, abOrder, "entries in A,B order must decode to the expected clock");
        assertEquals(expected, baOrder, "entries in B,A order must decode to the same clock");
    }

    /**
     * The doc states deserialization throws {@code IllegalStateException} on a version mismatch.
     * A leading byte other than {@code 0x01} — here a hypothetical {@code 0x02} on an otherwise
     * valid empty encoding — must be rejected, never decoded on a guess.
     *
     * Asserts an {@code IllegalStateException} naming the unsupported version.
     */
    @Test
    void unsupportedWireVersionIsRejected() {
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyVectorClock.fromBytes(new byte[] {0x02, 0x00, 0x00, 0x00, 0x00}),
                "a wire version other than 0x01 must be rejected");
        assertTrue(thrown.getMessage().contains("version"),
                "the rejection must name the version mismatch, got: " + thrown.getMessage());
    }

    /**
     * The public {@link CausalClock} facade shares this exact encoding (the doc's opening claim):
     * a facade-built clock encodes to the same pinned literal, and the facade decodes the pinned
     * literal back to an equal instance. This is the surface external implementers actually
     * interoperate with.
     *
     * Asserts {@code CausalClock.toBytes()} equals the pinned literal and
     * {@code CausalClock.fromBytes} of the literal equals the built instance.
     */
    @Test
    void causalClockFacadeSharesThePinnedEncoding() {
        CausalClock built = CausalClock.builder(Map.of("C1", C1_ID))
                .require("C1", C1_PARTITION, C1_OFFSET)
                .build();
        assertArrayEquals(SINGLE_ENTRY_BYTES, built.toBytes(),
                "the public CausalClock facade must produce the identical pinned encoding");
        assertEquals(built, CausalClock.fromBytes(SINGLE_ENTRY_BYTES),
                "the public CausalClock facade must decode the pinned literal to an equal clock");
    }

    /**
     * Hand-assembles one 28-byte entry — {@code [MSB:8][LSB:8][partition:4][offset:8]},
     * explicitly big-endian — independently of the production encoder.
     */
    private static byte[] pinnedEntry(Uuid topicId, int partition, long offset) {
        return ByteBuffer.allocate(ENTRY_LENGTH)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .putLong(offset)
                .array();
    }

    /** Splits an encoded clock past its 5-byte header into hex-rendered 28-byte entry slices. */
    private static Set<String> entrySlices(byte[] encoded) {
        assertEquals(0, (encoded.length - HEADER_LENGTH) % ENTRY_LENGTH,
                "the body must be a whole number of 28-byte entries");
        Set<String> slices = new HashSet<>();
        for (int start = HEADER_LENGTH; start < encoded.length; start += ENTRY_LENGTH) {
            slices.add(hex(Arrays.copyOfRange(encoded, start, start + ENTRY_LENGTH)));
        }
        return slices;
    }

    /** Renders bytes as fixed-width hex so set comparisons produce readable failure output. */
    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (byte[] part : parts) {
            buffer.put(part);
        }
        return buffer.array();
    }
}
