package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The header vocabulary: clock and sender tag round-trip, absence and corruption rules. */
class CausalHeadersTest {

    private static final Channel C1 = new Channel(UUID.nameUUIDFromBytes("c1".getBytes()), 0);

    /** A full send stamp round-trips: clock, sender id, and sender sequence. */
    @Test
    void sendStampRoundTrips() {
        UUID sender = UUID.nameUUIDFromBytes("p1".getBytes());
        var headers = new RecordHeaders();
        CausalHeaders.write(headers, new SendStamp(VectorClock.of(C1, 5), sender, 7));

        assertEquals(VectorClock.of(C1, 5), CausalHeaders.read(headers),
                "the clock must read back exactly as stamped");
        assertEquals(sender, CausalHeaders.readSender(headers),
                "the sender id must read back exactly as stamped");
        assertEquals(7, CausalHeaders.readSeq(headers),
                "the sender sequence must read back exactly as stamped");
    }

    /** Absent headers claim and tag nothing: null clock, null sender, sequence -1. */
    @Test
    void absentHeadersReadAsUntagged() {
        var headers = new RecordHeaders();
        assertNull(CausalHeaders.read(headers), "no clock header means no claims");
        assertNull(CausalHeaders.readSender(headers), "no sender header means untagged");
        assertEquals(-1, CausalHeaders.readSeq(headers), "no seq header means untagged");
    }

    /** A sender or sequence header of the wrong length throws — never reads as untagged. */
    @Test
    void malformedTagsFailClosed() {
        var badSender = new RecordHeaders();
        badSender.add(CausalHeaders.SENDER, new byte[] {1, 2, 3});
        assertThrows(CorruptClockException.class, () -> CausalHeaders.readSender(badSender),
                "a sender tag that is not 16 bytes must fail closed");

        var badSeq = new RecordHeaders();
        badSeq.add(CausalHeaders.SEQ, new byte[] {1, 2, 3});
        assertThrows(CorruptClockException.class, () -> CausalHeaders.readSeq(badSeq),
                "a seq tag that is not 8 bytes must fail closed");
    }

    /** Stamping replaces any prior protocol headers rather than appending to them. */
    @Test
    void stampingReplacesPriorHeaders() {
        UUID sender = UUID.nameUUIDFromBytes("p1".getBytes());
        var headers = new RecordHeaders();
        CausalHeaders.write(headers, new SendStamp(VectorClock.of(C1, 1), sender, 1));
        CausalHeaders.write(headers, new SendStamp(VectorClock.of(C1, 9), sender, 2));

        assertEquals(VectorClock.of(C1, 9), CausalHeaders.read(headers),
                "the last stamp must win");
        assertEquals(2, CausalHeaders.readSeq(headers), "the last sequence must win");
        int clockHeaders = 0;
        for (var ignored : headers.headers(CausalHeaders.CLOCK)) {
            clockHeaders++;
        }
        assertEquals(1, clockHeaders, "restamping must not accumulate clock headers");
    }
}
