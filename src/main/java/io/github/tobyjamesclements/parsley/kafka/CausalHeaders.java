package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.CorruptClockException;
import io.github.tobyjamesclements.parsley.core.SendStamp;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * The wire vocabulary: {@value #CLOCK} carries the record's dependency clock, and
 * {@value #SENDER} / {@value #SEQ} carry the sender tag that lets receivers resolve sequence
 * claims against this record. Absent headers claim and tag nothing. There are no protocol
 * records.
 */
public final class CausalHeaders {

    public static final String CLOCK = "parsley-clock";
    public static final String SENDER = "parsley-sender";
    public static final String SEQ = "parsley-seq";

    private CausalHeaders() {}

    /** Reads the clock header; null when absent. Undecodable bytes throw (fail closed). */
    public static Clock read(Headers headers) {
        Header h = headers.lastHeader(CLOCK);
        return h == null || h.value() == null ? null : Clock.deserialize(h.value());
    }

    /** Reads the sender tag; null when untagged. Undecodable bytes throw (fail closed). */
    public static UUID readSender(Headers headers) {
        Header h = headers.lastHeader(SENDER);
        if (h == null || h.value() == null) return null;
        if (h.value().length != 16) throw new CorruptClockException("malformed sender header");
        ByteBuffer b = ByteBuffer.wrap(h.value());
        return new UUID(b.getLong(), b.getLong());
    }

    /** Reads the sender sequence; -1 when untagged. Undecodable bytes throw (fail closed). */
    public static long readSeq(Headers headers) {
        Header h = headers.lastHeader(SEQ);
        if (h == null || h.value() == null) return -1;
        if (h.value().length != 8) throw new CorruptClockException("malformed seq header");
        return ByteBuffer.wrap(h.value()).getLong();
    }

    /** Replaces the protocol headers with {@code stamp}'s clock and sender tag. */
    public static void write(Headers headers, SendStamp stamp) {
        headers.remove(CLOCK);
        headers.add(CLOCK, stamp.clock().serialize());
        headers.remove(SENDER);
        ByteBuffer sender = ByteBuffer.allocate(16);
        sender.putLong(stamp.senderId().getMostSignificantBits());
        sender.putLong(stamp.senderId().getLeastSignificantBits());
        headers.add(SENDER, sender.array());
        headers.remove(SEQ);
        headers.add(SEQ, ByteBuffer.allocate(8).putLong(stamp.senderSeq()).array());
    }

    /** Replaces only the clock header (edge producers, which carry no sender tag). */
    public static void write(Headers headers, Clock clock) {
        headers.remove(CLOCK);
        headers.add(CLOCK, clock.serialize());
    }
}
