package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Names the record headers the protocol travels in.
 *
 * <p>{@value #CLOCK} carries the record's dependency clock. {@value #SENDER} and
 * {@value #SEQ} carry the sender tag that lets receivers resolve sequence claims against this
 * record.
 *
 * <p>Absent headers claim and tag nothing. There are no protocol records.
 */
public final class CausalHeaders {

    /** The header carrying the record's dependency clock. */
    public static final String CLOCK = "vc";

    /** The header carrying the sender's identity. */
    public static final String SENDER = "vc-sender";

    /** The header carrying the sender's send sequence. */
    public static final String SEQ = "vc-seq";

    private CausalHeaders() {}

    /**
     * Reads the clock header. Fails closed on undecodable bytes.
     *
     * @param headers the record's headers
     * @return the carried clock, or null when the header is absent
     * @throws CorruptClockException if the header is present but undecodable
     */
    static VectorClock read(Headers headers) {
        Header h = headers.lastHeader(CLOCK);
        return h == null || h.value() == null ? null : VectorClock.deserialize(h.value());
    }

    /**
     * Reads the sender tag. Fails closed on undecodable bytes.
     *
     * @param headers the record's headers
     * @return the sender identity, or null when the record is untagged
     * @throws CorruptClockException if the header is present but not sixteen bytes
     */
    public static UUID readSender(Headers headers) {
        Header h = headers.lastHeader(SENDER);
        if (h == null || h.value() == null) return null;
        if (h.value().length != 16) throw new CorruptClockException("malformed sender header");
        ByteBuffer b = ByteBuffer.wrap(h.value());
        return new UUID(b.getLong(), b.getLong());
    }

    /**
     * Reads the sender sequence. Fails closed on undecodable bytes.
     *
     * @param headers the record's headers
     * @return the send sequence, or {@code -1} when the record is untagged
     * @throws CorruptClockException if the header is present but not eight bytes
     */
    public static long readSeq(Headers headers) {
        Header h = headers.lastHeader(SEQ);
        if (h == null || h.value() == null) return -1;
        if (h.value().length != 8) throw new CorruptClockException("malformed seq header");
        return ByteBuffer.wrap(h.value()).getLong();
    }

    /**
     * Replaces the protocol headers with {@code stamp}'s clock and sender tag.
     *
     * @param headers the outbound record's headers, written in place
     * @param stamp the clock and sender tag to write
     */
    static void write(Headers headers, SendStamp stamp) {
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

    /**
     * Replaces only the clock header, for plain producers, which carry no sender tag.
     *
     * @param headers the outbound record's headers, written in place
     * @param clock the clock to write
     */
    static void write(Headers headers, VectorClock clock) {
        headers.remove(CLOCK);
        headers.add(CLOCK, clock.serialize());
    }
}
