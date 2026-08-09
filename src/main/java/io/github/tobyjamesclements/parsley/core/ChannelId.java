package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Identity of a channel: a topic-partition, identified by Kafka topic ID rather than name so that a topic deleted and
 * recreated under the same name is a different channel (SPEC Assumption 2).
 *
 * <p>The 20-byte encoding (topic ID most-significant half, least-significant half, partition; big-endian) is part of
 * the stable metadata representation documented in {@code docs/METADATA.md}.</p>
 */
public record ChannelId(UUID topicId, int partition) implements Comparable<ChannelId> {

    public static final int ENCODED_LENGTH = 20;

    public ChannelId {
        if (topicId == null) {
            throw new NullPointerException("topicId");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative: " + partition);
        }
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(topicId.getMostSignificantBits());
        buffer.putLong(topicId.getLeastSignificantBits());
        buffer.putInt(partition);
    }

    public static ChannelId readFrom(ByteBuffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        int partition = buffer.getInt();
        return new ChannelId(new UUID(msb, lsb), partition);
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        writeTo(buffer);
        return buffer.array();
    }

    /**
     * The channel total order of the wire format (docs/METADATA.md): the unsigned lexicographic order of the
     * 20-byte encoding. Deliberately not {@link UUID#compareTo}, whose signed comparison diverges from the encoded
     * byte order for high-bit UUIDs — the documented representation and the implementation must be one rule.
     */
    @Override
    public int compareTo(ChannelId other) {
        int c = Long.compareUnsigned(topicId.getMostSignificantBits(), other.topicId.getMostSignificantBits());
        if (c != 0) {
            return c;
        }
        c = Long.compareUnsigned(topicId.getLeastSignificantBits(), other.topicId.getLeastSignificantBits());
        return c != 0 ? c : Integer.compare(partition, other.partition);
    }

    @Override
    public String toString() {
        return topicId + "-" + partition;
    }
}
