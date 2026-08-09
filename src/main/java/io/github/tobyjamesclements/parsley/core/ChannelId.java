package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A channel, named by topic identity rather than topic name.
 *
 * <p>Names are reusable and identities are not, so a topic deleted and recreated under the
 * same name yields a different {@code ChannelId}. Positions carried against the old identity
 * cannot be mistaken for positions in the new log.
 *
 * <p>Ordering is unsigned over the topic identity, then by partition, which gives the
 * canonical order the wire format encodes in.
 *
 * @param topicId   the topic's identity as assigned by the broker
 * @param partition the partition within that topic
 * @see CausesCodec
 */
public record ChannelId(UUID topicId, int partition) implements Comparable<ChannelId> {

    /** Encoded width in bytes: two longs of topic identity, then the partition. */
    public static final int ENCODED_LENGTH = 20;

    /**
     * Validates the topic identity and partition.
     *
     * @throws NullPointerException     if {@code topicId} is null
     * @throws IllegalArgumentException if {@code partition} is negative
     */
    public ChannelId {
        if (topicId == null) {
            throw new NullPointerException("topicId");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative: " + partition);
        }
    }

    /**
     * Appends this channel to a buffer in wire order.
     *
     * @param buffer the buffer to write to, with {@link #ENCODED_LENGTH} bytes remaining
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(topicId.getMostSignificantBits());
        buffer.putLong(topicId.getLeastSignificantBits());
        buffer.putInt(partition);
    }

    /**
     * Reads one channel from a buffer.
     *
     * @param buffer positioned at an encoded channel
     * @return the channel
     */
    public static ChannelId readFrom(ByteBuffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        int partition = buffer.getInt();
        return new ChannelId(new UUID(msb, lsb), partition);
    }

    /**
     * Returns this channel encoded into a fresh array of {@link #ENCODED_LENGTH} bytes.
     *
     * @return this channel encoded into a fresh array of {@link #ENCODED_LENGTH} bytes
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        writeTo(buffer);
        return buffer.array();
    }

    /**
     * Orders by topic identity unsigned, then by partition.
     *
     * @param other the channel to compare against
     * @return a negative value, zero, or a positive value as this sorts before, with, or
     *         after {@code other}
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

    /**
     * Returns {@code topicId-partition}.
     *
     * @return {@code topicId-partition}
     */
    @Override
    public String toString() {
        return topicId + "-" + partition;
    }
}
