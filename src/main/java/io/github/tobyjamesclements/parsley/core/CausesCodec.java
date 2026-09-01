package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The wire representation of a causal frontier.
 *
 * <p>The format is frozen: entries grouped by topic, structural fields as minimal varints,
 * positions fixed-width (wire-format.md, D98). Any change to the grammar requires a new
 * {@link #FORMAT_VERSION} and a documented migration, because a frontier written by one
 * process is read by another.
 *
 * <p>Decoding is strict: a header that is truncated, miscounted, negatively positioned, out
 * of canonical order or padded in its varints is rejected rather than salvaged. Metadata
 * that cannot be trusted is treated as a reason to stop, so a corrupted frontier cannot
 * silently become a weaker one.
 *
 * @see Causes
 */
public final class CausesCodec {
    /** The header carrying the encoded frontier. */
    public static final String HEADER_KEY = "parsley.causes";

    /** Header prefix reserved for Parsley, which application headers may not use. */
    public static final String RESERVED_HEADER_PREFIX = "parsley.";

    /** Version byte leading every encoded frontier. */
    public static final byte FORMAT_VERSION = 1;

    private CausesCodec() {
    }

    /** Signals metadata that was present and could not be trusted. */
    public static final class UndecodableMetadataException extends Exception {
        /**
         * Builds the exception.
         *
         * @param message what was wrong with the encoding
         */
        public UndecodableMetadataException(String message) {
            super(message);
        }
    }

    /**
     * The exact encoded width of a frontier.
     *
     * <p>Size is a function of the frontier's shape — distinct topics, partitions, and the
     * varint widths of the structural fields — not of its entry count alone. The engine
     * maintains the same figure incrementally for its budget checks
     * ({@link ProcessEngine#frontierBytes()}), pinned against this arithmetic through
     * {@link #encode(Causes)}.
     *
     * @param causes the frontier to measure
     * @return the byte count {@link #encode(Causes)} produces for it
     */
    static int encodedSize(Causes causes) {
        return encodedSize(causes.byChannel());
    }

    static int encodedSize(SortedMap<ChannelId, Long> byChannel) {
        int size = 1 + unsignedVarintSize(topicCount(byChannel));
        UUID currentTopic = null;
        int partitions = 0;
        for (ChannelId channel : byChannel.keySet()) {
            if (!channel.topicId().equals(currentTopic)) {
                if (currentTopic != null) {
                    size += unsignedVarintSize(partitions);
                }
                currentTopic = channel.topicId();
                partitions = 0;
                size += 2 * Long.BYTES;
            }
            partitions++;
            size += unsignedVarintSize(channel.partition()) + Long.BYTES;
        }
        if (currentTopic != null) {
            size += unsignedVarintSize(partitions);
        }
        return size;
    }

    /**
     * Encodes a frontier.
     *
     * <p>Channels are written in {@link ChannelId} order — topics ascending unsigned, each
     * once, partitions ascending within their group — so the same frontier always yields
     * the same bytes.
     *
     * @param causes the frontier to encode
     * @return the header value
     */
    public static byte[] encode(Causes causes) {
        return encode(causes.byChannel());
    }

    /**
     * Encodes a frontier held as a map in {@link ChannelId} order — the engine's own
     * frontier, without copying it into a {@link Causes} first (D102). The map must be
     * sorted by the channel's natural order, which is the order every group and pair is
     * written in.
     *
     * @param byChannel per channel, the highest causal position, in {@link ChannelId} order
     * @return the header value
     */
    static byte[] encode(SortedMap<ChannelId, Long> byChannel) {
        ByteBuffer buffer = ByteBuffer.allocate(encodedSize(byChannel));
        buffer.put(FORMAT_VERSION);
        writeUnsignedVarint(buffer, topicCount(byChannel));
        // One pass to size each topic's group, one to write it: the partition count
        // precedes its pairs, and a sorted map yields each topic's partitions together.
        int[] groupSizes = new int[topicCount(byChannel)];
        int group = -1;
        UUID currentTopic = null;
        for (ChannelId channel : byChannel.keySet()) {
            if (!channel.topicId().equals(currentTopic)) {
                currentTopic = channel.topicId();
                group++;
            }
            groupSizes[group]++;
        }
        group = -1;
        currentTopic = null;
        for (Map.Entry<ChannelId, Long> entry : byChannel.entrySet()) {
            ChannelId channel = entry.getKey();
            if (!channel.topicId().equals(currentTopic)) {
                currentTopic = channel.topicId();
                group++;
                buffer.putLong(currentTopic.getMostSignificantBits());
                buffer.putLong(currentTopic.getLeastSignificantBits());
                writeUnsignedVarint(buffer, groupSizes[group]);
            }
            writeUnsignedVarint(buffer, channel.partition());
            buffer.putLong(entry.getValue());
        }
        return buffer.array();
    }

    /** The one spelling of "entries with equal topic id form one group": distinct topics, in order. */
    private static int topicCount(SortedMap<ChannelId, Long> byChannel) {
        int topics = 0;
        UUID currentTopic = null;
        for (ChannelId channel : byChannel.keySet()) {
            if (!channel.topicId().equals(currentTopic)) {
                currentTopic = channel.topicId();
                topics++;
            }
        }
        return topics;
    }

    private static void writeUnsignedVarint(ByteBuffer buffer, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * The width of a value as a minimal unsigned base-128 varint, shared with the engine's
     * incrementally maintained frontier size (wire-format.md's varint rule).
     */
    static int unsignedVarintSize(int value) {
        int size = 1;
        while ((value & 0xFFFFFF80) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    /**
     * Decodes a frontier.
     *
     * @param headerValue the header value, which may be {@code null}
     * @return the frontier
     * @throws UndecodableMetadataException if the value is null, carries an unknown version,
     *         is truncated, miscounts its groups or pairs, names a negative position or the
     *         reserved maximum one, the reserved zero topic ID or a topic with zero
     *         partitions, lists topics or partitions out of strictly ascending order, or
     *         spells a varint non-minimally or beyond the non-negative int range
     */
    public static Causes decode(byte[] headerValue) throws UndecodableMetadataException {
        if (headerValue == null) {
            throw new UndecodableMetadataException("causes header present with null value");
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerValue);
        try {
            byte version = buffer.get();
            if (version != FORMAT_VERSION) {
                throw new UndecodableMetadataException(
                        "unknown causes format version " + Byte.toUnsignedInt(version));
            }
            return decodeBody(buffer);
        } catch (BufferUnderflowException e) {
            throw new UndecodableMetadataException("truncated causes header");
        } catch (IllegalArgumentException e) {
            throw new UndecodableMetadataException("malformed causes header: " + e.getMessage());
        }
    }

    private static Causes decodeBody(ByteBuffer buffer) throws UndecodableMetadataException {
        int topicCount = readUnsignedVarint(buffer, "topic count");
        TreeMap<ChannelId, Long> byChannel = new TreeMap<>();
        UUID previousTopic = null;
        for (int group = 0; group < topicCount; group++) {
            UUID topicId = new UUID(buffer.getLong(), buffer.getLong());
            // The zero topic ID is reserved by the substrate and never assigned to a
            // channel, so no genuine cause can carry it — and once merged it would sit in
            // the frontier as an id no broker query can ever answer for. Refused here so it
            // can never enter a frontier at all (wire-format.md constraint 5, D83).
            if (ChannelId.isZeroTopicId(topicId)) {
                throw new UndecodableMetadataException("zero topic id at group " + group
                        + "; the substrate never assigns it to a channel");
            }
            if (previousTopic != null && ChannelId.compareTopicIds(topicId, previousTopic) <= 0) {
                throw new UndecodableMetadataException("topics not strictly ascending at " + topicId);
            }
            previousTopic = topicId;
            int partitionCount = readUnsignedVarint(buffer, "partition count");
            if (partitionCount == 0) {
                throw new UndecodableMetadataException("topic " + topicId + " names zero partitions");
            }
            int previousPartition = -1;
            for (int i = 0; i < partitionCount; i++) {
                int partition = readUnsignedVarint(buffer, "partition");
                if (partition <= previousPartition) {
                    throw new UndecodableMetadataException(
                            "partitions not strictly ascending at " + topicId + "-" + partition);
                }
                previousPartition = partition;
                ChannelId channel = new ChannelId(topicId, partition);
                long position = buffer.getLong();
                if (position < 0) {
                    throw new UndecodableMetadataException("negative position " + position + " on " + channel);
                }
                // No log reaches 2^63 - 1 records, so no genuine cause can name it, and the
                // engine keeps that value as its in-band fed-to-end marker: absorbed from a
                // header it would masquerade as a channel's deletion once it reached fedUpTo
                // (wire-format.md constraint 7, D105).
                if (position == Long.MAX_VALUE) {
                    throw new UndecodableMetadataException("position " + position + " on " + channel
                            + " is beyond any position a channel can assign");
                }
                byChannel.put(channel, position);
            }
        }
        if (buffer.remaining() != 0) {
            throw new UndecodableMetadataException(
                    buffer.remaining() + " trailing bytes after " + topicCount + " topic groups");
        }
        return Causes.of(byChannel);
    }

    /**
     * Reads one minimal unsigned base-128 varint.
     *
     * <p>Strictness matches the rest of the codec: a padded spelling would let two byte
     * strings mean one frontier, so a terminal zero byte after the first is refused. So is
     * a fifth byte carrying anything beyond the three bits a non-negative int has left —
     * Java's shift discards bits past 31, so without that refusal {@code 85 80 80 80 10}
     * would silently decode to the same value as {@code 05}, aliasing the padding check
     * cannot see — and the same guard refuses a sixth byte outright.
     */
    private static int readUnsignedVarint(ByteBuffer buffer, String field) throws UndecodableMetadataException {
        int value = 0;
        int shift = 0;
        while (true) {
            byte b = buffer.get();
            if (shift == 28 && (b & 0xF8) != 0) {
                throw new UndecodableMetadataException(field + " varint exceeds the non-negative int range");
            }
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                if (shift > 0 && (b & 0x7F) == 0) {
                    throw new UndecodableMetadataException("non-minimal varint in " + field);
                }
                return value;
            }
            shift += 7;
        }
    }

    /**
     * Builds a frontier from a plain map.
     *
     * @param byChannel per channel, the highest causal position
     * @return the frontier
     * @see Causes#of(Map)
     */
    public static Causes causes(Map<ChannelId, Long> byChannel) {
        return Causes.of(byChannel);
    }
}
