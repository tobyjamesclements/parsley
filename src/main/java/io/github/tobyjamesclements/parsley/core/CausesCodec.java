package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The wire representation of a causal frontier.
 *
 * <p>Both grammars are frozen. Version 1 is the flat entry list every writer emits; version 2
 * groups entries by topic and is accepted on decode ahead of the writer flip (D98). Any
 * further change requires a new version and a documented migration, because a frontier
 * written by one process is read by another.
 *
 * <p>Decoding is strict: a header that is truncated, miscounted, negatively positioned or not
 * in strictly ascending channel order is rejected rather than salvaged. Metadata that cannot
 * be trusted is treated as a reason to stop, so a corrupted frontier cannot silently become a
 * weaker one.
 *
 * @see Causes
 */
public final class CausesCodec {
    /** The header carrying the encoded frontier. */
    public static final String HEADER_KEY = "parsley.causes";

    /** Header prefix reserved for Parsley, which application headers may not use. */
    public static final String RESERVED_HEADER_PREFIX = "parsley.";

    /** Version byte leading every frontier this codec encodes. */
    public static final byte FORMAT_VERSION = 1;

    /**
     * Version byte of the grouped grammar, which {@link #decode(byte[])} accepts now and
     * {@link #encode(Causes)} adopts at the writer flip (D98).
     */
    public static final byte GROUPED_FORMAT_VERSION = 2;

    private static final int ENTRY_LENGTH = ChannelId.ENCODED_LENGTH + Long.BYTES;

    private static final int TOPIC_ID_LENGTH = 2 * Long.BYTES;

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
     * @param entries how many channels the frontier names
     * @return the byte count, used to test a frontier against the metadata budget before
     *         encoding it
     */
    public static int encodedSize(int entries) {
        return 1 + Integer.BYTES + entries * ENTRY_LENGTH;
    }

    /**
     * Encodes a frontier.
     *
     * <p>Channels are written in {@link ChannelId} order, so the same frontier always yields
     * the same bytes.
     *
     * @param causes the frontier to encode
     * @return the header value
     */
    public static byte[] encode(Causes causes) {
        ByteBuffer buffer = ByteBuffer.allocate(encodedSize(causes.size()));
        buffer.put(FORMAT_VERSION);
        buffer.putInt(causes.size());
        causes.byChannel().forEach((channel, position) -> {
            channel.writeTo(buffer);
            buffer.putLong(position);
        });
        return buffer.array();
    }

    /**
     * Encodes a frontier in the version-2 grouped grammar.
     *
     * <p>Not yet the wire encoding: {@link #encode(Causes)} still writes version 1, so that
     * readers which only know the flat grammar keep decoding every message. This encoder
     * exists now so the readers shipped in this phase are provably compatible, byte for
     * byte, with the writers of the flip (D98) — the version-2 golden vector and round-trip
     * pins exercise it.
     *
     * <p>Channels are written in {@link ChannelId} order — topics ascending unsigned, each
     * once, partitions ascending within — so the same frontier always yields the same bytes.
     *
     * @param causes the frontier to encode
     * @return the version-2 encoding
     */
    static byte[] encodeGrouped(Causes causes) {
        ByteBuffer buffer = ByteBuffer.allocate(groupedSize(causes));
        buffer.put(GROUPED_FORMAT_VERSION);
        List<Map.Entry<ChannelId, Long>> entries = List.copyOf(causes.byChannel().entrySet());
        int topics = 0;
        for (int i = 0; i < entries.size(); i++) {
            if (i == 0 || !entries.get(i).getKey().topicId().equals(entries.get(i - 1).getKey().topicId())) {
                topics++;
            }
        }
        writeUnsignedVarint(buffer, topics);
        int i = 0;
        while (i < entries.size()) {
            UUID topicId = entries.get(i).getKey().topicId();
            int end = i;
            while (end < entries.size() && entries.get(end).getKey().topicId().equals(topicId)) {
                end++;
            }
            buffer.putLong(topicId.getMostSignificantBits());
            buffer.putLong(topicId.getLeastSignificantBits());
            writeUnsignedVarint(buffer, end - i);
            for (; i < end; i++) {
                writeUnsignedVarint(buffer, entries.get(i).getKey().partition());
                buffer.putLong(entries.get(i).getValue());
            }
        }
        return buffer.array();
    }

    private static int groupedSize(Causes causes) {
        int size = 1;
        int topics = 0;
        UUID currentTopic = null;
        int partitions = 0;
        for (ChannelId channel : causes.byChannel().keySet()) {
            if (!channel.topicId().equals(currentTopic)) {
                if (currentTopic != null) {
                    size += unsignedVarintSize(partitions);
                }
                currentTopic = channel.topicId();
                partitions = 0;
                topics++;
                size += TOPIC_ID_LENGTH;
            }
            partitions++;
            size += unsignedVarintSize(channel.partition()) + Long.BYTES;
        }
        if (currentTopic != null) {
            size += unsignedVarintSize(partitions);
        }
        return size + unsignedVarintSize(topics);
    }

    private static void writeUnsignedVarint(ByteBuffer buffer, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    private static int unsignedVarintSize(int value) {
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
     * <p>Both documented grammars are accepted: version 1, the flat entry list, and
     * version 2, the grouped form (D98). The same cause set decodes to the same frontier
     * whichever grammar carried it.
     *
     * @param headerValue the header value, which may be {@code null}
     * @return the frontier
     * @throws UndecodableMetadataException if the value is null, carries an unknown version,
     *         is truncated, miscounts its entries or groups, names a negative position or
     *         the reserved zero topic ID, lists channels out of strictly ascending order,
     *         or spells a varint non-minimally or beyond the non-negative int range
     */
    public static Causes decode(byte[] headerValue) throws UndecodableMetadataException {
        if (headerValue == null) {
            throw new UndecodableMetadataException("causes header present with null value");
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerValue);
        try {
            byte version = buffer.get();
            if (version == FORMAT_VERSION) {
                return decodeFlat(buffer);
            }
            if (version == GROUPED_FORMAT_VERSION) {
                return decodeGrouped(buffer);
            }
            throw new UndecodableMetadataException("unknown causes format version " + version);
        } catch (BufferUnderflowException e) {
            throw new UndecodableMetadataException("truncated causes header");
        } catch (IllegalArgumentException e) {
            throw new UndecodableMetadataException("malformed causes header: " + e.getMessage());
        }
    }

    private static Causes decodeFlat(ByteBuffer buffer) throws UndecodableMetadataException {
        int count = buffer.getInt();
        if (count < 0) {
            throw new UndecodableMetadataException("negative cause count " + count);
        }
        if (buffer.remaining() != count * (long) ENTRY_LENGTH) {
            throw new UndecodableMetadataException(
                    "cause count " + count + " does not match remaining length " + buffer.remaining());
        }
        TreeMap<ChannelId, Long> byChannel = new TreeMap<>();
        ChannelId previous = null;
        for (int i = 0; i < count; i++) {
            ChannelId channel = ChannelId.readFrom(buffer);
            // The zero topic ID is reserved by the substrate and never assigned to a
            // channel, so no genuine cause can carry it — and once merged it would sit
            // in the frontier as an id no broker query can ever answer for. Refused
            // here so it can never enter a frontier at all (wire-format.md, D83).
            if (channel.topicId().getMostSignificantBits() == 0
                    && channel.topicId().getLeastSignificantBits() == 0) {
                throw new UndecodableMetadataException("zero topic id at entry " + i
                        + "; the substrate never assigns it to a channel");
            }
            long position = buffer.getLong();
            if (position < 0) {
                throw new UndecodableMetadataException("negative position " + position + " on " + channel);
            }
            if (previous != null && channel.compareTo(previous) <= 0) {
                throw new UndecodableMetadataException("channels not strictly ascending at " + channel);
            }
            previous = channel;
            byChannel.put(channel, position);
        }
        return Causes.of(byChannel);
    }

    private static Causes decodeGrouped(ByteBuffer buffer) throws UndecodableMetadataException {
        int topicCount = readUnsignedVarint(buffer, "topic count");
        TreeMap<ChannelId, Long> byChannel = new TreeMap<>();
        UUID previousTopic = null;
        for (int group = 0; group < topicCount; group++) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            UUID topicId = new UUID(msb, lsb);
            // Same refusal as the flat grammar's, once per group (wire-format.md, D83).
            if (msb == 0 && lsb == 0) {
                throw new UndecodableMetadataException("zero topic id at group " + group
                        + "; the substrate never assigns it to a channel");
            }
            if (previousTopic != null && compareTopics(topicId, previousTopic) <= 0) {
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
                byChannel.put(channel, position);
            }
        }
        if (buffer.remaining() != 0) {
            throw new UndecodableMetadataException(
                    buffer.remaining() + " trailing bytes after " + topicCount + " topic groups");
        }
        return Causes.of(byChannel);
    }

    private static int compareTopics(UUID a, UUID b) {
        int c = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        return c != 0 ? c : Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
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
