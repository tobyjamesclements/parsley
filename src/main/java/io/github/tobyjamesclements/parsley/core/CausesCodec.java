package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * The wire representation of a causal frontier.
 *
 * <p>The format is frozen. Any change to the grammar requires a new {@link #FORMAT_VERSION}
 * and a documented migration, because a frontier written by one process is read by another.
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

    /** Version byte leading every encoded frontier. */
    public static final byte FORMAT_VERSION = 1;

    private static final int ENTRY_LENGTH = ChannelId.ENCODED_LENGTH + Long.BYTES;

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
     * Decodes a frontier.
     *
     * @param headerValue the header value, which may be {@code null}
     * @return the frontier
     * @throws UndecodableMetadataException if the value is null, carries an unknown version,
     *         is truncated, miscounts its entries, names a negative position, or lists
     *         channels out of strictly ascending order
     */
    public static Causes decode(byte[] headerValue) throws UndecodableMetadataException {
        if (headerValue == null) {
            throw new UndecodableMetadataException("causes header present with null value");
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerValue);
        try {
            byte version = buffer.get();
            if (version != FORMAT_VERSION) {
                throw new UndecodableMetadataException("unknown causes format version " + version);
            }
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
        } catch (BufferUnderflowException e) {
            throw new UndecodableMetadataException("truncated causes header");
        } catch (IllegalArgumentException e) {
            throw new UndecodableMetadataException("malformed causes header: " + e.getMessage());
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
