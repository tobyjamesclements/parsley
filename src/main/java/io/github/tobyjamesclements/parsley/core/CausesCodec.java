package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * The stable wire representation of causal metadata (SPEC Structural 5, 11), carried in a single Kafka record header
 * named {@link #HEADER_KEY}. The format is documented in {@code docs/METADATA.md} and is strict: any deviation —
 * unknown version, truncation, trailing bytes, unsorted or duplicate channels, negative positions — is undecodable,
 * and undecodable metadata fails closed (SPEC Safety 7). An absent header means no causes (SPEC Safety 6).
 */
public final class CausesCodec {

    /** Reserved header key. Application headers may not use the {@code parsley.} prefix. */
    public static final String HEADER_KEY = "parsley.causes";
    public static final String RESERVED_HEADER_PREFIX = "parsley.";

    public static final byte FORMAT_VERSION = 1;

    private static final int ENTRY_LENGTH = ChannelId.ENCODED_LENGTH + Long.BYTES;

    private CausesCodec() {
    }

    /** Thrown when a causes header is present but not decodable. The receiver must fail closed, never deliver. */
    public static final class UndecodableMetadataException extends Exception {
        public UndecodableMetadataException(String message) {
            super(message);
        }
    }

    /** The encoded size of a causes set with this many entries — exact by construction of the format, so callers
     * can bound metadata without paying for an encode. */
    public static int encodedSize(int entries) {
        return 1 + Integer.BYTES + entries * ENTRY_LENGTH;
    }

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

    /** Convenience for building a {@link Causes} in tests and adapters. */
    public static Causes causes(Map<ChannelId, Long> byChannel) {
        return Causes.of(byChannel);
    }
}
