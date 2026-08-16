package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Key and value layout for ordering state.
 *
 * <p>Every key begins with a one-byte tag, which makes each class of entry a distinct prefix
 * range and lets the engine scan one class without reading the rest. Held messages are keyed
 * by channel then position, so a scan over a channel's prefix returns its hold-back buffer in
 * position order.
 *
 * <p>Both formats are versioned, and a version this build does not recognise stops the
 * process rather than being guessed at.
 *
 * @see OrderingStore
 * @see OrderingStateInspector
 */
final class StoreCodec {

    /** Tag for the store format version entry. */
    static final byte TAG_VERSION = 'v';
    /** Tag for how far each channel has been fed. */
    static final byte TAG_FED_UP_TO = 'f';
    /** Tag for the causal frontier. */
    static final byte TAG_FRONTIER = 'c';
    /** Tag for the delivered causal past, which clamps a channel joining later. */
    static final byte TAG_DELIVERED_PAST = 'p';
    /** Tag binding a topic name to the identity this process saw for it. */
    static final byte TAG_NAME_BINDING = 'n';
    /** Tag for a held message. */
    static final byte TAG_HELD = 'h';

    /**
     * Every state tag except {@link #TAG_VERSION}. The unversioned-state refusal and its
     * pinning test iterate this set; a new tag must be added here, or the state it marks
     * silently escapes the changelog-head-loss check.
     */
    static final byte[] STATE_TAGS = {TAG_FED_UP_TO, TAG_FRONTIER, TAG_DELIVERED_PAST,
            TAG_NAME_BINDING, TAG_HELD};

    /** Version of the key layout as a whole. */
    static final byte STORE_FORMAT_VERSION = 1;
    /** Version of the held-message value encoding. */
    static final byte HELD_BLOB_VERSION = 1;

    private StoreCodec() {
    }

    /**
     * Returns the key holding {@link #STORE_FORMAT_VERSION}.
     *
     * @return the key holding {@link #STORE_FORMAT_VERSION}
     */
    static byte[] versionKey() {
        return new byte[] {TAG_VERSION};
    }

    /**
     * @param tag     the entry class
     * @param channel the channel the entry concerns
     * @return the key for one channel's entry of that class
     */
    static byte[] channelKey(byte tag, ChannelId channel) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + ChannelId.ENCODED_LENGTH);
        buffer.put(tag);
        channel.writeTo(buffer);
        return buffer.array();
    }

    /**
     * @param topicName the topic name to bind
     * @return the key holding that name's recorded topic identity
     */
    static byte[] channelNameKey(String topicName) {
        byte[] name = topicName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + name.length);
        buffer.put(TAG_NAME_BINDING);
        buffer.put(name);
        return buffer.array();
    }

    /**
     * @param channel  the channel the message arrived on
     * @param position its position within that channel
     * @return the key for one held message, ordered by position within a channel
     */
    static byte[] heldKey(ChannelId channel, long position) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + ChannelId.ENCODED_LENGTH + Long.BYTES);
        buffer.put(TAG_HELD);
        channel.writeTo(buffer);
        buffer.putLong(position);
        return buffer.array();
    }

    /**
     * @param channel the channel to scan
     * @return the prefix matching every held message on that channel
     */
    static byte[] heldPrefix(ChannelId channel) {
        return channelKey(TAG_HELD, channel);
    }

    /**
     * @param tag the entry class to scan
     * @return the prefix matching every entry of that class
     */
    static byte[] tagPrefix(byte tag) {
        return new byte[] {tag};
    }

    /**
     * @param key a key built by {@link #channelKey(byte, ChannelId)}
     * @return the channel it names
     * @throws ParsleyFailClosedException with
     *         {@link ParsleyFailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static ChannelId channelOfEntryKey(byte[] key) {
        if (key.length != 1 + ChannelId.ENCODED_LENGTH) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "corrupt ordering key: length " + key.length + " for tag '" + (char) key[0] + "'");
        }
        return ChannelId.readFrom(ByteBuffer.wrap(key, 1, ChannelId.ENCODED_LENGTH));
    }

    /**
     * @param key a key built by {@link #heldKey(ChannelId, long)}
     * @return the channel it names
     * @throws ParsleyFailClosedException with
     *         {@link ParsleyFailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static ChannelId channelOfHeldKey(byte[] key) {
        requireHeldKeyLength(key);
        return ChannelId.readFrom(ByteBuffer.wrap(key, 1, ChannelId.ENCODED_LENGTH));
    }

    /**
     * @param key a key built by {@link #heldKey(ChannelId, long)}
     * @return the position it names
     * @throws ParsleyFailClosedException with
     *         {@link ParsleyFailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static long positionOfHeldKey(byte[] key) {
        requireHeldKeyLength(key);
        return ByteBuffer.wrap(key, 1 + ChannelId.ENCODED_LENGTH, Long.BYTES).getLong();
    }

    private static void requireHeldKeyLength(byte[] key) {
        if (key.length != 1 + ChannelId.ENCODED_LENGTH + Long.BYTES) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "corrupt held key: length " + key.length);
        }
    }

    /**
     * @param value the number to store
     * @return its eight-byte encoding
     */
    static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * @param value an eight-byte encoding
     * @return the number it holds
     * @throws ParsleyFailClosedException with
     *         {@link ParsleyFailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         value is not exactly eight bytes
     */
    static long decodeLong(byte[] value) {
        if (value.length != Long.BYTES) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "corrupt ordering value: length " + value.length + " where 8 bytes were written");
        }
        return ByteBuffer.wrap(value).getLong();
    }

    private static final int FLAG_KEY_NULL = 1;
    private static final int FLAG_VALUE_NULL = 2;

    /**
     * Encodes a held message with everything needed to deliver it after a restart.
     *
     * <p>Null keys, null values and null header values are each distinguished from empty
     * ones, so a restored message reaches application logic byte for byte as it arrived.
     *
     * @param timestamp the message timestamp
     * @param key       the key bytes, which may be {@code null}
     * @param value     the value bytes, which may be {@code null}
     * @param headers   the headers as received
     * @param causes    the frontier the message expressed
     * @return the encoded blob
     */
    static byte[] encodeHeld(long timestamp, byte[] key, byte[] value, List<HeaderKV> headers, Causes causes) {
        int size = 1 + Long.BYTES + 1;
        size += key == null ? 0 : Integer.BYTES + key.length;
        size += value == null ? 0 : Integer.BYTES + value.length;
        size += Integer.BYTES;
        List<byte[]> headerKeys = new ArrayList<>(headers.size());
        for (HeaderKV header : headers) {
            byte[] headerKey = header.key().getBytes(StandardCharsets.UTF_8);
            headerKeys.add(headerKey);
            size += Integer.BYTES + headerKey.length + Integer.BYTES
                    + (header.value() == null ? 0 : header.value().length);
        }
        size += Integer.BYTES + causes.size() * (ChannelId.ENCODED_LENGTH + Long.BYTES);

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(HELD_BLOB_VERSION);
        buffer.putLong(timestamp);
        int flags = (key == null ? FLAG_KEY_NULL : 0) | (value == null ? FLAG_VALUE_NULL : 0);
        buffer.put((byte) flags);
        if (key != null) {
            buffer.putInt(key.length).put(key);
        }
        if (value != null) {
            buffer.putInt(value.length).put(value);
        }
        buffer.putInt(headers.size());
        for (int i = 0; i < headers.size(); i++) {
            HeaderKV header = headers.get(i);
            byte[] headerKey = headerKeys.get(i);
            buffer.putInt(headerKey.length).put(headerKey);
            if (header.value() == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(header.value().length).put(header.value());
            }
        }
        buffer.putInt(causes.size());
        causes.byChannel().forEach((channel, position) -> {
            channel.writeTo(buffer);
            buffer.putLong(position);
        });
        return buffer.array();
    }

    /**
     * A decoded held message.
     *
     * @param timestamp the message timestamp
     * @param key       the key bytes, which may be {@code null}
     * @param value     the value bytes, which may be {@code null}
     * @param headers   the headers as received
     * @param causes    the frontier the message expressed
     */
    record HeldBlob(long timestamp, byte[] key, byte[] value, List<HeaderKV> headers, Causes causes) {
    }

    /**
     * Decodes a held message.
     *
     * @param blob bytes written by {@link #encodeHeld}
     * @return the decoded message
     * @throws ParsleyFailClosedException with
     *         {@link ParsleyFailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         blob carries an unknown version or is corrupt
     */
    static HeldBlob decodeHeld(byte[] blob) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(blob);
            byte version = buffer.get();
            if (version != HELD_BLOB_VERSION) {
                throw new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                        "held blob version " + version);
            }
            long timestamp = buffer.getLong();
            int flags = buffer.get();
            byte[] key = null;
            if ((flags & FLAG_KEY_NULL) == 0) {
                key = readSizedBytes(buffer, "key");
            }
            byte[] value = null;
            if ((flags & FLAG_VALUE_NULL) == 0) {
                value = readSizedBytes(buffer, "value");
            }
            int headerCount = buffer.getInt();
            if (headerCount < 0 || headerCount > buffer.remaining() / (2 * Integer.BYTES)) {
                throw corrupt("header count " + headerCount + " with " + buffer.remaining() + " bytes remaining");
            }
            List<HeaderKV> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                byte[] headerKey = readSizedBytes(buffer, "header key");
                int valueLength = buffer.getInt();
                byte[] headerValue = null;
                if (valueLength != -1) {
                    // -1 is the one null sentinel encodeHeld writes; any other negative is
                    // corruption, not an alternate spelling of null.
                    if (valueLength < 0 || valueLength > buffer.remaining()) {
                        throw corrupt("header value length " + valueLength
                                + " with " + buffer.remaining() + " bytes remaining");
                    }
                    headerValue = new byte[valueLength];
                    buffer.get(headerValue);
                }
                headers.add(new HeaderKV(new String(headerKey, StandardCharsets.UTF_8), headerValue));
            }
            int causeCount = buffer.getInt();
            if (causeCount < 0
                    || buffer.remaining() != causeCount * (long) (ChannelId.ENCODED_LENGTH + Long.BYTES)) {
                throw corrupt("cause count " + causeCount + " does not match "
                        + buffer.remaining() + " bytes remaining");
            }
            TreeMap<ChannelId, Long> causes = new TreeMap<>();
            for (int i = 0; i < causeCount; i++) {
                ChannelId channel = ChannelId.readFrom(buffer);
                causes.put(channel, buffer.getLong());
            }
            return new HeldBlob(timestamp, key, value, List.copyOf(headers), Causes.of(causes));
        } catch (BufferUnderflowException | IllegalArgumentException | IndexOutOfBoundsException
                 | NegativeArraySizeException e) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, "corrupt held blob", e);
        }
    }

    private static byte[] readSizedBytes(ByteBuffer buffer, String what) {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw corrupt(what + " length " + length + " with " + buffer.remaining() + " bytes remaining");
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private static ParsleyFailClosedException corrupt(String detail) {
        return new ParsleyFailClosedException(
                ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, "corrupt held blob: " + detail);
    }
}
