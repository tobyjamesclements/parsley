package io.github.tobyjamesclements.parsley.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Binary layout of the ordering-state store. Keys are tagged so one bytes store holds all ordering state, and held
 * messages sort by (channel, position) so a prefix scan yields one channel's hold-back buffer in position order.
 *
 * <pre>
 *   'v'                       → [u8 formatVersion]
 *   'f' + channelId(20)       → [i64 fedUpTo]
 *   'c' + channelId(20)       → [i64 frontierPosition]
 *   'p' + channelId(20)       → [i64 deliveredPast]  (highest position in the delivered causal past)
 *   'n' + topicNameUtf8       → channelId(20)        (identity first seen under this declared name)
 *   'h' + channelId(20) + pos → held message blob
 * </pre>
 */
final class StoreCodec {

    static final byte TAG_VERSION = 'v';
    static final byte TAG_FED_UP_TO = 'f';
    static final byte TAG_FRONTIER = 'c';
    static final byte TAG_DELIVERED_PAST = 'p';
    static final byte TAG_NAME_BINDING = 'n';
    static final byte TAG_HELD = 'h';

    static final byte STORE_FORMAT_VERSION = 1;
    static final byte HELD_BLOB_VERSION = 1;

    private StoreCodec() {
    }

    static byte[] versionKey() {
        return new byte[] {TAG_VERSION};
    }

    static byte[] channelKey(byte tag, ChannelId channel) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + ChannelId.ENCODED_LENGTH);
        buffer.put(tag);
        channel.writeTo(buffer);
        return buffer.array();
    }

    static byte[] channelNameKey(String topicName) {
        byte[] name = topicName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + name.length);
        buffer.put(TAG_NAME_BINDING);
        buffer.put(name);
        return buffer.array();
    }

    static byte[] heldKey(ChannelId channel, long position) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + ChannelId.ENCODED_LENGTH + Long.BYTES);
        buffer.put(TAG_HELD);
        channel.writeTo(buffer);
        buffer.putLong(position);
        return buffer.array();
    }

    static byte[] heldPrefix(ChannelId channel) {
        return channelKey(TAG_HELD, channel);
    }

    static byte[] tagPrefix(byte tag) {
        return new byte[] {tag};
    }

    static ChannelId channelOfKey(byte[] key) {
        ByteBuffer buffer = ByteBuffer.wrap(key, 1, ChannelId.ENCODED_LENGTH);
        return ChannelId.readFrom(buffer);
    }

    static long positionOfHeldKey(byte[] key) {
        return ByteBuffer.wrap(key, 1 + ChannelId.ENCODED_LENGTH, Long.BYTES).getLong();
    }

    static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    static long decodeLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    private static final int FLAG_KEY_NULL = 1;
    private static final int FLAG_VALUE_NULL = 2;

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

    record HeldBlob(long timestamp, byte[] key, byte[] value, List<HeaderKV> headers, Causes causes) {
    }

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
                key = new byte[buffer.getInt()];
                buffer.get(key);
            }
            byte[] value = null;
            if ((flags & FLAG_VALUE_NULL) == 0) {
                value = new byte[buffer.getInt()];
                buffer.get(value);
            }
            int headerCount = buffer.getInt();
            List<HeaderKV> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                byte[] headerKey = new byte[buffer.getInt()];
                buffer.get(headerKey);
                int valueLength = buffer.getInt();
                byte[] headerValue = null;
                if (valueLength >= 0) {
                    headerValue = new byte[valueLength];
                    buffer.get(headerValue);
                }
                headers.add(new HeaderKV(new String(headerKey, StandardCharsets.UTF_8), headerValue));
            }
            int causeCount = buffer.getInt();
            TreeMap<ChannelId, Long> causes = new TreeMap<>();
            for (int i = 0; i < causeCount; i++) {
                ChannelId channel = ChannelId.readFrom(buffer);
                causes.put(channel, buffer.getLong());
            }
            return new HeldBlob(timestamp, key, value, List.copyOf(headers), Causes.of(causes));
        } catch (BufferUnderflowException | IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, "corrupt held blob", e);
        }
    }
}
