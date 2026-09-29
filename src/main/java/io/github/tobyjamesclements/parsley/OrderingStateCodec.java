package io.github.tobyjamesclements.parsley;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

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
 */
final class OrderingStateCodec {

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
     * Every state tag except {@link #TAG_VERSION}. The unversioned-state refusal iterates
     * this set, so a new tag must be added here or the state it marks silently escapes the
     * changelog-head-loss check. {@code OrderingStateCodecCorruptionTest} pins the set
     * against the {@code TAG_} constants by reflection, so a tag added above without an
     * entry here fails the build rather than drifting.
     */
    private static final byte[] STATE_TAGS = {TAG_FED_UP_TO, TAG_FRONTIER, TAG_DELIVERED_PAST,
            TAG_NAME_BINDING, TAG_HELD};

    /**
     * Returns every state tag except {@link #TAG_VERSION}, as a copy: the backing array
     * sits on the fail-closed path, and a shared mutable reference would let one caller
     * silently weaken the refusal for every other.
     *
     * @return every state tag except {@link #TAG_VERSION}
     */
    static byte[] stateTags() {
        return STATE_TAGS.clone();
    }

    /** Version of the key layout as a whole. */
    static final byte STORE_FORMAT_VERSION = 1;
    /** Version of the held-message value encoding. */
    static final byte HELD_BLOB_VERSION = 1;

    private OrderingStateCodec() {
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
    static byte[] channelKey(byte tag, Channel channel) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + Channel.ENCODED_LENGTH);
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
    static byte[] heldKey(Channel channel, long position) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + Channel.ENCODED_LENGTH + Long.BYTES);
        buffer.put(TAG_HELD);
        channel.writeTo(buffer);
        buffer.putLong(position);
        return buffer.array();
    }

    /**
     * @param channel the channel to scan
     * @return the prefix matching every held message on that channel
     */
    static byte[] heldPrefix(Channel channel) {
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
     * @param key a key built by {@link #channelKey(byte, Channel)}
     * @return the channel it names
     * @throws FailClosedException with
     *         {@link FailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static Channel channelOfEntryKey(byte[] key) {
        if (key.length != 1 + Channel.ENCODED_LENGTH) {
            throw new FailClosedException(
                    FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "corrupt ordering key: length " + key.length + " for tag '" + (char) key[0] + "'");
        }
        return Channel.readFrom(ByteBuffer.wrap(key, 1, Channel.ENCODED_LENGTH));
    }

    /**
     * @param key a key built by {@link #heldKey(Channel, long)}
     * @return the channel it names
     * @throws FailClosedException with
     *         {@link FailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static Channel channelOfHeldKey(byte[] key) {
        requireHeldKeyLength(key);
        return Channel.readFrom(ByteBuffer.wrap(key, 1, Channel.ENCODED_LENGTH));
    }

    /**
     * @param key a key built by {@link #heldKey(Channel, long)}
     * @return the position it names
     * @throws FailClosedException with
     *         {@link FailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         key is not the exact length that builder writes
     */
    static long positionOfHeldKey(byte[] key) {
        requireHeldKeyLength(key);
        return ByteBuffer.wrap(key, 1 + Channel.ENCODED_LENGTH, Long.BYTES).getLong();
    }

    private static void requireHeldKeyLength(byte[] key) {
        if (key.length != 1 + Channel.ENCODED_LENGTH + Long.BYTES) {
            throw new FailClosedException(
                    FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
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
     * @throws FailClosedException with
     *         {@link FailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         value is not exactly eight bytes
     */
    static long decodeLong(byte[] value) {
        if (value.length != Long.BYTES) {
            throw new FailClosedException(
                    FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
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
    static byte[] encodeHeld(long timestamp, byte[] key, byte[] value, List<Header> headers, Causes causes) {
        int size = 1 + Long.BYTES + 1;
        size += key == null ? 0 : Integer.BYTES + key.length;
        size += value == null ? 0 : Integer.BYTES + value.length;
        size += Integer.BYTES;
        List<byte[]> headerKeys = new ArrayList<>(headers.size());
        for (Header header : headers) {
            byte[] headerKey = header.key().getBytes(StandardCharsets.UTF_8);
            headerKeys.add(headerKey);
            size += Integer.BYTES + headerKey.length + Integer.BYTES
                    + (header.value() == null ? 0 : header.value().length);
        }
        size += Integer.BYTES + causes.size() * (Channel.ENCODED_LENGTH + Long.BYTES);

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
            Header header = headers.get(i);
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
    record HeldBlob(long timestamp, byte[] key, byte[] value, List<Header> headers, Causes causes) {
    }

    /**
     * Decodes a held message.
     *
     * @param blob bytes written by {@link #encodeHeld}
     * @return the decoded message
     * @throws FailClosedException with
     *         {@link FailClosedException.Reason#UNKNOWN_ORDERING_STATE_FORMAT} if the
     *         blob carries an unknown version or is corrupt
     */
    static HeldBlob decodeHeld(byte[] blob) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(blob);
            byte version = buffer.get();
            if (version != HELD_BLOB_VERSION) {
                throw new FailClosedException(
                        FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT,
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
            List<Header> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                byte[] headerKey = readSizedBytes(buffer, "header key");
                int valueLength = buffer.getInt();
                byte[] headerValue = null;
                if (valueLength != -1) {
                    /**
                     * -1 is the one null sentinel encodeHeld writes; any other negative is
                     * corruption, not an alternate spelling of null.
                     */
                    if (valueLength < 0 || valueLength > buffer.remaining()) {
                        throw corrupt("header value length " + valueLength
                                + " with " + buffer.remaining() + " bytes remaining");
                    }
                    headerValue = new byte[valueLength];
                    buffer.get(headerValue);
                }
                headers.add(new Header(new String(headerKey, StandardCharsets.UTF_8), headerValue));
            }
            int causeCount = buffer.getInt();
            if (causeCount < 0
                    || buffer.remaining() != causeCount * (long) (Channel.ENCODED_LENGTH + Long.BYTES)) {
                throw corrupt("cause count " + causeCount + " does not match "
                        + buffer.remaining() + " bytes remaining");
            }
            TreeMap<Channel, Long> causes = new TreeMap<>();
            for (int i = 0; i < causeCount; i++) {
                Channel channel = Channel.readFrom(buffer);
                causes.put(channel, buffer.getLong());
            }
            return new HeldBlob(timestamp, key, value, List.copyOf(headers), Causes.of(causes));
        } catch (BufferUnderflowException | IllegalArgumentException | IndexOutOfBoundsException
                 | NegativeArraySizeException e) {
            throw new FailClosedException(
                    FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, "corrupt held blob", e);
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

    private static FailClosedException corrupt(String detail) {
        return new FailClosedException(
                FailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, "corrupt held blob: " + detail);
    }
    /**
     * Readers over an image of the state, as the latest value per key. These are the
     * questions the start-time checks ask of state a previous run left behind, and they
     * need no engine: only the key layout above.
     */

    /**
     * Whether a key is a held message's, by its tag and shape. A host reading ordering
     * state for its start-time checks needs to know that a hold exists, never what it
     * carries, so it can keep a marker in place of the body.
     *
     * @param key a key from the ordering state
     * @return {@code true} when the key is a held message's
     */
    static boolean isHeldKey(byte[] key) {
        return key.length == 1 + Channel.ENCODED_LENGTH + Long.BYTES && key[0] == TAG_HELD;
    }

    /**
     * Finds the channels holding undelivered messages.
     *
     * @param latestPerKey the ordering state, as the latest value per key
     * @return the channels with at least one held message, in {@link Channel} order
     */
    static Set<Channel> heldChannels(Map<byte[], byte[]> latestPerKey) {
        Set<Channel> channels = new TreeSet<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && isHeldKey(key)) {
                channels.add(channelOfHeldKey(key));
            }
        });
        return channels;
    }

    /**
     * Reports whether a covered position is the fed-to-end sentinel.
     *
     * <p>A channel settled on its topic's confirmed deletion carries this value, which no
     * offset can follow.
     *
     * @param coveredUpTo a position from {@link #coveredPositions}
     * @return {@code true} when the channel was covered to the end
     */
    static boolean isFedToEnd(long coveredUpTo) {
        return coveredUpTo == ProcessEngine.FED_TO_END_OF_CHANNEL;
    }

    /**
     * Recovers how far each channel was covered as fed-or-never-arriving.
     *
     * <p>This is the durable record of a previous execution's read coverage. A start that
     * must re-establish a lost read position resumes at this coverage plus one, the next
     * position the previous execution would have read, and leaves it to the substrate's
     * fetch, under {@code auto.offset.reset=none}, to refuse the position if retention has
     * since discarded it.
     *
     * <p>A channel settled on its topic's confirmed deletion carries the fed-to-end
     * sentinel, {@code Long.MAX_VALUE}, returned verbatim. Arithmetic on a returned value
     * must not assume it can be incremented without overflow. Test it with
     * {@link #isFedToEnd}.
     *
     * @param latestPerKey the ordering state, as the latest value per key
     * @return per channel, the highest position covered as fed-or-never-arriving
     * @throws FailClosedException if a coverage entry is corrupt
     */
    static Map<Channel, Long> coveredPositions(Map<byte[], byte[]> latestPerKey) {
        Map<Channel, Long> covered = new HashMap<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length > 0 && key[0] == TAG_FED_UP_TO) {
                covered.put(channelOfEntryKey(key), decodeLong(value));
            }
        });
        return covered;
    }

    /**
     * Recovers which topic identity each topic name was bound to.
     *
     * @param latestPerKey the ordering state, as the latest value per key
     * @return topic name to the identity this process recorded for it
     */
    static Map<String, UUID> nameBindings(Map<byte[], byte[]> latestPerKey) {
        Map<String, UUID> bindings = new HashMap<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length > 1 && key[0] == TAG_NAME_BINDING
                    && value.length == Channel.ENCODED_LENGTH) {
                String name = new String(key, 1, key.length - 1, StandardCharsets.UTF_8);
                bindings.put(name, Channel.readFrom(ByteBuffer.wrap(value)).topicId());
            }
        });
        return bindings;
    }

    /**
     * Finds topics that now resolve to a different identity than the one recorded.
     *
     * <p>A topic deleted and recreated under the same name resolves to a new identity, which
     * makes every stored position for it meaningless.
     *
     * @param latestPerKey     the ordering state, as the latest value per key
     * @param resolvedTopicIds topic name to the identity the broker reports now
     * @return the names whose identity changed, sorted
     */
    static List<String> identityChangedTopics(Map<byte[], byte[]> latestPerKey,
                                              Map<String, UUID> resolvedTopicIds) {
        Map<String, UUID> bindings = nameBindings(latestPerKey);
        List<String> changed = new ArrayList<>();
        resolvedTopicIds.forEach((name, resolvedId) -> {
            UUID bound = bindings.get(name);
            if (bound != null && !bound.equals(resolvedId)) {
                changed.add(name);
            }
        });
        changed.sort(String::compareTo);
        return changed;
    }
}
