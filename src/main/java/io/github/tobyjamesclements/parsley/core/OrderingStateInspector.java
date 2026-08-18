package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Reads ordering state without an engine.
 *
 * <p>These are the questions a startup check asks of state left behind by a previous run, and
 * that an operator asks of a stopped process: what is still held, and which topics has this
 * process bound to an identity that no longer resolves.
 *
 * @see StoreCodec
 */
public final class OrderingStateInspector {
    private OrderingStateInspector() {
    }

    /**
     * Finds the channels holding undelivered messages.
     *
     * @param latestPerKey the ordering state, as the latest value per key
     * @return the channels with at least one held message, in {@link ChannelId} order
     */
    public static Set<ChannelId> heldChannels(Map<byte[], byte[]> latestPerKey) {
        Set<ChannelId> channels = new TreeSet<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length == 1 + ChannelId.ENCODED_LENGTH + Long.BYTES
                    && key[0] == StoreCodec.TAG_HELD) {
                channels.add(StoreCodec.channelOfHeldKey(key));
            }
        });
        return channels;
    }

    /**
     * Recovers how far each channel was covered as fed-or-never-arriving.
     *
     * <p>This is the durable record of a previous execution's read coverage. A start that
     * re-establishes a lost read position must not place it beyond this coverage plus one:
     * doing so would fabricate a read-position report claiming positions were fed or will
     * never arrive when they were simply discarded unread.
     *
     * <p>A channel settled on its topic's confirmed deletion carries the engine's
     * fed-to-end sentinel, {@code Long.MAX_VALUE} — everything covered. It is returned
     * verbatim; arithmetic on a returned value must not assume it can be incremented
     * without overflow.
     *
     * @param latestPerKey the ordering state, as the latest value per key
     * @return per channel, the highest position covered as fed-or-never-arriving
     * @throws ParsleyFailClosedException if a coverage entry is corrupt
     */
    public static Map<ChannelId, Long> coveredPositions(Map<byte[], byte[]> latestPerKey) {
        Map<ChannelId, Long> covered = new HashMap<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length > 0 && key[0] == StoreCodec.TAG_FED_UP_TO) {
                covered.put(StoreCodec.channelOfEntryKey(key), StoreCodec.decodeLong(value));
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
    public static Map<String, UUID> nameBindings(Map<byte[], byte[]> latestPerKey) {
        Map<String, UUID> bindings = new HashMap<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length > 1 && key[0] == StoreCodec.TAG_NAME_BINDING
                    && value.length == ChannelId.ENCODED_LENGTH) {
                String name = new String(key, 1, key.length - 1, StandardCharsets.UTF_8);
                bindings.put(name, ChannelId.readFrom(ByteBuffer.wrap(value)).topicId());
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
    public static List<String> identityChangedTopics(Map<byte[], byte[]> latestPerKey,
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
