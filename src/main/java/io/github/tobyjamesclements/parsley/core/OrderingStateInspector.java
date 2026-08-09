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
 * Read-only interpretation of ordering-state entries, for the runtime's start-time checks. A declaration that
 * removes a channel with undelivered held messages must be refused (SPEC Structural 16) even when the removal
 * shrinks the task set so far that the task owning the held messages would never be instantiated to refuse it
 * itself — the runtime inspects the ordering store's changelog instead. The same view carries the name-to-identity
 * bindings (D33), so a topic recreated under a still-declared name is diagnosed as the identity change it is,
 * never as a declaration change (ASSESSMENT 1.3).
 */
public final class OrderingStateInspector {

    private OrderingStateInspector() {
    }

    /**
     * The channels with live held-message entries, given the latest value per ordering-store key (a compacted view
     * of the store's changelog: tombstoned keys must be absent or mapped to null).
     */
    public static Set<ChannelId> heldChannels(Map<byte[], byte[]> latestPerKey) {
        Set<ChannelId> channels = new TreeSet<>();
        latestPerKey.forEach((key, value) -> {
            if (value != null && key.length == 1 + ChannelId.ENCODED_LENGTH + Long.BYTES
                    && key[0] == StoreCodec.TAG_HELD) {
                channels.add(StoreCodec.channelOfKey(key));
            }
        });
        return channels;
    }

    /**
     * The topic identity recorded for each declared name (D33), given the latest value per ordering-store key.
     * Bindings are written per task, so partitions differ across entries of one name in an aggregated changelog
     * view; the topic id — the identity that matters — is the same for all of them.
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
     * The declared topic names whose recorded identity no longer matches the current resolution: each was deleted
     * and recreated under its name since this process's state was built, so its name-keyed read positions belong
     * to a dead channel and its held entries carry the old identity (SPEC Assumption 2; D33).
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
