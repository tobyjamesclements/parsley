package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Establishes that ordering state can be read without an engine.
 *
 * <p>These are the questions a startup check asks of state left by a previous run.
 */
class OrderingStateInspectorTest {
    /** Finds live held entries, ignoring tombstones and other key classes. */
    @Test
    void findsLiveHeldEntriesAndIgnoresTombstonesAndOtherTags() {
        ChannelId held = new ChannelId(new UUID(4, 1), 2);
        ChannelId tombstoned = new ChannelId(new UUID(4, 2), 0);
        Map<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);
        latest.put(StoreCodec.heldKey(held, 7), new byte[] {1});
        latest.put(StoreCodec.heldKey(tombstoned, 3), null);
        latest.put(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, tombstoned), StoreCodec.encodeLong(5));
        latest.put(StoreCodec.versionKey(), new byte[] {1});

        assertEquals(Set.of(held), OrderingStateInspector.heldChannels(latest));
    }

    /** Reads name bindings across tasks by topic id. */
    @Test
    void readsNameBindingsAcrossTasksByTopicId() {
        UUID topicId = new UUID(4, 9);
        Map<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);

        latest.put(StoreCodec.channelNameKey("orders"), new ChannelId(topicId, 2).toBytes());
        latest.put(StoreCodec.channelNameKey("tombstoned"), null);
        latest.put(StoreCodec.versionKey(), new byte[] {1});

        assertEquals(Map.of("orders", topicId), OrderingStateInspector.nameBindings(latest));
    }

    /** Recreation under a still declared name is an identity change not a removal. */
    @Test
    void recreationUnderAStillDeclaredNameIsAnIdentityChangeNotARemoval() {
        UUID oldId = new UUID(4, 10);
        UUID newId = new UUID(4, 11);
        UUID stableId = new UUID(4, 12);
        Map<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);
        latest.put(StoreCodec.channelNameKey("recreated"), new ChannelId(oldId, 0).toBytes());
        latest.put(StoreCodec.channelNameKey("stable"), new ChannelId(stableId, 0).toBytes());
        latest.put(StoreCodec.heldKey(new ChannelId(oldId, 0), 5), new byte[] {1});

        assertEquals(java.util.List.of("recreated"), OrderingStateInspector.identityChangedTopics(
                latest, Map.of("recreated", newId, "stable", stableId)));
        assertEquals(java.util.List.of(), OrderingStateInspector.identityChangedTopics(
                latest, Map.of("recreated", oldId, "stable", stableId)),
                "an unchanged identity is not flagged");
        assertEquals(java.util.List.of(), OrderingStateInspector.identityChangedTopics(
                latest, Map.of("fresh", newId)),
                "a name never bound has no identity to have changed");
    }
}
