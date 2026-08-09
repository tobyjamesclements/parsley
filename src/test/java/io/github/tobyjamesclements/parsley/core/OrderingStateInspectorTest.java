package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The runtime's stranded-held check reads the changelog through this interpretation (SPEC Structural 16). */
class OrderingStateInspectorTest {

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

    @Test
    void readsNameBindingsAcrossTasksByTopicId() {
        UUID topicId = new UUID(4, 9);
        Map<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);
        // Bindings are written per task: several partitions of one topic bind the same name; the topic id agrees.
        latest.put(StoreCodec.channelNameKey("orders"), new ChannelId(topicId, 2).toBytes());
        latest.put(StoreCodec.channelNameKey("tombstoned"), null);
        latest.put(StoreCodec.versionKey(), new byte[] {1});

        assertEquals(Map.of("orders", topicId), OrderingStateInspector.nameBindings(latest));
    }

    /**
     * A topic deleted and recreated under a still-declared name must be diagnosed as the identity change it is —
     * with D33's deliberate-reset remedy — not as a declaration change (ASSESSMENT 1.3: the held entries carry the
     * old identity, so a naive held-versus-declared comparison misreports "channel removed" though nothing was
     * removed).
     */
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
