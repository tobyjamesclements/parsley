package io.github.tobyjamesclements.parsley.core;

import java.util.Map;
import java.util.Set;

/**
 * Facts about positions, reported by the host and substrate and fed into the engine as data (SPEC Host obligation 2,
 * Assumption 15). The deliverability decision itself stays pure: these facts mutate ordering state before it runs.
 *
 * @param committedNextRead per channel, the host's reported read position: every position below it has been fed to
 *                          this process or will never arrive as a message
 * @param logStart          per channel, the earliest retained position
 * @param deadChannels      channels whose topic no longer exists (topic IDs are never reused, so death is terminal)
 * @param recreatedChannels channels whose topic name now resolves to a different topic ID: the topic was deleted
 *                          and recreated under the same name while this process ran. Affirmative evidence — a stale
 *                          metadata view can serve the old binding but cannot invent the new one — so the old
 *                          channel is definitively dead, and a received channel in this set means the feed path can
 *                          no longer be trusted to carry the old channel (SPEC Assumption 2)
 */
public record PositionFacts(
        Map<ChannelId, Long> committedNextRead,
        Map<ChannelId, Long> logStart,
        Set<ChannelId> deadChannels,
        Set<ChannelId> recreatedChannels) {

    public static final PositionFacts EMPTY = new PositionFacts(Map.of(), Map.of(), Set.of(), Set.of());

    public PositionFacts(Map<ChannelId, Long> committedNextRead, Map<ChannelId, Long> logStart,
                         Set<ChannelId> deadChannels) {
        this(committedNextRead, logStart, deadChannels, Set.of());
    }

    public PositionFacts {
        committedNextRead = Map.copyOf(committedNextRead);
        logStart = Map.copyOf(logStart);
        deadChannels = Set.copyOf(deadChannels);
        recreatedChannels = Set.copyOf(recreatedChannels);
    }
}
