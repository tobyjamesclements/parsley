package io.github.tobyjamesclements.parsley.core;

import java.util.Map;
import java.util.Set;

/**
 * What the broker currently says about the channels a process reads.
 *
 * <p>These facts are what let the engine distinguish a cause that has not arrived yet from
 * one that never will. Without them a process holding for a cause on an idle channel could
 * not tell whether waiting would terminate.
 *
 * @param committedNextRead per channel, the first position not yet settled as readable
 * @param logStart          per channel, the earliest position still retained
 * @param deadChannels      channels whose topic no longer exists
 * @param recreatedChannels channels whose topic exists under a new identity
 */
public record PositionFacts(
        Map<ChannelId, Long> committedNextRead,
        Map<ChannelId, Long> logStart,
        Set<ChannelId> deadChannels,
        Set<ChannelId> recreatedChannels) {

    /** Facts about nothing, used before the first refresh completes. */
    public static final PositionFacts EMPTY = new PositionFacts(Map.of(), Map.of(), Set.of(), Set.of());

    /**
     * Facts with no recreated channels.
     *
     * @param committedNextRead per channel, the first position not yet settled as readable
     * @param logStart          per channel, the earliest position still retained
     * @param deadChannels      channels whose topic no longer exists
     */
    public PositionFacts(Map<ChannelId, Long> committedNextRead, Map<ChannelId, Long> logStart,
                         Set<ChannelId> deadChannels) {
        this(committedNextRead, logStart, deadChannels, Set.of());
    }

    /** Defensively copies every collection. */
    public PositionFacts {
        committedNextRead = Map.copyOf(committedNextRead);
        logStart = Map.copyOf(logStart);
        deadChannels = Set.copyOf(deadChannels);
        recreatedChannels = Set.copyOf(recreatedChannels);
    }
}
