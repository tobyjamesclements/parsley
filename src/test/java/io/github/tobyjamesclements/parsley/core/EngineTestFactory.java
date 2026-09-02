package io.github.tobyjamesclements.parsley.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds engines and messages for the core tests.
 */
public final class EngineTestFactory {
    public enum SabotageMode {
        NONE,
        IGNORE_CAUSES,
        NO_FIFO,
        REDELIVER_REFEEDS,
        UNDECODABLE_AS_ABSENT,
        SKIP_RECEIPT_MERGE,
        DROP_HELD,
        IGNORE_TRUNCATION,
        IGNORE_REMOVED_CHANNELS,
        SILENT_DROP,
        OVEREXPRESS,
        IGNORE_RECREATION,
        DELIVER_PAST_DEAD_HOLDS,
        TREAT_COVERED_FEED_AS_REPLAY
    }

    private EngineTestFactory() {
    }

    public static ProcessEngine create(
            String processName, Map<ChannelId, String> receivedChannels, OrderingStore store, SabotageMode mode) {
        Sabotage sabotage = mode == SabotageMode.NONE
                ? Sabotage.NONE
                : new Sabotage(Set.of(Sabotage.Mode.valueOf(mode.name())));
        return new ProcessEngine(processName, receivedChannels, store, sabotage);
    }

    /**
     * A received message with no causal stamp: the uid doubles as key and value, and the
     * position doubles as the offset.
     */
    public static ReceivedMessage plain(ChannelId channel, long position, String uid) {
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(), List.of());
    }
}
