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
        IGNORE_REMOVED_CHANNELS,
        SILENT_DROP,
        OVEREXPRESS,
        IGNORE_RECREATION,
        DELIVER_PAST_DEAD_HOLDS
    }

    private EngineTestFactory() {
    }

    public static ProcessEngine create(
            String processName, Map<ChannelId, String> receivedChannels, OrderingStore store, SabotageMode mode) {
        return create(processName, receivedChannels, store, mode, Map.of());
    }

    /**
     * Builds an engine under a sabotage mode, with the positions the host feeds from (SPEC
     * Host obligation 2): the simulated host passes its committed read positions here the
     * way the Kafka host passes the bootstrap's.
     */
    public static ProcessEngine create(
            String processName, Map<ChannelId, String> receivedChannels, OrderingStore store, SabotageMode mode,
            Map<ChannelId, Long> startPositions) {
        Sabotage sabotage = mode == SabotageMode.NONE
                ? Sabotage.NONE
                : new Sabotage(Set.of(Sabotage.Mode.valueOf(mode.name())));
        return new ProcessEngine(processName, receivedChannels, store,
                ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES, sabotage, startPositions);
    }

    /**
     * A received message with no causal stamp: the uid doubles as key and value, and the
     * position doubles as the offset.
     */
    public static ReceivedMessage plain(ChannelId channel, long position, String uid) {
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(), List.of());
    }
}
