package io.github.tobyjamesclements.parsley.core;

import java.util.Map;
import java.util.Set;

/**
 * Builds engines for tests, including deliberately broken ones. The sabotage modes exist so the suite can prove it
 * would catch each violation (EVIDENCE.md): a test that stays green against a sabotaged engine is worse than no test.
 * This factory lives in the test tree; the public API cannot construct a sabotaged engine.
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
        DELIVER_PAST_DEAD_HOLDS
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
}
