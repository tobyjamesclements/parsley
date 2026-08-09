package io.github.tobyjamesclements.parsley.kafka;

import java.util.Map;
import java.util.Set;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

/**
 * Supplies position facts — read-position reports, the substrate's earliest retained positions, and which channels
 * still exist (SPEC Host obligation 2, Assumption 15). Facts are data fed into ordering state; the deliverability
 * decision itself stays a pure function (SPEC Structural 7). Suppliers may return stale facts freely: every fact is
 * a lower bound, so staleness delays progress and pruning but never unblocks anything early.
 */
interface FactsSource {

    /**
     * @param receivedChannels the channels the calling process receives from (read positions and log starts wanted)
     * @param fedUpToHints     per received channel, the caller's fed-or-never-arriving frontier; non-empty only when
     *                         messages are held, inviting the supplier to probe whether the positions just above a
     *                         hint will ever yield messages (SPEC Liveness 3 — the host's committed offsets alone
     *                         do not advance over a trailing never-yielding run the current execution never read)
     * @param frontierChannels the channels the causal frontier currently names (log starts and existence wanted)
     * @throws Exception on any failure to gather; the caller skips the round and retries later
     */
    PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                         Set<ChannelId> frontierChannels) throws Exception;
}
