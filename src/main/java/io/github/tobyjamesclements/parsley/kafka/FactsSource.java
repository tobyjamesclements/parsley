package io.github.tobyjamesclements.parsley.kafka;

import java.util.Map;
import java.util.Set;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

/**
 * Supplies the broker's current view of the channels a process reads.
 *
 * <p>This is the seam behind which the admin client sits, so a topology can be driven
 * without a broker.
 *
 * @see AdminFactsSource
 */
interface FactsSource {
    /**
     * Gathers facts for one process.
     *
     * @param receivedChannels  the channels the process receives
     * @param fedUpToHints      per channel, the highest position already fed, which bounds
     *                          how far a query needs to look
     * @param frontierChannels  channels named by the frontier, which may lie outside the
     *                          received set
     * @return the broker's view
     * @throws Exception if the broker cannot be queried
     */
    PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                         Set<ChannelId> frontierChannels) throws Exception;
}
