package io.github.tobyjamesclements.parsley.kafka;

import java.util.Set;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

/**
 * Supplies the broker's current view of the channels a process reads.
 *
 * <p>This is the seam behind which the admin client sits, so a topology can be driven
 * without a broker.
 *
 * <p>Two rounds are distinguished. The seed round at task initialisation carries the
 * group's committed read positions, which are the baseline below which every position
 * counts as already satisfied (SPEC Structural 12): a process started at the latest
 * position, or a channel joining the received set, has received nothing on a channel and
 * would otherwise hold for causes below where it began reading. Background rounds carry
 * log starts and topic identity only. No read-position report is needed after the seed,
 * because every cause names a delivered record's position, and receiving that record, or
 * the first surviving record above it, is what settles the cause (D114).
 *
 * @see AdminFactsSource
 */
interface FactsSource {
    /**
     * Gathers the background facts for one process: log starts and topic identity, and
     * no committed read positions.
     *
     * @param receivedChannels the channels the process receives
     * @param frontierChannels channels named by the frontier, which may lie outside the
     *                         received set
     * @return the broker's view
     * @throws Exception if the broker cannot be queried
     */
    PositionFacts gather(Set<ChannelId> receivedChannels, Set<ChannelId> frontierChannels) throws Exception;

    /**
     * Gathers facts for a task's startup seed, including the group's committed read
     * positions, without waiting unboundedly for the source.
     *
     * <p>Task initialisation runs on the stream thread, so a source busy with a slow broker
     * must not stack every initialising task behind it. An implementation may return
     * {@link PositionFacts#EMPTY} when it cannot answer within a bounded wait; facts are
     * per-position lower bounds, so starting unseeded merely defers the baseline until the
     * channel's first receipt.
     *
     * @param receivedChannels the channels the process receives
     * @param frontierChannels channels named by the frontier
     * @return the broker's view, or {@link PositionFacts#EMPTY} if the source is busy
     * @throws Exception if the broker cannot be queried
     */
    default PositionFacts gatherForSeed(Set<ChannelId> receivedChannels, Set<ChannelId> frontierChannels)
            throws Exception {
        return gather(receivedChannels, frontierChannels);
    }
}
