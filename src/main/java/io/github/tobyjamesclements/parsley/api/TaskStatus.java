package io.github.tobyjamesclements.parsley.api;

import java.util.List;
import java.util.OptionalLong;

/**
 * The delivery state of one task of a process: what it holds, what each hold is waiting for,
 * and how much causal metadata it carries.
 *
 * <p>A task is the specification's process — partition {@link #partition()} of every topic
 * its process receives. The snapshot is taken on the task's own thread once per status
 * interval ({@link ParsleyConfig#statusInterval()}), so a reading is at most one interval
 * old; a task that has been reassigned away disappears from its process's status rather
 * than lingering stale.
 *
 * <p>A held message is not a failure: it is waiting for a cause, and {@link HeldChannel#blockers()}
 * names which one, with the position required and the position the channel has reached.
 *
 * @param partition        the partition this task receives from each of its process's topics
 * @param frontierChannels how many channels the causal frontier names
 * @param frontierBytes    the encoded width of the frontier, which every emission carries
 *                         and which {@link ParsleyConfig#metadataBudgetBytes()} bounds
 * @param heldMessages     how many received messages are waiting for a cause, over all channels
 * @param heldChannels     the channels with at least one held message, in channel order
 * @see Parsley#status()
 * @see ProcessStatus#tasks()
 */
public record TaskStatus(
        int partition,
        int frontierChannels,
        int frontierBytes,
        int heldMessages,
        List<HeldChannel> heldChannels) {

    /**
     * Copies the held channels and refuses null components.
     *
     * @throws IllegalArgumentException if {@code heldChannels} is null or contains null, or
     *         any count is negative
     */
    public TaskStatus {
        if (heldChannels == null) {
            throw new IllegalArgumentException("every component of a task status must be non-null");
        }
        if (partition < 0 || frontierChannels < 0 || frontierBytes < 0 || heldMessages < 0) {
            throw new IllegalArgumentException("task status counts must be non-negative");
        }
        if (heldChannels.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("held channels must not contain null");
        }
        heldChannels = List.copyOf(heldChannels);
    }

    /**
     * One channel with held messages.
     *
     * @param topic        the channel's topic
     * @param partition    the channel's partition
     * @param held         how many messages are held on it
     * @param headPosition the position of the oldest held message, the one the decision reads
     * @param blockers     every cause the head is still waiting for; empty means the head is
     *                     deliverable and will go on the next drain
     */
    public record HeldChannel(String topic, int partition, int held, long headPosition, List<Blocker> blockers) {
        /**
         * Copies the blockers and refuses null components.
         *
         * @throws IllegalArgumentException if {@code topic} or {@code blockers} is null, or
         *         {@code blockers} contains null
         */
        public HeldChannel {
            if (topic == null || blockers == null) {
                throw new IllegalArgumentException("every component of a held channel must be non-null");
            }
            if (blockers.stream().anyMatch(java.util.Objects::isNull)) {
                throw new IllegalArgumentException("blockers must not contain null");
            }
            blockers = List.copyOf(blockers);
        }
    }

    /**
     * One cause a held message is waiting for.
     *
     * @param topic            the cause's topic, which this process receives
     * @param partition        the cause's partition
     * @param requiredPosition the position that must settle before the message can deliver
     * @param settledPosition  the position that channel has settled to, or empty when nothing
     *                         is known of it yet
     */
    public record Blocker(String topic, int partition, long requiredPosition, OptionalLong settledPosition) {
        /**
         * Refuses null components.
         *
         * @throws IllegalArgumentException if {@code topic} or {@code settledPosition} is null
         */
        public Blocker {
            if (topic == null || settledPosition == null) {
                throw new IllegalArgumentException("every component of a blocker must be non-null");
            }
        }
    }
}
