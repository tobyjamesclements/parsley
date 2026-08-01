package io.github.tobyjamesclements.parsley;

import java.util.Set;
import java.util.UUID;

/**
 * Static configuration of one causal node, which is one task, the partition group of one
 * processing stage.
 *
 * @param nodeId stable identifier for logs and diagnostics
 * @param senderId stable identity for this node's sequence claims, which must survive
 *     restarts, since a changed sender identity orphans the claims of the previous one
 * @param consumed the channels this node consumes, its declared input topics at its own task
 *     partition
 * @param sinkTopics the topic UUIDs this node may produce to
 * @param taskPartition the task's own partition number
 */
record NodeConfig(
        String nodeId,
        UUID senderId,
        Set<Channel> consumed,
        Set<UUID> sinkTopics,
        int taskPartition) {

    /**
     * Copies the channel and sink sets so the configuration is immutable.
     *
     * @throws NullPointerException if {@code senderId} is null
     */
    NodeConfig {
        if (senderId == null) throw new NullPointerException("senderId");
        consumed = Set.copyOf(consumed);
        sinkTopics = Set.copyOf(sinkTopics);
    }
}
