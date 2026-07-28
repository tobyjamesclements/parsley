package io.github.tobyjamesclements.parsley.core;

import java.util.Set;
import java.util.UUID;

/**
 * Static configuration of one causal node (one task: one partition group of a processing
 * stage).
 *
 * @param nodeId stable identifier for logs and diagnostics
 * @param senderId stable identity for this node's sequence claims; must survive restarts (a
 *     changed sender identity orphans the claims of the previous one)
 * @param consumed the channels this node consumes (its declared input topics at its own task
 *     partition)
 * @param sinkTopics the topic UUIDs this node may produce to
 * @param taskPartition the task's own partition number
 */
public record NodeConfig(
        String nodeId,
        UUID senderId,
        Set<Channel> consumed,
        Set<UUID> sinkTopics,
        int taskPartition) {

    public NodeConfig {
        if (senderId == null) throw new NullPointerException("senderId");
        consumed = Set.copyOf(consumed);
        sinkTopics = Set.copyOf(sinkTopics);
    }
}
