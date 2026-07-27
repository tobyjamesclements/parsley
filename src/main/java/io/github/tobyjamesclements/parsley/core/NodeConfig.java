package io.github.tobyjamesclements.parsley.core;

import java.util.Set;
import java.util.UUID;

/**
 * Static configuration of one causal node (one task: one partition group of a processing
 * stage).
 *
 * @param nodeId stable identifier for logs and diagnostics
 * @param consumed the channels this node consumes (its declared input topics at its own task
 *     partition)
 * @param sinkTopics the topic UUIDs this node may produce to
 * @param taskPartition the task's own partition number; null messages route to this partition
 *     (modulo each sink's partition count) so per-partition progress coverage is deterministic
 * @param maxHeldPerChannel hold-queue depth above which {@code pauseWanted} turns on for a
 *     channel (backpressure, never a safety bound — records are never dropped)
 */
public record NodeConfig(
        String nodeId,
        Set<Channel> consumed,
        Set<UUID> sinkTopics,
        int taskPartition,
        int maxHeldPerChannel) {

    public NodeConfig {
        consumed = Set.copyOf(consumed);
        sinkTopics = Set.copyOf(sinkTopics);
        if (maxHeldPerChannel < 1) throw new IllegalArgumentException("maxHeldPerChannel " + maxHeldPerChannel);
    }
}
