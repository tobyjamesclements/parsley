package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Assigns partition {@code p} of every received topic to one member, so a task is partition
 * {@code p} of every topic its process receives (D114; the kafka-clients host's equivalent
 * of the Streams assignor's co-partitioning). Tasks are spread round-robin over the members
 * in member-id order, which every member computes identically from the same group view.
 *
 * <p>Instantiated reflectively by the consumer from {@code partition.assignment.strategy},
 * so it is public with a public no-argument constructor. Eager: a rebalance revokes
 * everything and reassigns, which keeps the runtime's revoke-commit-restore cycle simple.
 */
public final class CoPartitionAssignor implements ConsumerPartitionAssignor {

    /** Public for the consumer's reflective construction. */
    public CoPartitionAssignor() {
    }

    @Override
    public String name() {
        return "parsley-copartition";
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        List<String> members = new ArrayList<>(subscriptions.keySet());
        Collections.sort(members);
        TreeSet<String> topics = new TreeSet<>();
        subscriptions.values().forEach(subscription -> topics.addAll(subscription.topics()));

        Map<String, Integer> widths = new HashMap<>();
        int width = 0;
        for (String topic : topics) {
            List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
            int count = partitions == null ? 0 : partitions.size();
            widths.put(topic, count);
            width = Math.max(width, count);
        }

        Map<String, List<TopicPartition>> assigned = new HashMap<>();
        for (String member : members) {
            assigned.put(member, new ArrayList<>());
        }
        for (int partition = 0; partition < width; partition++) {
            String member = members.get(partition % members.size());
            List<String> subscribed = subscriptions.get(member).topics();
            for (String topic : topics) {
                if (partition < widths.get(topic) && subscribed.contains(topic)) {
                    assigned.get(member).add(new TopicPartition(topic, partition));
                }
            }
        }
        Map<String, Assignment> result = new HashMap<>();
        assigned.forEach((member, partitions) -> result.put(member, new Assignment(partitions)));
        return new GroupAssignment(result);
    }
}
