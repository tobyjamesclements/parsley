package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Resolves topic names to their stable Kafka UUIDs — the identity half of a causal channel.
 * Production code uses {@link #fromAdmin}; tests inject a fixed mapping (TopologyTestDriver has
 * no broker to ask).
 */
public interface TopicIds {

    /**
     * @return the topic's UUID and partition count
     * @throws IllegalStateException when the topic cannot be resolved — strict, because
     *     starting with identity unresolved would bind coordinates to nothing
     */
    Resolved resolve(String topic);

    record Resolved(UUID id, int partitions) {}

    static TopicIds fixed(Map<String, Resolved> mapping) {
        Map<String, Resolved> copy = Map.copyOf(mapping);
        return topic -> {
            Resolved r = copy.get(topic);
            if (r == null) throw new IllegalStateException("unresolvable topic: " + topic);
            return r;
        };
    }

    /** Resolves through a Kafka admin client, caching per name for the resolver's lifetime. */
    static TopicIds fromAdmin(Admin admin) {
        Map<String, Resolved> cache = new HashMap<>();
        return topic -> cache.computeIfAbsent(topic, t -> {
            try {
                TopicDescription d = admin.describeTopics(Set.of(t)).allTopicNames().get().get(t);
                // Kafka's Uuid serializes as base64, not the dashed form — convert by bits.
                UUID id = new UUID(d.topicId().getMostSignificantBits(),
                        d.topicId().getLeastSignificantBits());
                return new Resolved(id, d.partitions().size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted resolving topic " + t, e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("cannot resolve topic " + t, e.getCause());
            }
        });
    }
}
