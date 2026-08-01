package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Resolves topic names to their stable Kafka UUIDs, the identity half of a causal channel.
 *
 * <p>Production code uses {@link #fromAdmin}. The broker-less test wiring synthesizes a
 * resolver instead.
 */
interface TopicIds {

    /**
     * Resolves one topic name.
     *
     * <p>Resolution is strict, because starting with identity unresolved would bind
     * coordinates to nothing.
     *
     * @param topic the topic name to resolve
     * @return the topic's UUID and partition count
     * @throws IllegalStateException when the topic cannot be resolved
     */
    Resolved resolve(String topic);

    /**
     * One topic's resolved identity.
     *
     * @param id the topic's stable UUID, as the broker assigns it
     * @param partitions how many partitions the topic has
     */
    record Resolved(UUID id, int partitions) {}

    /**
     * Returns a resolver backed by a Kafka admin client, caching per name for its lifetime.
     *
     * <p>Not thread-safe. Use one resolver per thread.
     *
     * @param admin the admin client to describe topics through, owned by the caller
     * @return the caching resolver
     */
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
