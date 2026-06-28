package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;

/**
 * Resolves a topic name to its stable Kafka UUID — the identity {@link CausalDependencies} key their
 * coordinates by, so that a topic deleted and recreated under the same name is treated as a different
 * topic. A {@code ConsumerRecord} carries only the topic <em>name</em>; building a UUID-keyed
 * dependency from it (via {@link CausalDependencies#using} or {@link CausalDependencies#builder}) needs
 * this mapping.
 *
 * <h2>Usage</h2>
 * Back the resolver with a Kafka {@link Admin} the caller already owns; Parsley resolves UUIDs through
 * it and caches them, and <strong>never closes it</strong> — the caller keeps ownership:
 * <pre>{@code
 * try (Admin admin = Admin.create(adminConfig)) {
 *     CausalTopics topics = CausalTopics.of(admin);
 *     CausalDependencies deps = CausalDependencies.using(topics).observe(consumedRecord);
 *     producer.send(deps.stamp(new ProducerRecord<>("orders", key, value)));
 * }
 * }</pre>
 * Tests without a live broker can supply a fixed name&rarr;UUID map with {@link #of(Map)}.
 *
 * <h2>Thread safety</h2>
 * An {@link Admin}-backed resolver is safe to share across threads; resolved UUIDs are cached so a
 * topic is described at most once.
 */
@FunctionalInterface
public interface CausalTopics {

    /**
     * Resolves {@code topic} to its stable Kafka UUID.
     *
     * @param topic the topic name; must not be {@code null}
     * @return the topic's Kafka UUID
     * @throws IllegalArgumentException if no topic with that name exists
     */
    Uuid topicId(String topic);

    /**
     * Returns a resolver backed by a caller-owned {@link Admin}. Parsley describes each topic on first
     * use and caches the UUID; it <strong>never closes</strong> {@code admin}.
     *
     * @param admin the Kafka admin client to resolve UUIDs through; must not be {@code null}
     * @return an {@code Admin}-backed resolver
     */
    static CausalTopics of(Admin admin) {
        Objects.requireNonNull(admin, "admin must not be null");
        return new ParsleyCausalTopics(ParsleyTopicAdmin.ofAdmin(admin));
    }

    /**
     * Returns a resolver over a fixed name&rarr;UUID map — the broker-free path for tests or callers
     * that already hold the UUIDs.
     *
     * @param ids the topic names mapped to their Kafka UUIDs; must not be {@code null}
     * @return a resolver over a defensive copy of {@code ids}
     */
    static CausalTopics of(Map<String, Uuid> ids) {
        Objects.requireNonNull(ids, "ids must not be null");
        Map<String, Uuid> copy = Map.copyOf(ids);
        return topic -> {
            Uuid id = copy.get(Objects.requireNonNull(topic, "topic must not be null"));
            if (id == null) {
                throw new IllegalArgumentException("unknown topic '" + topic + "'");
            }
            return id;
        };
    }
}
