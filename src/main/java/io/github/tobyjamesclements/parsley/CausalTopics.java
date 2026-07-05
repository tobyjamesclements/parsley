package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Resolves a topic name to its stable Kafka UUID — the identity {@link CausalDependencies} keys their
 * coordinates by, so that a topic deleted and recreated under the same name is treated as a different
 * topic. A {@code ConsumerRecord} carries only the topic <em>name</em>; building a UUID-keyed
 * dependency from it (via {@link CausalDependencies#using} or {@link CausalDependencies#builder}) needs
 * this mapping. Internal to {@link CausalDependencies}, which is the public entry point — callers never
 * construct or hold a {@code CausalTopics} directly.
 *
 * <h2>Thread safety</h2>
 * A resolver is safe to share across threads; resolved UUIDs are cached so a topic is described at
 * most once.
 */
@FunctionalInterface
interface CausalTopics {

    /**
     * Resolves {@code topic} to its stable Kafka UUID.
     *
     * @param topic the topic name; must not be {@code null}
     * @return the topic's Kafka UUID
     * @throws IllegalArgumentException if no topic with that name exists
     */
    Uuid topicId(String topic);

    /**
     * Returns a resolver backed by {@code props}. Nothing is resolved eagerly: each distinct topic name
     * is described (and its UUID cached) the first time {@link #topicId} is called for it, through a
     * fresh, short-lived Kafka admin client opened and closed for that one lookup — so the resolver
     * holds no live connection between calls and needs no explicit lifecycle of its own.
     *
     * @param props the Kafka client configuration to resolve through; must not be {@code null}
     * @return a {@code props}-backed resolver
     */
    static CausalTopics of(Properties props) {
        Objects.requireNonNull(props, "props must not be null");
        return new ParsleyTopics(props);
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
