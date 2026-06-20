package io.parsley;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * A topic's stable causal identity: its name paired with its Kafka-assigned UUID.
 *
 * <p>Supply one of these per input topic to {@link CausalConsumers.Builder#addCausalTopic} or
 * {@link CausalProcessors.Builder#addCausalTopic} so Parsley can key frontiers and dependencies by
 * UUID rather than name — the UUID is what stays stable across a topic's deletion and recreation;
 * the name does not. Typically sourced from {@code AdminClient.describeTopics(topics)}, or from
 * {@link CausalPosition#deriveUuid} in tests without a live broker.
 *
 * @param topic   the topic name
 * @param topicId the Kafka UUID Parsley should treat as this topic's identity
 */
public record CausalTopic(String topic, Uuid topicId) {

    public CausalTopic {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(topicId, "topicId must not be null");
    }
}
