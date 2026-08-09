package io.github.tobyjamesclements.parsley.kafka;

import java.util.UUID;

/**
 * One resolved topic.
 *
 * @param topicId    the topic's identity as assigned by the broker
 * @param partitions how many partitions it has
 */
record TopicInfo(UUID topicId, int partitions) {
    /**
     * @param id a Kafka identifier
     * @return the same value as a {@link UUID}
     */
    static UUID toJavaUuid(org.apache.kafka.common.Uuid id) {
        return new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    /**
     * @param id a {@link UUID}
     * @return the same value as a Kafka identifier
     */
    static org.apache.kafka.common.Uuid toKafkaUuid(UUID id) {
        return new org.apache.kafka.common.Uuid(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }
}
