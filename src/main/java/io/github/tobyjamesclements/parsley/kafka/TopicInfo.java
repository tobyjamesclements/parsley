package io.github.tobyjamesclements.parsley.kafka;

import java.util.UUID;

/** A declared topic as resolved against the cluster: its identity and partition count (SPEC Assumption 2, 14). */
record TopicInfo(UUID topicId, int partitions) {

    /** The one place the topic-id bit shuffling lives: a transposed most/least pair would corrupt channel
     * identity, the property the whole facts machinery defends. */
    static UUID toJavaUuid(org.apache.kafka.common.Uuid id) {
        return new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    static org.apache.kafka.common.Uuid toKafkaUuid(UUID id) {
        return new org.apache.kafka.common.Uuid(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }
}
