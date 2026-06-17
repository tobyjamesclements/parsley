package io.parsley;

import org.apache.kafka.common.Uuid;

import java.nio.charset.StandardCharsets;

/**
 * A neutral coordinate in the causal log: a topic (identified by its Kafka UUID), a partition, and
 * an offset. The same type serves as a frontier entry ("I have observed this far") and as a
 * dependency entry ("I require at least this position").
 *
 * <p>Topic UUIDs are the stable clock key across topic deletion and recreation. Use
 * {@link #deriveUuid(String)} to derive a deterministic UUID from a topic name when no live
 * AdminClient UUID is available (unit tests, {@code TopologyTestDriver}).
 *
 * @param topicId   the Kafka topic UUID
 * @param partition the partition index
 * @param offset    the offset (inclusive)
 */
public record CausalPosition(Uuid topicId, int partition, long offset) {

    /**
     * Returns a deterministic UUID derived from {@code topicName} via
     * {@link java.util.UUID#nameUUIDFromBytes}. Useful in tests and in paths where a live
     * AdminClient UUID is unavailable — the result is stable for a given topic name but is NOT the
     * UUID Kafka assigns (and will collide if a topic is deleted and recreated under the same name).
     *
     * @param topicName the topic name; must not be {@code null}
     * @return a deterministic, name-derived UUID
     */
    public static Uuid deriveUuid(String topicName) {
        java.util.UUID jvm = java.util.UUID.nameUUIDFromBytes(
                topicName.getBytes(StandardCharsets.UTF_8));
        return new Uuid(jvm.getMostSignificantBits(), jvm.getLeastSignificantBits());
    }
}
