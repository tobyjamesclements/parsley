package io.parsley;

import java.util.Objects;

/**
 * An immutable identifier for a Kafka topic-partition.
 *
 * <p>Combines a {@code topic} name and a non-negative {@code partition} number to uniquely
 * identify a single Kafka partition. {@code Partition} is the key type used in a
 * {@link VectorClock} to track per-partition progress.
 *
 * @param topic     the Kafka topic name; must not be {@code null}
 * @param partition the zero-based partition index; must be non-negative
 */
public record Partition(String topic, int partition) {
    public Partition {
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) throw new IllegalArgumentException("partition must be non-negative");
    }
}
