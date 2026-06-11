package io.parsley.kafka;

import io.parsley.VectorClock;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Kafka-specific implementation of {@link VectorClock} backed by a {@link TopicPartition} map.
 *
 * <p>Tracks causal progress as a map from each Kafka {@link TopicPartition} to the highest
 * offset observed from that partition. Use {@link #advance} to produce a new clock with an
 * updated offset, and {@link #satisfiedBy} to test whether a frontier clock has seen at least
 * everything this clock requires.
 */
public record KafkaVectorClock(Map<TopicPartition, Long> positions) implements VectorClock {

    /**
     * Returns an empty clock with no partition positions recorded.
     *
     * @return an empty {@code KafkaVectorClock} with no partition positions
     */
    public static KafkaVectorClock empty() {
        return new KafkaVectorClock(Map.of());
    }

    public KafkaVectorClock(Map<TopicPartition, Long> positions) {
        this.positions = Map.copyOf(positions);
    }

    /**
     * Returns the underlying partition-to-offset map.
     *
     * @return an unmodifiable map from {@link TopicPartition} to observed offset
     */
    @Override
    public Map<TopicPartition, Long> positions() {
        return positions;
    }

    /**
     * Returns a new clock with {@code tp} advanced to {@code max(current, offset)}.
     *
     * @param tp     the topic-partition to advance
     * @param offset the new observed offset
     * @return a new {@code KafkaVectorClock} with the updated position
     */
    public KafkaVectorClock advance(TopicPartition tp, long offset) {
        Map<TopicPartition, Long> next = new HashMap<>(positions);
        next.merge(tp, offset, Math::max);
        return new KafkaVectorClock(next);
    }

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything this clock
     * requires — i.e. for every partition in this clock, the frontier's offset is ≥ this
     * clock's offset.
     *
     * <p>Partitions absent from {@code frontier} are treated as offset {@code -1}.
     */
    @Override
    public boolean satisfiedBy(VectorClock frontier) {
        KafkaVectorClock f = (KafkaVectorClock) frontier;
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            if (f.positions.getOrDefault(entry.getKey(), -1L) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a new clock that is the causal union of this and {@code other},
     * taking the max offset per partition.
     */
    @Override
    public VectorClock merge(VectorClock other) {
        KafkaVectorClock o = (KafkaVectorClock) other;
        Map<TopicPartition, Long> merged = new HashMap<>(positions);
        for (Map.Entry<TopicPartition, Long> entry : o.positions.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return new KafkaVectorClock(merged);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof KafkaVectorClock(Map<TopicPartition, Long> positions1) && positions.equals(positions1);
    }

    @Override
    public String toString() {
        return "KafkaVectorClock" + positions;
    }
}
