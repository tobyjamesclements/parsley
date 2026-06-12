package io.parsley.kafka;

import io.parsley.VectorClock;
import io.parsley.VectorClocks;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Kafka-specific implementation of {@link VectorClock} backed by a {@link TopicPartition} map.
 *
 * <p>Tracks causal progress as a map from each Kafka {@link TopicPartition} to the highest
 * offset observed from that partition. Use {@link #advance} to produce a new clock with an
 * updated offset, and {@link #satisfiedBy} to test whether a frontier clock has seen at least
 * everything this clock requires.
 *
 * @param positions the partition-to-offset map; copied defensively
 */
public record KafkaVectorClock(Map<TopicPartition, Long> positions) implements VectorClock<KafkaVectorClock> {

    /**
     * Returns an empty clock with no partition positions recorded.
     *
     * @return an empty {@code KafkaVectorClock} with no partition positions
     */
    public static KafkaVectorClock empty() {
        return new KafkaVectorClock(Map.of());
    }

    /**
     * Canonical constructor; defensively copies {@code positions}.
     */
    public KafkaVectorClock(Map<TopicPartition, Long> positions) {
        this.positions = Map.copyOf(positions);
    }

    /**
     * Returns a new clock with {@code tp} advanced to {@code max(current, offset)}.
     *
     * @param tp     the topic-partition to advance
     * @param offset the new observed offset
     * @return a new {@code KafkaVectorClock} with the updated position
     */
    public KafkaVectorClock advance(TopicPartition tp, long offset) {
        return new KafkaVectorClock(VectorClocks.advance(positions, tp, offset));
    }

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything this clock
     * requires — i.e. for every partition in this clock, the frontier's offset is ≥ this
     * clock's offset. Partitions absent from {@code frontier} are unsatisfied.
     */
    @Override
    public boolean satisfiedBy(KafkaVectorClock frontier) {
        return VectorClocks.satisfied(positions, frontier.positions());
    }

    /**
     * Returns a new clock that is the causal union of this and {@code other},
     * taking the max offset per partition.
     */
    @Override
    public KafkaVectorClock merge(KafkaVectorClock other) {
        return new KafkaVectorClock(VectorClocks.merge(positions, other.positions()));
    }

    @Override
    public String toString() {
        return "KafkaVectorClock" + positions;
    }
}
