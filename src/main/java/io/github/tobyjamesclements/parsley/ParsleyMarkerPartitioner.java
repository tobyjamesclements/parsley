package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.Set;

/**
 * A Decorator (GoF) over a stage's own sink {@link StreamPartitioner} (or the default key-hash
 * partitioner, if the stage declared none), routing a Parsley null message to the forwarding task's
 * own owned partition —
 * {@link ParsleyMarkerPartition#get()} — instead of whatever the wrapped partitioner would compute from
 * the message's key. {@link CausalTopology} installs one of these on every sink a stage declares, in place
 * of {@code stage.partitioner} directly, so a null-message forward — {@code advertise} in
 * {@link ParsleyProcessor}, funnelled through
 * {@code forwardToSinks} — always reaches the correct partition, never depending on a business key being
 * available or on the wrapped partitioner happening to preserve the
 * co-partitioning contract for a null message's borrowed key.
 *
 * <p>A business forward is untouched: {@link ParsleyMarkerPartition#get()} is non-null only for the
 * duration of a marker's own {@code context.forward} call, so every other record still routes through the
 * wrapped partitioner exactly as if this wrapper were not installed — including falling back to Kafka's
 * own default partitioner when {@code delegate} is {@code null}, since returning {@link Optional#empty()}
 * is that fallback's documented trigger.
 *
 * @param <K> the sink's key type
 * @param <V> the sink's value type
 */
final class ParsleyMarkerPartitioner<K, V> implements StreamPartitioner<K, V> {

    private final @Nullable StreamPartitioner<? super K, ? super V> delegate;

    ParsleyMarkerPartitioner(@Nullable StreamPartitioner<? super K, ? super V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, K key, V value, int numPartitions) {
        Integer explicit = ParsleyMarkerPartition.get();
        if (explicit != null) {
            return Optional.of(Set.of(explicit));
        }
        return delegate != null ? delegate.partitions(topic, key, value, numPartitions) : Optional.empty();
    }
}
