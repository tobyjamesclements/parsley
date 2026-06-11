package io.parsley.kafka.buffer;

import io.parsley.BufferingPolicy;
import io.parsley.VectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

/**
 * Factory for creating {@link CausalBuffer} instances.
 *
 * <p>The concrete buffer implementation is chosen based on the supplied {@link BufferingPolicy}:
 * <ul>
 *   <li>{@link io.parsley.BufferingPolicy.Ignore Ignore} — creates an {@code IgnoreBuffer}
 *       that forwards evicted records out-of-order
 *   <li>{@link io.parsley.BufferingPolicy.Drop Drop} — creates a {@code DropBuffer}
 *       that silently discards evicted records
 *   <li>{@link io.parsley.BufferingPolicy.DeadLetter DeadLetter} — creates a
 *       {@code DeadLetterBuffer} that routes evicted records to a dead-letter sink;
 *       requires the 3-argument overload
 * </ul>
 */
public final class CausalBuffers {

    private CausalBuffers() {}

    /**
     * Creates a {@link CausalBuffer} for {@link io.parsley.BufferingPolicy.Ignore Ignore} or
     * {@link io.parsley.BufferingPolicy.Drop Drop} policies.
     *
     * @param <K>        the record key type
     * @param <V>        the record value type
     * @param policy     the buffering policy; must be {@code Ignore} or {@code Drop}
     * @param serialiser the serialiser used to decode vector-clock headers in records
     * @return a new {@code CausalBuffer} appropriate for {@code policy}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} — use
     *                                  {@link #create(BufferingPolicy.DeadLetter, VectorClockSerialiser, Consumer)}
     *                                  instead
     */
    public static <K, V> CausalBuffer<K, V> create(BufferingPolicy policy, VectorClockSerialiser serialiser) {
        return switch (policy) {
            case BufferingPolicy.Ignore ignore -> new IgnoreBuffer<>(serialiser);
            case BufferingPolicy.Drop drop -> new DropBuffer<>(serialiser);
            case BufferingPolicy.DeadLetter dl -> throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use create(policy, serialiser, sink)");
        };
    }

    /**
     * Creates a {@link CausalBuffer} for a
     * {@link io.parsley.BufferingPolicy.DeadLetter DeadLetter} policy.
     *
     * <p>Evicted records are passed to {@code deadLetterSink} instead of being forwarded on
     * the primary stream.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param policy         the dead-letter buffering policy
     * @param serialiser     the serialiser used to decode vector-clock headers in records
     * @param deadLetterSink the consumer that receives evicted records for dead-lettering
     * @return a new {@code CausalBuffer} configured for dead-letter routing
     */
    public static <K, V> CausalBuffer<K, V> create(
            BufferingPolicy.DeadLetter policy,
            VectorClockSerialiser serialiser,
            Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        return new DeadLetterBuffer<>(serialiser, deadLetterSink);
    }
}
