package io.parsley.core;

import io.parsley.BufferingPolicy;
import io.parsley.CausalBuffer;
import io.parsley.CausalRecord;
import io.parsley.VectorClock;
import io.parsley.core.internal.buffer.DeadLetterBuffer;
import io.parsley.core.internal.buffer.DropBuffer;
import io.parsley.core.internal.buffer.IgnoreBuffer;

import java.util.function.Consumer;

/**
 * Factory for creating {@link CausalBuffer} instances.
 *
 * <p>The concrete buffer implementation is chosen based on the supplied {@link BufferingPolicy}:
 * <ul>
 *   <li>{@link io.parsley.BufferingPolicy.Ignore Ignore} — evicted records are returned for
 *       out-of-order forwarding
 *   <li>{@link io.parsley.BufferingPolicy.Drop Drop} — evicted records are silently discarded
 *   <li>{@link io.parsley.BufferingPolicy.DeadLetter DeadLetter} — evicted records are routed
 *       to a dead-letter sink; requires the 2-argument overload
 * </ul>
 */
public final class CausalBuffers {

    private CausalBuffers() {}

    /**
     * Creates a {@link CausalBuffer} for {@link io.parsley.BufferingPolicy.Ignore Ignore} or
     * {@link io.parsley.BufferingPolicy.Drop Drop} policies.
     *
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @param <T>    the concrete {@link VectorClock} type
     * @param policy the buffering policy; must be {@code Ignore} or {@code Drop}
     * @return a new {@code CausalBuffer} appropriate for {@code policy}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} — use
     *                                  {@link #create(BufferingPolicy.DeadLetter, Consumer)}
     *                                  instead
     */
    public static <K, V, T extends VectorClock<T>> CausalBuffer<K, V, T> create(BufferingPolicy policy) {
        return switch (policy) {
            case BufferingPolicy.Ignore ignore -> new IgnoreBuffer<>();
            case BufferingPolicy.Drop drop -> new DropBuffer<>();
            case BufferingPolicy.DeadLetter dl -> throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use create(policy, sink)");
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
     * @param <T>            the concrete {@link VectorClock} type
     * @param policy         the dead-letter buffering policy
     * @param deadLetterSink the consumer that receives evicted records for dead-lettering
     * @return a new {@code CausalBuffer} configured for dead-letter routing
     */
    public static <K, V, T extends VectorClock<T>> CausalBuffer<K, V, T> create(
            BufferingPolicy.DeadLetter policy,
            Consumer<CausalRecord<K, V, T>> deadLetterSink) {
        return new DeadLetterBuffer<>(deadLetterSink);
    }
}
