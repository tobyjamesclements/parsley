package io.parsley.stream;

import io.parsley.BufferingPolicy;

import java.util.function.Consumer;

/**
 * Factory for {@link CausalBuffer} instances, choosing the implementation from the
 * {@link BufferingPolicy}.
 */
final class CausalBuffers {

    private CausalBuffers() {}

    /**
     * Creates a buffer for a {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link BufferingPolicy.Drop Drop} policy.
     *
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <K, V> CausalBuffer<K, V> create(BufferingPolicy policy) {
        return switch (policy) {
            case BufferingPolicy.ForwardUnsafe forwardUnsafe -> new ForwardUnsafeBuffer<>();
            case BufferingPolicy.Drop drop -> new DropBuffer<>();
            case BufferingPolicy.DeadLetter dl -> throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use create(policy, sink)");
        };
    }

    /**
     * Creates a buffer for a {@link BufferingPolicy.DeadLetter DeadLetter} policy, routing
     * evicted records to {@code deadLetterSink}.
     */
    static <K, V> CausalBuffer<K, V> create(
            BufferingPolicy.DeadLetter policy,
            Consumer<CausalRecord<K, V>> deadLetterSink) {
        return new DeadLetterBuffer<>(deadLetterSink);
    }
}
