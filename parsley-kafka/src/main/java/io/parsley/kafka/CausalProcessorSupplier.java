package io.parsley.kafka;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.VectorClockSerialiser;
import io.parsley.kafka.buffer.CausalViolationHandler;
import io.parsley.kafka.internal.CausalProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Set;
import java.util.function.Consumer;

/**
 * A Kafka Streams {@link ProcessorSupplier} that enforces causal ordering in a streaming
 * topology.
 *
 * <p>Insert a {@code CausalProcessorSupplier} into a {@link org.apache.kafka.streams.kstream.KStream}
 * to hold records with unsatisfied vector-clock dependencies in an in-memory buffer. Records
 * are released as the frontier advances; records that exceed the configured
 * {@link BufferLimit} are handled according to the {@link BufferingPolicy}.
 *
 * <h2>Topology integration</h2>
 * <pre>{@code
 * CausalProcessorSupplier<String, Order> supplier = new CausalProcessorSupplier<>(
 *         BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         (record, reason) -> log.warn("Violation: {}", reason),
 *         serialiser);
 *
 * KStream<String, Order> ordered = stream.process(supplier);
 * }</pre>
 *
 * <h2>State store</h2>
 * <p>The processor registers a persistent key-value store named {@code parsley-frontier}
 * to survive restarts. This store is automatically included in {@link #stores()}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public final class CausalProcessorSupplier<K, V> implements ProcessorSupplier<K, V, K, V> {

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final VectorClockSerialiser serialiser;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;

    /**
     * Creates a {@code CausalProcessorSupplier} for {@link BufferingPolicy.Ignore Ignore} or
     * {@link BufferingPolicy.Drop Drop} policies.
     *
     * @param policy           the buffering policy; must not be a {@code DeadLetter} policy —
     *                         use the 4-argument constructor instead
     * @param violationHandler the callback invoked when a causal violation occurs
     * @param serialiser       the serialiser used to decode vector-clock record headers
     * @throws IllegalArgumentException      if {@code policy} is {@link BufferingPolicy.DeadLetter}
     * @throws UnsupportedOperationException if the policy's {@link BufferLimit} variant is not
     *                                       yet supported (e.g. {@link BufferLimit.BytesLimit})
     */
    public CausalProcessorSupplier(
            BufferingPolicy policy,
            CausalViolationHandler violationHandler,
            VectorClockSerialiser serialiser) {
        if (policy instanceof BufferingPolicy.DeadLetter) {
            throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use the 4-argument constructor");
        }
        validateLimit(limitOf(policy));
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.serialiser = serialiser;
        this.deadLetterSink = null;
    }

    /**
     * Creates a {@code CausalProcessorSupplier} for a
     * {@link BufferingPolicy.DeadLetter DeadLetter} policy.
     *
     * <p>Evicted records are passed to {@code deadLetterSink} (e.g. to forward them to a
     * dead-letter Kafka topic via a separate producer).
     *
     * @param policy           the dead-letter buffering policy
     * @param violationHandler the callback invoked when a causal violation occurs
     * @param serialiser       the serialiser used to decode vector-clock record headers
     * @param deadLetterSink   the consumer that receives evicted records for dead-lettering
     * @throws UnsupportedOperationException if the policy's {@link BufferLimit} variant is not
     *                                       yet supported
     */
    public CausalProcessorSupplier(
            BufferingPolicy.DeadLetter policy,
            CausalViolationHandler violationHandler,
            VectorClockSerialiser serialiser,
            Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        validateLimit(limitOf(policy));
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.serialiser = serialiser;
        this.deadLetterSink = deadLetterSink;
    }

    private static BufferLimit limitOf(BufferingPolicy policy) {
        return switch (policy) {
            case BufferingPolicy.Ignore ig -> ig.limit();
            case BufferingPolicy.Drop dp -> dp.limit();
            case BufferingPolicy.DeadLetter de -> de.limit();
        };
    }

    private static void validateLimit(BufferLimit limit) {
        switch (limit) {
            case BufferLimit.DurationLimit ignored -> {}
            case BufferLimit.SizeLimit ignored -> {}
            case BufferLimit.FirstLimit fl -> { for (BufferLimit inner : fl.limits()) validateLimit(inner); }
            case BufferLimit.BytesLimit ignored ->
                throw new UnsupportedOperationException(
                        "BytesLimit is not supported; use DurationLimit or SizeLimit");
            case BufferLimit.FrontierAdvancementLimit ignored ->
                throw new UnsupportedOperationException(
                        "FrontierAdvancementLimit is not supported; use DurationLimit or SizeLimit");
        }
    }

    /**
     * Returns a new {@link CausalProcessor} instance for each Kafka Streams task.
     *
     * @return a new processor; never {@code null}
     */
    @Override
    public Processor<K, V, K, V> get() {
        return new CausalProcessor<>(policy, violationHandler, serialiser, deadLetterSink);
    }

    /**
     * Returns the set of state store builders required by the processor.
     *
     * <p>Includes a persistent key-value store named {@code parsley-frontier} (keyed by
     * {@link String}, valued by {@code byte[]}) that persists the causal frontier across
     * application restarts.
     *
     * @return a singleton set containing the frontier store builder
     */
    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, byte[]>> frontierBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(CausalProcessor.FRONTIER_STORE),
                        Serdes.String(),
                        Serdes.ByteArray());

        return Set.of(frontierBuilder);
    }
}
