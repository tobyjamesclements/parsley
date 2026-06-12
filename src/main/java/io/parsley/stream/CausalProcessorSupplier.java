package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.function.Consumer;

/**
 * A Kafka Streams {@link ProcessorSupplier} that enforces causal ordering in a topology.
 *
 * <p>Insert it into a {@link org.apache.kafka.streams.kstream.KStream} to hold records whose
 * {@link io.parsley.VectorClock} dependencies are not yet satisfied; records are released as the
 * frontier advances, and records that exceed the configured {@link BufferLimit} are handled
 * according to the {@link BufferingPolicy}. The required {@code parsley-frontier} state store is
 * registered automatically via {@link #stores()}.
 *
 * <h2>Topology integration</h2>
 * <pre>{@code
 * CausalProcessorSupplier<String, Order> supplier = CausalProcessorSupplier.create(
 *         BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         (record, reason) -> log.warn("Violation on {}: {}", record.topic(), reason));
 *
 * KStream<String, Order> ordered = stream.process(supplier);
 * }</pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalProcessorSupplier<K, V> extends ProcessorSupplier<K, V, K, V> {

    /**
     * Creates a supplier for an {@link BufferingPolicy.Ignore Ignore} or
     * {@link BufferingPolicy.Drop Drop} policy.
     *
     * @param <K>              the record key type
     * @param <V>              the record value type
     * @param policy           the buffering policy; must not be a {@code DeadLetter} policy —
     *                         use {@link #create(BufferingPolicy.DeadLetter, CausalViolationHandler, Consumer)}
     * @param violationHandler the callback invoked when a causal violation occurs
     * @return a new {@code CausalProcessorSupplier}
     * @throws IllegalArgumentException if {@code policy} is a
     *                                  {@link BufferingPolicy.DeadLetter DeadLetter} policy
     */
    static <K, V> CausalProcessorSupplier<K, V> create(
            BufferingPolicy policy, CausalViolationHandler violationHandler) {
        if (policy instanceof BufferingPolicy.DeadLetter) {
            throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use the 3-argument create()");
        }
        return new KafkaCausalProcessorSupplier<>(policy, violationHandler, null);
    }

    /**
     * Creates a supplier for a {@link BufferingPolicy.DeadLetter DeadLetter} policy, routing
     * evicted records to {@code deadLetterSink}.
     *
     * @param <K>              the record key type
     * @param <V>              the record value type
     * @param policy           the dead-letter buffering policy
     * @param violationHandler the callback invoked when a causal violation occurs
     * @param deadLetterSink   the consumer that receives evicted records for dead-lettering
     * @return a new {@code CausalProcessorSupplier}
     */
    static <K, V> CausalProcessorSupplier<K, V> create(
            BufferingPolicy.DeadLetter policy,
            CausalViolationHandler violationHandler,
            Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        return new KafkaCausalProcessorSupplier<>(policy, violationHandler, deadLetterSink);
    }
}
