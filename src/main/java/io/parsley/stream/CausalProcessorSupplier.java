package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.internal.Attributes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.function.Consumer;

/**
 * A Kafka Streams {@link ProcessorSupplier} that enforces causal ordering in a topology.
 *
 * <p>Insert it into a {@link org.apache.kafka.streams.kstream.KStream} to hold records whose
 * {@link io.parsley.VectorClock} dependencies are not yet satisfied; records are released as the
 * frontier advances, and records that exceed the configured {@link BufferLimit} are handled
 * according to the {@link BufferingPolicy}. The required frontier state store is registered
 * automatically via {@link #stores()}.
 *
 * <h2>Topology integration</h2>
 * <pre>{@code
 * CausalProcessorSupplier<String, Order> supplier = CausalProcessorSupplier.create(
 *         BufferingPolicy.forwardUnsafe(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         (record, reason) -> log.warn("Violation on {}: {}", record.topic(), reason));
 *
 * KStream<String, Order> ordered = stream.process(supplier);
 * }</pre>
 *
 * <h2>Multiple causal processors in one topology</h2>
 * Each processor persists its frontier to a named state store (defaulting to
 * {@code "parsley-frontier"}). Kafka Streams requires store names to be unique within a topology,
 * so if you place more than one causal processor in the same topology, give each a <strong>distinct
 * {@code frontierStoreName}</strong> via the naming {@code create(...)} overloads. The name is
 * persistent and changelog-backed, so keep it <strong>stable across restarts</strong>.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalProcessorSupplier<K, V> extends ProcessorSupplier<K, V, K, V> {

    /**
     * Creates a supplier for a {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link BufferingPolicy.Drop Drop} policy, using the default frontier store name.
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
        return create(policy, violationHandler, Attributes.FRONTIER_STORE);
    }

    /**
     * Creates a supplier for a {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link BufferingPolicy.Drop Drop} policy with an explicit frontier store name.
     *
     * <p>Use a distinct, stable name per causal processor when several share one topology.
     *
     * @param <K>               the record key type
     * @param <V>               the record value type
     * @param policy            the buffering policy; must not be a {@code DeadLetter} policy
     * @param violationHandler  the callback invoked when a causal violation occurs
     * @param frontierStoreName the persistent state store name for this processor's frontier
     * @return a new {@code CausalProcessorSupplier}
     * @throws IllegalArgumentException if {@code policy} is a
     *                                  {@link BufferingPolicy.DeadLetter DeadLetter} policy
     */
    static <K, V> CausalProcessorSupplier<K, V> create(
            BufferingPolicy policy, CausalViolationHandler violationHandler, String frontierStoreName) {
        if (policy instanceof BufferingPolicy.DeadLetter) {
            throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use a create() overload with a sink");
        }
        return new KafkaCausalProcessorSupplier<>(policy, violationHandler, null, frontierStoreName);
    }

    /**
     * Creates a supplier for a {@link BufferingPolicy.DeadLetter DeadLetter} policy, using the
     * default frontier store name and routing evicted records to {@code deadLetterSink}.
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
        return create(policy, violationHandler, deadLetterSink, Attributes.FRONTIER_STORE);
    }

    /**
     * Creates a supplier for a {@link BufferingPolicy.DeadLetter DeadLetter} policy with an explicit
     * frontier store name, routing evicted records to {@code deadLetterSink}.
     *
     * <p>Use a distinct, stable name per causal processor when several share one topology.
     *
     * @param <K>               the record key type
     * @param <V>               the record value type
     * @param policy            the dead-letter buffering policy
     * @param violationHandler  the callback invoked when a causal violation occurs
     * @param deadLetterSink    the consumer that receives evicted records for dead-lettering
     * @param frontierStoreName the persistent state store name for this processor's frontier
     * @return a new {@code CausalProcessorSupplier}
     */
    static <K, V> CausalProcessorSupplier<K, V> create(
            BufferingPolicy.DeadLetter policy,
            CausalViolationHandler violationHandler,
            Consumer<ConsumerRecord<K, V>> deadLetterSink,
            String frontierStoreName) {
        return new KafkaCausalProcessorSupplier<>(policy, violationHandler, deadLetterSink, frontierStoreName);
    }
}
