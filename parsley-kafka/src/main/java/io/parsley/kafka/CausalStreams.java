package io.parsley.kafka;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

/**
 * Static helper methods for inserting causal ordering into a Kafka Streams topology.
 *
 * <p>These methods are thin wrappers around {@link KStream#process(org.apache.kafka.streams.processor.api.ProcessorSupplier, String...)}
 * that accept a {@link CausalProcessorSupplier} directly. Use them to keep topology-building
 * code concise:
 *
 * <pre>{@code
 * KStream<String, Order> causally = CausalStreams.process(rawStream, processorSupplier);
 * causally.to("ordered-orders");
 * }</pre>
 */
public final class CausalStreams {

    private CausalStreams() {}

    /**
     * Attaches a {@link CausalProcessorSupplier} to {@code stream}, returning a new stream
     * containing only causally ordered records.
     *
     * @param <K>      the stream key type
     * @param <V>      the stream value type
     * @param stream   the source Kafka Streams {@link KStream}; must not be {@code null}
     * @param supplier the causal ordering processor supplier; must not be {@code null}
     * @return a new {@link KStream} of causally ordered records
     */
    public static <K, V> KStream<K, V> process(KStream<K, V> stream, CausalProcessorSupplier<K, V> supplier) {
        return stream.process(supplier);
    }

    /**
     * Attaches a {@link CausalProcessorSupplier} to {@code stream} with an explicit processor
     * {@link Named} label, returning a new stream containing only causally ordered records.
     *
     * <p>The {@code named} label is used in Kafka Streams' topology description and can help
     * with debugging and monitoring.
     *
     * @param <K>      the stream key type
     * @param <V>      the stream value type
     * @param stream   the source Kafka Streams {@link KStream}; must not be {@code null}
     * @param supplier the causal ordering processor supplier; must not be {@code null}
     * @param named    the logical name for this processor node; must not be {@code null}
     * @return a new {@link KStream} of causally ordered records
     */
    public static <K, V> KStream<K, V> process(
            KStream<K, V> stream,
            CausalProcessorSupplier<K, V> supplier,
            Named named) {
        return stream.process(supplier, named);
    }
}
