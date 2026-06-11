package io.parsley.kafka;

import io.parsley.FenceToken;
import io.parsley.kafka.internal.DefaultCausalProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * A Kafka producer that attaches causal vector-clock headers to every record it sends.
 *
 * <p>Each {@link #send} call embeds the serialised {@link io.parsley.VectorClock} from the
 * supplied {@link FenceToken} as a {@code parsley-vector-clock} header. Downstream
 * {@link CausalConsumer}s and {@link CausalProcessorSupplier}s use this header to determine
 * whether a record's causal dependencies have been satisfied.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * CausalProducer<String, String> producer = CausalProducer.create(producerConfig);
 *
 * FenceToken token = consumer.fenceToken();   // causal snapshot from an upstream consumer
 * producer.send(new ProducerRecord<>("orders", key, value), token);
 * }</pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalProducer<K, V> {

    /**
     * Sends a record with the causal vector clock from {@code token} attached as a header.
     *
     * @param record the record to send; must not be {@code null}
     * @param token  the fence token whose {@link io.parsley.VectorClock} will be embedded;
     *               must not be {@code null}
     * @return a {@link Future} for the {@link RecordMetadata} of the sent record
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token);

    /**
     * Sends a record with the causal vector clock from {@code token} attached, invoking
     * {@code callback} when the send completes.
     *
     * @param record   the record to send; must not be {@code null}
     * @param token    the fence token whose {@link io.parsley.VectorClock} will be embedded;
     *                 must not be {@code null}
     * @param callback the callback invoked on send completion or failure; may be {@code null}
     * @return a {@link Future} for the {@link RecordMetadata} of the sent record
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token, Callback callback);

    /**
     * Closes the underlying Kafka producer and releases all resources.
     */
    void close();

    /**
     * Creates a new {@code CausalProducer} with the given Kafka producer configuration.
     *
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @param config Kafka producer configuration ({@code ProducerConfig} properties);
     *               must include at minimum {@code bootstrap.servers},
     *               {@code key.serializer}, and {@code value.serializer}
     * @return a new {@code CausalProducer}
     */
    static <K, V> CausalProducer<K, V> create(Map<String, Object> config) {
        return new DefaultCausalProducer<>(config);
    }
}
