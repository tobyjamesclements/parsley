package io.parsley.producer;

import io.parsley.VectorClock;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * A Kafka producer that attaches a causal vector-clock header to every record it sends.
 *
 * <p>Each {@link #send} call embeds the serialised {@link VectorClock} as a
 * {@code parsley-vector-clock} header. Downstream causal consumers and processors use this
 * header to determine whether a record's causal dependencies have been satisfied. The clock is
 * typically the frontier of an upstream causal consumer.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * CausalProducer<String, String> producer = CausalProducer.create(producerConfig);
 * producer.send(new ProducerRecord<>("orders", key, value), consumer.frontier());
 * }</pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalProducer<K, V> extends AutoCloseable {

    /**
     * Sends a record with {@code clock} attached as a {@code parsley-vector-clock} header.
     *
     * @param record the record to send; must not be {@code null}
     * @param clock  the causal vector clock to embed; must not be {@code null}
     * @return a {@link Future} for the {@link RecordMetadata} of the sent record
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, VectorClock clock);

    /**
     * Sends a record with {@code clock} attached, invoking {@code callback} on completion.
     *
     * @param record   the record to send; must not be {@code null}
     * @param clock    the causal vector clock to embed; must not be {@code null}
     * @param callback the callback invoked on send completion or failure; may be {@code null}
     * @return a {@link Future} for the {@link RecordMetadata} of the sent record
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, VectorClock clock, Callback callback);

    /**
     * Closes the underlying Kafka producer and releases all resources.
     */
    @Override
    void close();

    /**
     * Creates a new {@code CausalProducer} with the given Kafka producer configuration.
     *
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @param config Kafka producer configuration ({@code ProducerConfig} properties); must
     *               include at minimum {@code bootstrap.servers}, {@code key.serializer}, and
     *               {@code value.serializer}
     * @return a new {@code CausalProducer}
     */
    static <K, V> CausalProducer<K, V> create(Map<String, Object> config) {
        return new KafkaCausalProducer<>(config);
    }
}
