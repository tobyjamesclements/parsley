package io.parsley;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * A Kafka producer that attaches a causal vector-clock header to every record it sends.
 *
 * <p>Each {@link #send} call embeds the serialised {@link CausalDependencies} as a
 * {@code parsley-vector-clock} header. Downstream causal consumers and processors use this
 * header to determine whether a record's causal dependencies have been satisfied.
 *
 * <h2>Usage</h2>
 * Prefer stamping the clock of the message that triggered this send — bounded by that hop's fan-in
 * and transitively carrying its own dependencies:
 * <pre>{@code
 * CausalProducer<String, String> producer = CausalProducers.<String, String>builder(producerConfig).build();
 * CausalDependencies context = CausalDependencies.fromRecord(trigger).orElseGet(consumer::frontier);
 * producer.send(new ProducerRecord<>("orders", key, value), context);
 * }</pre>
 * Pass {@code consumer.frontier()} only when the produced record genuinely depends on everything the
 * consumer has read (e.g. an aggregator): the clock size is proportional to the number of relevant
 * topic-partitions and counts against Kafka's record-size limit ({@code message.max.bytes}), so a
 * wide-fan-in frontier can grow large — see {@link CausalDependencies} for the size envelope.
 *
 * <h2>Thread safety</h2>
 * A {@code CausalProducer} is thread-safe and a single instance can be shared across threads, like
 * the {@code KafkaProducer} it wraps. {@link #send} builds a fresh record carrying the clock header
 * and never mutates the {@link org.apache.kafka.clients.producer.ProducerRecord} passed in, so the
 * caller's record may be reused.
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
    Future<RecordMetadata> send(ProducerRecord<K, V> record, CausalDependencies clock);

    /**
     * Sends a record with {@code clock} attached, invoking {@code callback} on completion.
     *
     * @param record   the record to send; must not be {@code null}
     * @param clock    the causal vector clock to embed; must not be {@code null}
     * @param callback the callback invoked on send completion or failure; may be {@code null}
     * @return a {@link Future} for the {@link RecordMetadata} of the sent record
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, CausalDependencies clock, Callback callback);

    /**
     * Closes the underlying Kafka producer and releases all resources.
     */
    @Override
    void close();
}
