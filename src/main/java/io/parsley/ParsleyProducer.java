package io.parsley;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Decorator over a Kafka {@link Producer} that stamps the causal-dependencies header on each send.
 */
final class ParsleyProducer<K, V> implements CausalProducer<K, V> {

    private final Producer<K, V> delegate;

    ParsleyProducer(Map<String, Object> config) {
        this(new KafkaProducer<>(config));
    }

    ParsleyProducer(Producer<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, CausalDependencies clock) {
        return delegate.send(withClock(record, clock));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, CausalDependencies clock, Callback callback) {
        return delegate.send(withClock(record, clock), callback);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private ProducerRecord<K, V> withClock(ProducerRecord<K, V> record, CausalDependencies clock) {
        ProducerRecord<K, V> enriched = new ProducerRecord<>(
                record.topic(), record.partition(), record.timestamp(),
                record.key(), record.value(), record.headers());
        enriched.headers().add(new RecordHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, clock.toBytes()));
        return enriched;
    }
}
