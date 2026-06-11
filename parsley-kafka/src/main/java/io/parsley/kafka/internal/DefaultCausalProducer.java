package io.parsley.kafka.internal;

import io.parsley.FenceToken;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.kafka.CausalProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Future;

public final class DefaultCausalProducer<K, V> implements CausalProducer<K, V> {

    private static final String CLOCK_HEADER = CausalProcessor.CLOCK_HEADER;

    private final KafkaProducer<K, V> delegate;
    private final VectorClockSerialiser serialiser;

    public DefaultCausalProducer(Map<String, Object> config) {
        this.delegate = new KafkaProducer<>(config);
        this.serialiser = ServiceLoader.load(VectorClockSerialiser.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No VectorClockSerialiser on classpath — add parsley-kafka"));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token) {
        return delegate.send(withClock(record, token.vectorClock()));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token, Callback callback) {
        return delegate.send(withClock(record, token.vectorClock()), callback);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private ProducerRecord<K, V> withClock(ProducerRecord<K, V> record, VectorClock clock) {
        byte[] clockBytes = serialiser.serialise(clock);
        ProducerRecord<K, V> enriched = new ProducerRecord<>(
                record.topic(), record.partition(), record.timestamp(),
                record.key(), record.value(), record.headers());
        enriched.headers().add(new RecordHeader(CLOCK_HEADER, clockBytes));
        return enriched;
    }
}
