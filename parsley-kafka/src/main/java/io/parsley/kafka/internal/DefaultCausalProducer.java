package io.parsley.kafka.internal;

import io.parsley.FenceToken;
import io.parsley.ParsleyAttributes;
import io.parsley.VectorClockSerialiser;
import io.parsley.core.ParsleyServices;
import io.parsley.kafka.CausalProducer;
import io.parsley.kafka.KafkaVectorClock;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Map;
import java.util.concurrent.Future;

public final class DefaultCausalProducer<K, V> implements CausalProducer<K, V> {

    private final Producer<K, V> delegate;
    private final VectorClockSerialiser<KafkaVectorClock> serialiser;

    public DefaultCausalProducer(Map<String, Object> config) {
        this(new KafkaProducer<>(config));
    }

    @SuppressWarnings("unchecked")
    DefaultCausalProducer(Producer<K, V> delegate) {
        this.delegate = delegate;
        this.serialiser = (VectorClockSerialiser<KafkaVectorClock>)
                ParsleyServices.global().resolve(VectorClockSerialiser.class);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken<KafkaVectorClock> token) {
        return delegate.send(withClock(record, token.vectorClock()));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken<KafkaVectorClock> token,
                                       Callback callback) {
        return delegate.send(withClock(record, token.vectorClock()), callback);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private ProducerRecord<K, V> withClock(ProducerRecord<K, V> record, KafkaVectorClock clock) {
        byte[] clockBytes = serialiser.serialise(clock);
        ProducerRecord<K, V> enriched = new ProducerRecord<>(
                record.topic(), record.partition(), record.timestamp(),
                record.key(), record.value(), record.headers());
        enriched.headers().add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, clockBytes));
        return enriched;
    }
}
