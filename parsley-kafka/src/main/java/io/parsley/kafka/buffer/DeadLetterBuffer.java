package io.parsley.kafka.buffer;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.kafka.KafkaVectorClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Consumer;

public final class DeadLetterBuffer<K, V> implements CausalBuffer<K, V> {

    private final VectorClockSerialiser serialiser;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;
    private final LinkedHashMap<Long, IgnoreBuffer.BufferedRecord<K, V>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    public DeadLetterBuffer(VectorClockSerialiser serialiser, Consumer<ConsumerRecord<K, V>> deadLetterSink) {
        this.serialiser = serialiser;
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public void add(ConsumerRecord<K, V> record) {
        buffer.put(sequence++, new IgnoreBuffer.BufferedRecord<>(record, extractClock(record), Instant.now()));
    }

    @Override
    public List<ConsumerRecord<K, V>> drain(VectorClock frontier) {
        List<ConsumerRecord<K, V>> released = new ArrayList<>();
        buffer.entrySet().removeIf(entry -> {
            if (entry.getValue().clock().satisfiedBy(frontier)) {
                released.add(entry.getValue().record());
                return true;
            }
            return false;
        });
        return released;
    }

    @Override
    public List<ConsumerRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler) {
        buffer.values().forEach(br -> {
            deadLetterSink.accept(br.record());
            handler.onViolation(br.record(), CausalViolationReason.LIMIT_REACHED);
        });
        buffer.clear();
        return List.of();
    }

    private VectorClock extractClock(ConsumerRecord<K, V> record) {
        Header header = record.headers().lastHeader(IgnoreBuffer.CLOCK_HEADER);
        if (header == null) return KafkaVectorClock.empty();
        return serialiser.deserialise(header.value());
    }
}
