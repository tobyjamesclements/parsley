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

public final class IgnoreBuffer<K, V> implements CausalBuffer<K, V> {

    static final String CLOCK_HEADER = "parsley-vector-clock";

    private final VectorClockSerialiser serialiser;
    private final LinkedHashMap<Long, BufferedRecord<K, V>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    record BufferedRecord<K, V>(ConsumerRecord<K, V> record, VectorClock clock, Instant bufferedAt) {}

    public IgnoreBuffer(VectorClockSerialiser serialiser) {
        this.serialiser = serialiser;
    }

    @Override
    public void add(ConsumerRecord<K, V> record) {
        buffer.put(sequence++, new BufferedRecord<>(record, extractClock(record), Instant.now()));
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
        List<ConsumerRecord<K, V>> evicted = new ArrayList<>(buffer.size());
        buffer.values().forEach(br -> {
            handler.onViolation(br.record(), CausalViolationReason.LIMIT_REACHED);
            evicted.add(br.record());
        });
        buffer.clear();
        return evicted;
    }

    private VectorClock extractClock(ConsumerRecord<K, V> record) {
        Header header = record.headers().lastHeader(CLOCK_HEADER);
        if (header == null) return KafkaVectorClock.empty();
        return serialiser.deserialise(header.value());
    }
}
