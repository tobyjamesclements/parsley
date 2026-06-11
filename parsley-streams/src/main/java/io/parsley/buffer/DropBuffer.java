package io.parsley.buffer;

import io.parsley.BufferLimit;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class DropBuffer<K, V> implements CausalBuffer<K, V> {

    private final VectorClockSerialiser serialiser;
    private final LinkedHashMap<Long, IgnoreBuffer.BufferedRecord<K, V>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    public DropBuffer(VectorClockSerialiser serialiser) {
        this.serialiser = serialiser;
    }

    @Override
    public void add(ConsumerRecord<K, V> record) {
        buffer.put(sequence++, new IgnoreBuffer.BufferedRecord<>(record, extractClock(record), Instant.now()));
    }

    @Override
    public List<ConsumerRecord<K, V>> drain(VectorClock frontier) {
        List<ConsumerRecord<K, V>> released = new ArrayList<>();
        buffer.entrySet().removeIf(entry -> {
            if (frontier.dominates(entry.getValue().clock())) {
                released.add(entry.getValue().record());
                return true;
            }
            return false;
        });
        return released;
    }

    @Override
    public List<ConsumerRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler) {
        buffer.values().forEach(br -> handler.onViolation(br.record(), CausalViolationReason.LIMIT_REACHED));
        buffer.clear();
        return List.of();
    }

    private VectorClock extractClock(ConsumerRecord<K, V> record) {
        Header header = record.headers().lastHeader(IgnoreBuffer.CLOCK_HEADER);
        if (header == null) return io.parsley.internal.ImmutableVectorClock.empty();
        return serialiser.deserialise(header.value());
    }
}
