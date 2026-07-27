package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JVM-wide registry of a Streams application's own produced records, fed by
 * {@link Interceptor} (configured on the application's producers) and read by each task's
 * {@link InterceptorSendTracker}.
 *
 * <p>Keying: under EOSv2 each stream thread owns one producer whose {@code client.id} is the
 * thread name plus {@code -producer}, so a task running on the thread finds its entry by its
 * own thread name. Acknowledgements are attributed per producer, i.e. per thread — a task may
 * therefore observe a co-threaded sibling task's acknowledgement, which only ever over-claims
 * real appended offsets (delay-only, sound).
 */
public final class OwnOutputs {

    static final class Entry {
        final AtomicLong pending = new AtomicLong();
        final Queue<RecordMetadata> acks = new ConcurrentLinkedQueue<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
    }

    private static final ConcurrentHashMap<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private OwnOutputs() {}

    static Entry entry(String producerClientId) {
        return ENTRIES.computeIfAbsent(producerClientId, k -> new Entry());
    }

    /** The producer interceptor; add to {@code producer.interceptor.classes}. */
    public static final class Interceptor implements ProducerInterceptor<byte[], byte[]> {

        private Entry entry;

        @Override
        public void configure(Map<String, ?> configs) {
            entry = OwnOutputs.entry(String.valueOf(configs.get("client.id")));
        }

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            entry.pending.incrementAndGet();
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                entry.failure.compareAndSet(null, exception);
            } else if (metadata != null) {
                entry.acks.add(metadata);
            }
            entry.pending.decrementAndGet();
        }

        @Override
        public void close() {}
    }
}
