package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The producer-side half of the {@code ownOutputs} clock: it publishes every acknowledged send to a
 * declared sink topic into this instance's {@link ParsleyOwnOutputRegistry}, from which the stream
 * threads fold the coordinates into each outbound stamp.
 *
 * <p><strong>Not public API.</strong> This class is {@code public} only because Kafka instantiates
 * producer interceptors reflectively; {@link CausalStreams} injects it into every stream producer
 * through the {@code producer.interceptor.classes} config. Do not configure it by hand: without the
 * registry id {@link CausalStreams} co-injects ({@link ParsleyOwnOutputRegistry#CONFIG_KEY}) every
 * callback is a no-op.
 */
public final class ParsleyOwnOutputInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyOwnOutputInterceptor.class);

    private @Nullable ParsleyOwnOutputRegistry registry;
    private ParsleyOwnOutputRegistry.@Nullable PendingTracker tracker;

    @Override
    public void configure(Map<String, ?> configs) {
        Object id = configs.get(ParsleyOwnOutputRegistry.CONFIG_KEY);
        if (id == null) {
            return;
        }
        ParsleyOwnOutputRegistry resolved = ParsleyOwnOutputRegistry.lookup(id.toString());
        if (resolved != null) {
            this.registry = resolved;
            this.tracker = resolved.newTracker();
        }
    }

    /**
     * Records a tracked send as pending and binds the sending thread — the producer's one owning
     * stream thread under EOS v2 — to this producer's tracker, so the crossing wait can resolve
     * "this task's pending sends" from the current thread. The record is returned untouched:
     * mutating it here would alter what the broker appends.
     */
    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> producerRecord) {
        ParsleyOwnOutputRegistry registry = this.registry;
        ParsleyOwnOutputRegistry.PendingTracker tracker = this.tracker;
        if (registry != null && tracker != null && registry.tracks(producerRecord.topic())) {
            registry.bindThread(Thread.currentThread(), tracker);
            tracker.sent(producerRecord.topic(), producerRecord.partition());
            if (log.isDebugEnabled()) {
                log.debug("Tracked own-output send to {}-{} on {}",
                        producerRecord.topic(), producerRecord.partition(), Thread.currentThread().getName());
            }
        }
        return producerRecord;
    }

    /**
     * Resolves a tracked send: folds a successful ack's committed coordinate into the registry's
     * per-coordinate max, then clears its pending count (T2.1 — exactly one callback per send,
     * abort included, failures carrying a non-null exception). Runs on the producer network
     * thread; everything it touches is the registry's concurrent state. A null {@code metadata}
     * (no coordinate to resolve) is counted as a failure so a crossing wait releases loudly
     * rather than hanging on a send that will never resolve.
     *
     * <p><strong>Fold-before-acknowledge is load-bearing</strong>: {@code tracker.acknowledged}
     * wakes any crossing-wait waiter, whose very next step is draining the registry into the
     * {@code ownOutputs} clock and stamping. Folding after the wake-up opens a window where the
     * released stamp misses exactly the coordinate whose ack released it — an under-claiming
     * stamp, the crossing wait's whole reason to exist (found live in
     * {@code CausalFanOutScopedFrontierIT}). The acked map is a {@code ConcurrentHashMap} written
     * before the tracker's monitor notification, so the woken waiter always observes the fold.
     */
    @Override
    public void onAcknowledgement(@Nullable RecordMetadata metadata, @Nullable Exception exception) {
        ParsleyOwnOutputRegistry registry = this.registry;
        ParsleyOwnOutputRegistry.PendingTracker tracker = this.tracker;
        if (registry == null || tracker == null) {
            return;
        }
        if (metadata == null) {
            tracker.acknowledged("", -1, true);
            return;
        }
        if (!registry.tracks(metadata.topic())) {
            return;
        }
        if (exception == null && metadata.hasOffset()) {
            registry.fold(metadata.topic(), metadata.partition(), metadata.offset());
            if (log.isDebugEnabled()) {
                log.debug("Folded own-output ack {}-{} @{}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
        tracker.acknowledged(metadata.topic(), metadata.partition(), exception != null);
    }

    @Override
    public void close() {
        ParsleyOwnOutputRegistry registry = this.registry;
        ParsleyOwnOutputRegistry.PendingTracker tracker = this.tracker;
        if (registry != null && tracker != null) {
            registry.unbind(tracker);
        }
    }
}
