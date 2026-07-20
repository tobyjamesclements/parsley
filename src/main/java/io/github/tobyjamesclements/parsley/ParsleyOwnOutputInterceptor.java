package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * The producer-side half of the {@code ownOutputs} clock (D2): publishes every acknowledged send
 * to a declared sink topic into this instance's {@link ParsleyOwnOutputRegistry}, from which the
 * stream threads fold coordinates into {@link ParsleyChannels#acknowledge} before each stamp.
 *
 * <p><strong>Not public API.</strong> This class is {@code public} only because Kafka instantiates
 * it reflectively — {@link CausalStreams} injects it into every stream producer through the public
 * {@code producer.interceptor.classes} config (T2.1 validated this path end to end: the EOS stream
 * producer instantiates and configures it, {@code onAcknowledgement} carries the exact committed
 * coordinate, and the callback runs on the producer network thread — hence the concurrent registry).
 * Do not configure it by hand; without the registry id {@link CausalStreams} co-injects
 * ({@link ParsleyOwnOutputRegistry#CONFIG_KEY}) every callback is a no-op.
 */
public final class ParsleyOwnOutputInterceptor implements ProducerInterceptor<Object, Object> {

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
        }
        return producerRecord;
    }

    /**
     * Resolves a tracked send: clears its pending count (T2.1 — exactly one callback per send,
     * abort included, failures carrying a non-null exception) and folds a successful ack's
     * committed coordinate into the registry's per-coordinate max. Runs on the producer network
     * thread; everything it touches is the registry's concurrent state. A null {@code metadata}
     * (no coordinate to resolve) is counted as a failure so a crossing wait releases loudly
     * rather than hanging on a send that will never resolve.
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
        tracker.acknowledged(metadata.topic(), metadata.partition(), exception != null);
        if (exception == null && metadata.hasOffset()) {
            registry.fold(metadata.topic(), metadata.partition(), metadata.offset());
        }
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
