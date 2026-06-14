package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.Metrics;
import io.parsley.VectorClock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. The engine classifies each record (missing or unresolvable
 * dependency clock → violation; dependencies satisfied → forward; otherwise → buffer),
 * advances the causal frontier, and cascades releases from the buffer as the frontier moves.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierListener} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class CausalEngine<K, V> {

    /**
     * Receives the new frontier after every advancement, before the record that caused the
     * advancement is returned for forwarding.
     */
    @FunctionalInterface
    interface FrontierListener {
        void frontierAdvanced(VectorClock frontier);
    }

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final CausalBuffer<K, V> buffer;
    private final FrontierListener frontierListener;
    private final BufferPersistence<K, V> persistence;
    private final Metrics metrics;
    private final Map<CausalRecord<K, V>, Instant> bufferedAt = new IdentityHashMap<>();

    private VectorClock frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    CausalEngine(BufferingPolicy policy,
                 CausalViolationHandler violationHandler,
                 VectorClock initialFrontier,
                 Consumer<CausalRecord<K, V>> deadLetterSink,
                 FrontierListener frontierListener) {
        this(policy, violationHandler, initialFrontier, deadLetterSink, frontierListener,
                BufferPersistence.noop(), Metrics.noop());
    }

    CausalEngine(BufferingPolicy policy,
                 CausalViolationHandler violationHandler,
                 VectorClock initialFrontier,
                 Consumer<CausalRecord<K, V>> deadLetterSink,
                 FrontierListener frontierListener,
                 Metrics metrics) {
        this(policy, violationHandler, initialFrontier, deadLetterSink, frontierListener,
                BufferPersistence.noop(), metrics);
    }

    CausalEngine(BufferingPolicy policy,
                 CausalViolationHandler violationHandler,
                 VectorClock initialFrontier,
                 Consumer<CausalRecord<K, V>> deadLetterSink,
                 FrontierListener frontierListener,
                 BufferPersistence<K, V> persistence,
                 Metrics metrics) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.frontier = initialFrontier;
        this.frontierListener = frontierListener;
        this.persistence = persistence;
        this.metrics = metrics;
        this.buffer = createBuffer(policy, deadLetterSink);
        configureLimits(limitOf(policy));
    }

    /**
     * Processes one incoming record.
     *
     * @param record the record to process
     * @return the records to forward downstream, in order; possibly empty
     */
    List<CausalRecord<K, V>> onRecord(CausalRecord<K, V> record) {
        List<CausalRecord<K, V>> out = new ArrayList<>();

        byte[] encoded = record.encodedDependencies();
        if (encoded == null) {
            violate(record, CausalViolationReason.MISSING_HEADER);
            advanceFrontier(record);
            out.add(record);
            drainInto(out);
            return out;
        }

        VectorClock dependencies;
        try {
            dependencies = VectorClock.fromBytes(encoded);
        } catch (Exception e) {
            violate(record, CausalViolationReason.UNRESOLVABLE_CLOCK);
            advanceFrontier(record);
            out.add(record);
            drainInto(out);
            return out;
        }

        if (dependencies.satisfiedBy(frontier)) {
            advanceFrontier(record);
            out.add(record);
            drainInto(out);
        } else {
            buffer.add(record, dependencies);
            bufferedAt.put(record, Instant.now());
            persistence.onHeld(record, dependencies);
            metrics.onMessageBuffered();
            if (buffer.size() >= sizeLimit) {
                out.addAll(evictNow());
            }
        }
        return out;
    }

    /**
     * Re-adds a record to the buffer during restoration from persistent storage, without
     * re-persisting it or advancing the frontier. The record is gated normally on subsequent
     * frontier advances.
     *
     * @param record       the record to rehydrate
     * @param dependencies the record's decoded causal dependencies
     */
    void restore(CausalRecord<K, V> record, VectorClock dependencies) {
        buffer.add(record, dependencies);
        bufferedAt.put(record, Instant.now());
    }

    /**
     * Evicts the buffer because a limit fired (called by the engine itself for size limits,
     * and periodically by the processor for duration limits).
     *
     * @return records to forward downstream out-of-order; non-empty only for the
     *         {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} policy
     */
    List<CausalRecord<K, V>> evictNow() {
        if (buffer.size() == 0) {
            return List.of();
        }
        List<CausalRecord<K, V>> toForward = buffer.evict(limitOf(policy), (record, reason) -> {
            metrics.onViolation(reason);
            violationHandler.onViolation(record, reason);
        }, persistence::onUnheld);
        bufferedAt.clear();
        for (CausalRecord<K, V> record : toForward) {
            advanceFrontier(record);
        }
        return toForward;
    }

    /**
     * Returns the current causal frontier.
     *
     * @return the frontier
     */
    VectorClock frontier() {
        return frontier;
    }

    /**
     * Returns the interval at which the processor must call {@link #evictNow}, if the policy's
     * limit contains a {@link BufferLimit.DurationLimit DurationLimit}.
     *
     * @return the eviction interval, or empty if no duration limit is configured
     */
    Optional<Duration> evictionInterval() {
        return Optional.ofNullable(evictionInterval);
    }

    private void drainInto(List<CausalRecord<K, V>> out) {
        List<CausalRecord<K, V>> released = buffer.drain(frontier);
        while (!released.isEmpty()) {
            for (CausalRecord<K, V> record : released) {
                advanceFrontier(record);
                persistence.onUnheld(record);
                Instant at = bufferedAt.remove(record);
                if (at != null) {
                    metrics.onMessageReleased(Duration.between(at, Instant.now()));
                }
                out.add(record);
            }
            released = buffer.drain(frontier);
        }
    }

    private void advanceFrontier(CausalRecord<K, V> record) {
        frontier = frontier.advance(record.sourcePartition(), record.sourceOffset());
        frontierListener.frontierAdvanced(frontier);
        metrics.onFrontierAdvanced(frontier);
    }

    private void violate(CausalRecord<K, V> record, CausalViolationReason reason) {
        metrics.onViolation(reason);
        violationHandler.onViolation(record.toConsumerRecord(), reason);
    }

    private CausalBuffer<K, V> createBuffer(BufferingPolicy policy,
                                            Consumer<CausalRecord<K, V>> deadLetterSink) {
        if (policy instanceof BufferingPolicy.DeadLetter deadLetter) {
            if (deadLetterSink == null) {
                throw new IllegalArgumentException(
                        "DeadLetter policy requires a dead-letter sink");
            }
            return CausalBuffers.create(deadLetter, deadLetterSink);
        }
        return CausalBuffers.create(policy);
    }

    private static BufferLimit limitOf(BufferingPolicy policy) {
        return switch (policy) {
            case BufferingPolicy.ForwardUnsafe forwardUnsafe -> forwardUnsafe.limit();
            case BufferingPolicy.Drop drop -> drop.limit();
            case BufferingPolicy.DeadLetter deadLetter -> deadLetter.limit();
        };
    }

    private void configureLimits(BufferLimit limit) {
        switch (limit) {
            case BufferLimit.DurationLimit durationLimit ->
                    evictionInterval = durationLimit.duration();
            case BufferLimit.SizeLimit sl ->
                    sizeLimit = sl.messages();
            case BufferLimit.FirstLimit firstLimit -> {
                for (BufferLimit inner : firstLimit.limits()) {
                    configureLimits(inner);
                }
            }
        }
    }
}
