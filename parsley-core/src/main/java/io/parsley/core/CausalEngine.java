package io.parsley.core;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalBuffer;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.ParsleyMetrics;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The broker-neutral causal buffering engine.
 *
 * <p>A broker adapter feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. The engine classifies each record (missing or unresolvable
 * dependency clock → violation; dependencies satisfied → forward; otherwise → buffer),
 * advances the causal frontier, and cascades releases from the buffer as the frontier moves.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierListener} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so an adapter that persists the frontier in the listener is guaranteed to have
 * persisted it before the record leaves the engine.
 *
 * <p>If the policy's {@link BufferLimit} contains a
 * {@link BufferLimit.DurationLimit DurationLimit}, the adapter must schedule a periodic call
 * to {@link #evictNow} every {@link #evictionInterval()}; size-limit eviction is handled by
 * the engine itself.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 * @param <T> the concrete {@link VectorClock} type
 */
public final class CausalEngine<K, V, T extends VectorClock<T>> {

    /**
     * Receives the new frontier after every advancement, before the record that caused the
     * advancement is returned for forwarding. Adapters persist the frontier here.
     *
     * @param <T> the concrete {@link VectorClock} type
     */
    @FunctionalInterface
    public interface FrontierListener<T extends VectorClock<T>> {

        /**
         * Called after the frontier advances.
         *
         * @param frontier the new frontier
         */
        void frontierAdvanced(T frontier);
    }

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final VectorClockSerialiser<T> serialiser;
    private final CausalBuffer<K, V, T> buffer;
    private final FrontierListener<T> frontierListener;
    private final ParsleyMetrics metrics;
    private final Map<CausalRecord<K, V, T>, Instant> bufferedAt = new IdentityHashMap<>();

    private T frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    /**
     * Creates an engine with no metrics.
     *
     * @param policy           the buffering policy
     * @param violationHandler the violation callback
     * @param serialiser       decodes records' dependency clocks
     * @param initialFrontier  the frontier to start from (e.g. restored from a state store)
     * @param deadLetterSink   receives evicted records; required iff {@code policy} is
     *                         {@link BufferingPolicy.DeadLetter DeadLetter}, ignored otherwise
     * @param frontierListener notified before each forwarded record, see class javadoc
     */
    public CausalEngine(BufferingPolicy policy,
                        CausalViolationHandler violationHandler,
                        VectorClockSerialiser<T> serialiser,
                        T initialFrontier,
                        Consumer<CausalRecord<K, V, T>> deadLetterSink,
                        FrontierListener<T> frontierListener) {
        this(policy, violationHandler, serialiser, initialFrontier, deadLetterSink,
                frontierListener, ParsleyMetrics.noop());
    }

    /**
     * Creates an engine.
     *
     * @param policy           the buffering policy
     * @param violationHandler the violation callback
     * @param serialiser       decodes records' dependency clocks
     * @param initialFrontier  the frontier to start from (e.g. restored from a state store)
     * @param deadLetterSink   receives evicted records; required iff {@code policy} is
     *                         {@link BufferingPolicy.DeadLetter DeadLetter}, ignored otherwise
     * @param frontierListener notified before each forwarded record, see class javadoc
     * @param metrics          the observability hook
     * @throws IllegalArgumentException      if {@code policy} is {@code DeadLetter} and
     *                                       {@code deadLetterSink} is {@code null}
     * @throws UnsupportedOperationException if the policy's limit contains a
     *                                       {@link BufferLimit.BytesLimit BytesLimit} or
     *                                       {@link BufferLimit.FrontierAdvancementLimit
     *                                       FrontierAdvancementLimit}
     */
    public CausalEngine(BufferingPolicy policy,
                        CausalViolationHandler violationHandler,
                        VectorClockSerialiser<T> serialiser,
                        T initialFrontier,
                        Consumer<CausalRecord<K, V, T>> deadLetterSink,
                        FrontierListener<T> frontierListener,
                        ParsleyMetrics metrics) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.serialiser = serialiser;
        this.frontier = initialFrontier;
        this.frontierListener = frontierListener;
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
    public List<CausalRecord<K, V, T>> onRecord(CausalRecord<K, V, T> record) {
        List<CausalRecord<K, V, T>> out = new ArrayList<>();

        byte[] encoded = record.encodedDependencies();
        if (encoded == null) {
            violate(record, CausalViolationReason.MISSING_HEADER);
            advanceFrontier(record.position());
            out.add(record);
            drainInto(out);
            return out;
        }

        T dependencies;
        try {
            dependencies = serialiser.deserialise(encoded);
        } catch (Exception e) {
            violate(record, CausalViolationReason.UNRESOLVABLE_CLOCK);
            advanceFrontier(record.position());
            out.add(record);
            drainInto(out);
            return out;
        }

        if (dependencies.satisfiedBy(frontier)) {
            advanceFrontier(record.position());
            out.add(record);
            drainInto(out);
        } else {
            buffer.add(record, dependencies);
            bufferedAt.put(record, Instant.now());
            metrics.onMessageBuffered();
            if (buffer.size() >= sizeLimit) {
                out.addAll(evictNow());
            }
        }
        return out;
    }

    /**
     * Evicts the buffer because a limit fired (called by the engine itself for size limits,
     * and periodically by the adapter for duration limits).
     *
     * @return records the adapter must forward downstream out-of-order; non-empty only for
     *         the {@link BufferingPolicy.Ignore Ignore} policy
     */
    public List<CausalRecord<K, V, T>> evictNow() {
        if (buffer.size() == 0) {
            return List.of();
        }
        List<CausalRecord<K, V, T>> toForward = buffer.evict(limitOf(policy), (record, reason) -> {
            metrics.onViolation(reason);
            violationHandler.onViolation(record, reason);
        });
        bufferedAt.clear();
        for (CausalRecord<K, V, T> record : toForward) {
            advanceFrontier(record.position());
        }
        return toForward;
    }

    /**
     * Returns the current causal frontier.
     *
     * @return the frontier
     */
    public T frontier() {
        return frontier;
    }

    /**
     * Returns the interval at which the adapter must call {@link #evictNow}, if the policy's
     * limit contains a {@link BufferLimit.DurationLimit DurationLimit}.
     *
     * @return the eviction interval, or empty if no duration limit is configured
     */
    public Optional<Duration> evictionInterval() {
        return Optional.ofNullable(evictionInterval);
    }

    private void drainInto(List<CausalRecord<K, V, T>> out) {
        List<CausalRecord<K, V, T>> released = buffer.drain(frontier);
        while (!released.isEmpty()) {
            for (CausalRecord<K, V, T> record : released) {
                advanceFrontier(record.position());
                Instant at = bufferedAt.remove(record);
                if (at != null) {
                    metrics.onMessageReleased(Duration.between(at, Instant.now()));
                }
                out.add(record);
            }
            released = buffer.drain(frontier);
        }
    }

    private void advanceFrontier(T position) {
        frontier = frontier.merge(position);
        frontierListener.frontierAdvanced(frontier);
        metrics.onFrontierAdvanced(frontier);
    }

    private void violate(CausalRecord<K, V, T> record, CausalViolationReason reason) {
        metrics.onViolation(reason);
        violationHandler.onViolation(record, reason);
    }

    private CausalBuffer<K, V, T> createBuffer(BufferingPolicy policy,
                                               Consumer<CausalRecord<K, V, T>> deadLetterSink) {
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
            case BufferingPolicy.Ignore ignore -> ignore.limit();
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
            case BufferLimit.BytesLimit ignored ->
                    throw new UnsupportedOperationException(
                            "BytesLimit is not supported by CausalEngine; use DurationLimit or SizeLimit");
            case BufferLimit.FrontierAdvancementLimit ignored ->
                    throw new UnsupportedOperationException(
                            "FrontierAdvancementLimit is not supported by CausalEngine; use DurationLimit or SizeLimit");
        }
    }
}
