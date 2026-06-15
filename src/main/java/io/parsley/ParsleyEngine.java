package io.parsley;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * <p>The engine also owns policy-driven eviction: when a {@link CausalBufferLimit} fires it surrenders the
 * buffer and, per {@link CausalBufferPolicy}, forwards the evicted records out-of-order
 * ({@link ForwardUnsafe ForwardUnsafe}), discards them ({@link Drop
 * Drop}), or routes them to the dead-letter sink ({@link DeadLetter DeadLetter}) —
 * reporting a {@link CausalViolation} (with the causal gap) for each.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierCallback} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleyEngine<K, V> {

    /**
     * Receives the new frontier after every advancement, before the record that caused the
     * advancement is returned for forwarding.
     */
    @FunctionalInterface
    interface FrontierCallback {
        void frontierAdvanced(CausalDependencies frontier);
    }

    private final CausalBufferPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ParsleyRecord<K, V>> deadLetterSink;
    private final CausalBufferStore<K, V> buffer;
    private final FrontierCallback frontierListener;

    private CausalDependencies frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    ParsleyEngine(CausalBufferPolicy policy,
                 CausalViolationHandler violationHandler,
                 CausalDependencies initialFrontier,
                 Consumer<ParsleyRecord<K, V>> deadLetterSink,
                 FrontierCallback frontierListener,
                 CausalBufferStore<K, V> buffer) {
        if (policy instanceof DeadLetter && deadLetterSink == null) {
            throw new IllegalArgumentException("DeadLetter policy requires a dead-letter sink");
        }
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.frontier = initialFrontier;
        this.deadLetterSink = deadLetterSink;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        configureLimits(limitOf(policy));
    }

    /**
     * Processes one incoming record.
     *
     * @param record the record to process
     * @return the records to forward downstream, in order; possibly empty
     */
    List<ParsleyRecord<K, V>> onRecord(ParsleyRecord<K, V> record) {
        List<ParsleyRecord<K, V>> out = new ArrayList<>();

        // A record we cannot gate (no clock header, or one we cannot decode) is admitted
        // unconditionally rather than held forever: we report the violation, then advance the
        // frontier and forward it exactly as for a satisfied record — so its offset still counts as
        // observed and any records buffered behind it can drain. The required clock is reported as
        // empty because there is no decodable dependency to measure the gap against.
        byte[] encoded = record.encodedDependencies();
        if (encoded == null) {
            violate(record, CausalViolationReason.MISSING_HEADER, CausalDependencies.empty());
            advanceFrontier(record);
            out.add(record);
            drainInto(out);
            return out;
        }

        CausalDependencies dependencies;
        try {
            dependencies = CausalDependencies.fromBytes(encoded);
        } catch (Exception e) {
            violate(record, CausalViolationReason.UNRESOLVABLE_CLOCK, CausalDependencies.empty());
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
            buffer.add(record);
            if (buffer.size() >= sizeLimit) {
                out.addAll(evictNow());
            }
        }
        return out;
    }

    /**
     * Evicts the buffer because a limit fired (called by the engine itself for size limits,
     * and periodically by the processor for duration limits). Reports a {@link CausalViolation} per
     * evicted record and applies the policy.
     *
     * @return records to forward downstream out-of-order; non-empty only for the
     *         {@link ForwardUnsafe ForwardUnsafe} policy
     */
    List<ParsleyRecord<K, V>> evictNow() {
        List<CausalBufferStore.Entry<K, V>> evicted = buffer.entries();
        if (evicted.isEmpty()) {
            return List.of();
        }
        for (CausalBufferStore.Entry<K, V> entry : evicted) {
            violate(entry.record(), CausalViolationReason.LIMIT_REACHED, entry.dependencies());
            buffer.remove(entry.sequence());
        }
        List<ParsleyRecord<K, V>> toForward = new ArrayList<>();
        switch (policy) {
            case ForwardUnsafe forwardUnsafe -> {
                for (CausalBufferStore.Entry<K, V> entry : evicted) {
                    advanceFrontier(entry.record());
                    toForward.add(entry.record());
                }
            }
            case Drop drop -> { /* discard the evicted records */ }
            case DeadLetter deadLetter -> {
                for (CausalBufferStore.Entry<K, V> entry : evicted) {
                    deadLetterSink.accept(entry.record());
                }
            }
        }
        return toForward;
    }

    /**
     * Returns the current causal frontier.
     *
     * @return the frontier
     */
    CausalDependencies frontier() {
        return frontier;
    }

    /**
     * Returns the interval at which the processor must call {@link #evictNow}, if the policy's
     * limit contains a {@link DurationLimit DurationLimit}.
     *
     * @return the eviction interval, or empty if no duration limit is configured
     */
    Optional<Duration> evictionInterval() {
        return Optional.ofNullable(evictionInterval);
    }

    // Releasing a record advances the frontier, which may in turn satisfy records still buffered
    // behind it — so we drain in passes until a pass releases nothing. (A single pass is not enough:
    // B may depend on A, and A's release is what unblocks B.) Each pass selects against the frontier
    // as of the pass start, then advances through the released records; the next pass sees the moved
    // frontier. Each released record is forwarded just like a directly satisfied one.
    private void drainInto(List<ParsleyRecord<K, V>> out) {
        List<CausalBufferStore.Entry<K, V>> releasable = releasableEntries();
        while (!releasable.isEmpty()) {
            for (CausalBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                advanceFrontier(entry.record());
                out.add(entry.record());
            }
            releasable = releasableEntries();
        }
    }

    // The buffered entries whose dependencies the current frontier already satisfies, in arrival
    // order. The whole set is materialised before any release, so a release within the pass does not
    // change which records the pass forwards.
    private List<CausalBufferStore.Entry<K, V>> releasableEntries() {
        List<CausalBufferStore.Entry<K, V>> releasable = new ArrayList<>();
        for (CausalBufferStore.Entry<K, V> entry : buffer.entries()) {
            if (entry.dependencies().satisfiedBy(frontier)) {
                releasable.add(entry);
            }
        }
        return releasable;
    }

    private void advanceFrontier(ParsleyRecord<K, V> record) {
        frontier = frontier.advance(record.sourcePartition(), record.sourceOffset());
        frontierListener.frontierAdvanced(frontier);
    }

    private void violate(ParsleyRecord<K, V> record, CausalViolationReason reason, CausalDependencies required) {
        violationHandler.onViolation(new CausalViolation(
                record.toConsumerRecord(), reason, frontier, required, required.missingAgainst(frontier)));
    }

    private static CausalBufferLimit limitOf(CausalBufferPolicy policy) {
        return switch (policy) {
            case ForwardUnsafe forwardUnsafe -> forwardUnsafe.limit();
            case Drop drop -> drop.limit();
            case DeadLetter deadLetter -> deadLetter.limit();
        };
    }

    private void configureLimits(CausalBufferLimit limit) {
        switch (limit) {
            case DurationLimit durationLimit ->
                    evictionInterval = durationLimit.duration();
            case SizeLimit sl ->
                    sizeLimit = sl.messages();
            case FirstLimit firstLimit -> {
                for (CausalBufferLimit inner : firstLimit.limits()) {
                    configureLimits(inner);
                }
            }
        }
    }
}
