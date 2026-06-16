package io.parsley;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

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
 * ({@link ForwardUnsafePolicy ForwardUnsafe}), discards them ({@link DropPolicy
 * Drop}), or routes them to the dead-letter sink ({@link DeadLetterPolicy DeadLetter}) —
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

    private static final Logger log = LoggerFactory.getLogger(ParsleyEngine.class);

    /**
     * Receives the new frontier after every advancement, before the record that caused the
     * advancement is returned for forwarding.
     */
    @FunctionalInterface
    interface FrontierCallback {
        void frontierAdvanced(CausalFrontier frontier);
    }

    private final CausalBufferPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ParsleyRecord<K, V>> deadLetterSink;
    private final CausalBufferStore<K, V> buffer;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;

    private CausalFrontier frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    ParsleyEngine(CausalBufferPolicy policy,
                 CausalViolationHandler violationHandler,
                 CausalFrontier initialFrontier,
                 Consumer<ParsleyRecord<K, V>> deadLetterSink,
                 FrontierCallback frontierListener,
                 CausalBufferStore<K, V> buffer,
                 ParsleyMetrics metrics) {
        if (policy instanceof DeadLetterPolicy && deadLetterSink == null) {
            throw new IllegalArgumentException("DeadLetter policy requires a dead-letter sink");
        }
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.frontier = initialFrontier;
        this.deadLetterSink = deadLetterSink;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.metrics = metrics;
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
            int depth = buffer.size();
            log.debug("Holding {}-{} @{} (buffer depth: {})",
                    record.sourcePartition().topic(), record.sourcePartition().partition(),
                    record.sourceOffset(), depth);
            metrics.recordBuffered(depth);
            if (depth >= sizeLimit) {
                out.addAll(evictNow());
            }
        }
        return out;
    }

    /**
     * Evicts the buffer because a limit fired. Reports a {@link CausalViolation} per evicted
     * record and applies the policy.
     *
     * @return records to forward downstream out-of-order; non-empty only for the
     *         {@link ForwardUnsafePolicy ForwardUnsafe} policy
     */
    List<ParsleyRecord<K, V>> evictNow() {
        List<CausalBufferStore.Entry<K, V>> evicted = buffer.entries();
        if (evicted.isEmpty()) {
            return List.of();
        }
        log.warn("Evicting {} held record(s) (policy: {})", evicted.size(), policy.getClass().getSimpleName());
        for (CausalBufferStore.Entry<K, V> entry : evicted) {
            violate(entry.record(), CausalViolationReason.LIMIT_REACHED, entry.dependencies());
            buffer.remove(entry.sequence());
        }
        metrics.recordEvicted(evicted.size());
        List<ParsleyRecord<K, V>> toForward = new ArrayList<>();
        switch (policy) {
            case ForwardUnsafePolicy forwardUnsafe -> {
                for (CausalBufferStore.Entry<K, V> entry : evicted) {
                    advanceFrontier(entry.record());
                    toForward.add(entry.record());
                }
            }
            case DropPolicy drop -> { /* discard the evicted records */ }
            case DeadLetterPolicy deadLetter -> {
                for (CausalBufferStore.Entry<K, V> entry : evicted) {
                    deadLetterSink.accept(withDlqHeaders(entry.record(), entry.dependencies()));
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
    CausalFrontier frontier() {
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

    private void drainInto(List<ParsleyRecord<K, V>> out) {
        List<CausalBufferStore.Entry<K, V>> releasable = releasableEntries();
        int totalReleased = 0;
        while (!releasable.isEmpty()) {
            for (CausalBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                advanceFrontier(entry.record());
                out.add(entry.record());
            }
            totalReleased += releasable.size();
            releasable = releasableEntries();
        }
        if (totalReleased > 0) {
            log.debug("Released {} record(s) from buffer (depth now {})", totalReleased, buffer.size());
            metrics.recordReleased(totalReleased, buffer.size());
        }
    }

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
        frontier = frontier.advance(record.sourceTopicId(), record.sourcePartitionIndex(), record.sourceOffset());
        frontierListener.frontierAdvanced(frontier);
    }

    private void violate(ParsleyRecord<K, V> record, CausalViolationReason reason, CausalDependencies required) {
        CausalViolation violation = new CausalViolation(
                record.toConsumerRecord(), reason, frontier, required, required.missingAgainst(frontier));
        log.warn("Causal violation [{} on {}-{} @{}] gap: {}",
                reason, record.sourcePartition().topic(), record.sourcePartition().partition(),
                record.sourceOffset(), violation.gap());
        violationHandler.onViolation(violation);
        metrics.recordViolation();
    }

    private ParsleyRecord<K, V> withDlqHeaders(ParsleyRecord<K, V> record, CausalDependencies required) {
        List<CausalPosition> gap = required.missingAgainst(frontier);
        CausalDependencies gapAsDeps = CausalDependencies.empty();
        for (CausalPosition p : gap) {
            gapAsDeps = gapAsDeps.advance(p.topicId(), p.partition(), p.offset());
        }
        List<ParsleyHeader> h = new ArrayList<>(record.headers());
        h.add(new ParsleyHeader(CausalViolation.DLQ_REASON_HEADER,
                CausalViolationReason.LIMIT_REACHED.name().getBytes(UTF_8)));
        h.add(new ParsleyHeader(CausalViolation.DLQ_REQUIRED_CLOCK_HEADER, required.toBytes()));
        h.add(new ParsleyHeader(CausalViolation.DLQ_GAP_HEADER, gapAsDeps.toBytes()));
        return new ParsleyRecord<>(record.key(), record.value(), record.timestamp(), h);
    }

    private static CausalBufferLimit limitOf(CausalBufferPolicy policy) {
        return switch (policy) {
            case ForwardUnsafePolicy forwardUnsafe -> forwardUnsafe.limit();
            case DropPolicy drop -> drop.limit();
            case DeadLetterPolicy deadLetter -> deadLetter.limit();
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
