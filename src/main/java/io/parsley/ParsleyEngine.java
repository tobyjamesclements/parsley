package io.parsley;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. The engine classifies each record (missing or unresolvable
 * missing or unresolvable dependencies → violation; dependencies satisfied → forward; otherwise → buffer),
 * advances the causal frontier, and cascades releases from the buffer as the frontier moves.
 *
 * <p>The engine also owns policy-driven eviction: when a {@link CausalBufferLimit} fires it surrenders the
 * buffer and, per {@link CausalBufferPolicy}, forwards the evicted records out-of-order
 * ({@link ParsleyForwardUnsafePolicy ForwardUnsafe}), discards them ({@link ParsleyDropPolicy
 * Drop}), or routes them to the dead-letter sink ({@link ParsleyDeadLetterPolicy DeadLetter}) —
 * reporting a {@link CausalViolation} (with the causal gap) for each.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierCallback} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine.
 *
 * <p><strong>Drain algorithm:</strong> the engine uses a {@link ParsleyWaitIndex} to avoid a full
 * buffer scan on every frontier advance. When a coordinate advances, only records indexed on
 * that coordinate are checked for causal satisfaction. The cascade repeats for each newly
 * released record's source coordinate.
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

    private record CoordinateKey(Uuid topicId, int partition) {}

    private final CausalBufferPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ParsleyRecord<K, V>> deadLetterSink;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyWaitIndex waitIndex;
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
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyWaitIndex waitIndex,
                 ParsleyMetrics metrics) {
        if (policy instanceof ParsleyDeadLetterPolicy && deadLetterSink == null) {
            throw new IllegalArgumentException("DeadLetter policy requires a dead-letter sink");
        }
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.frontier = initialFrontier;
        this.deadLetterSink = deadLetterSink;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.waitIndex = waitIndex;
        this.metrics = metrics;
        configureLimits(limitOf(policy));
        // Populate the wait index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction.
        for (ParsleyBufferStore.Entry<K, V> entry : buffer.entries()) {
            waitIndex.index(entry.sequence(), entry.dependencies(), frontier);
        }
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
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
            return out;
        }

        CausalDependencies dependencies;
        try {
            dependencies = CausalDependencies.fromBytes(encoded);
        } catch (Exception e) {
            violate(record, CausalViolationReason.UNRESOLVABLE_DEPENDENCIES, CausalDependencies.empty());
            advanceFrontier(record);
            out.add(record);
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
            return out;
        }

        dependencies = dependencies.withoutSelfReference(
                record.sourceTopicId(), record.sourcePartitionIndex(), record.sourceOffset());

        if (dependencies.isSatisfiedBy(frontier)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    record.sourcePartition().topic(), record.sourcePartition().partition(),
                    record.sourceOffset());
            advanceFrontier(record);
            out.add(record);
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
        } else {
            long seq = buffer.add(record);
            waitIndex.index(seq, dependencies, frontier);
            int depth = buffer.size();
            if (log.isDebugEnabled()) {
                log.debug("Holding {}-{} @{} (buffer depth: {}, gap: {})",
                        record.sourcePartition().topic(), record.sourcePartition().partition(),
                        record.sourceOffset(), depth, dependencies.findMissing(frontier));
            }
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
     *         {@link ParsleyForwardUnsafePolicy ForwardUnsafe} policy
     */
    List<ParsleyRecord<K, V>> evictNow() {
        List<ParsleyBufferStore.Entry<K, V>> evicted = buffer.entries();
        if (evicted.isEmpty()) {
            return List.of();
        }
        log.warn("Evicting {} held record(s) (policy: {})", evicted.size(), policy.getClass().getSimpleName());
        for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
            violate(entry.record(), CausalViolationReason.LIMIT_REACHED, entry.dependencies());
            buffer.remove(entry.sequence());
        }
        metrics.recordEvicted(evicted.size());
        List<ParsleyRecord<K, V>> toForward = new ArrayList<>();
        switch (policy) {
            case ParsleyForwardUnsafePolicy forwardUnsafe -> {
                for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
                    advanceFrontier(entry.record());
                    toForward.add(entry.record());
                }
            }
            case ParsleyDropPolicy drop -> { /* discard the evicted records */ }
            case ParsleyDeadLetterPolicy deadLetter -> {
                for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
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
     * limit contains a {@link ParsleyDurationLimit ParsleyDurationLimit}.
     *
     * @return the eviction interval, or empty if no duration limit is configured
     */
    Optional<Duration> evictionInterval() {
        return Optional.ofNullable(evictionInterval);
    }

    /**
     * Releases buffered records that have become causally satisfiable because the coordinate
     * {@code (topicId, partition)} just advanced. Cascades: each released record advances its
     * own source coordinate, which may unlock further records.
     */
    private void drainInto(List<ParsleyRecord<K, V>> out, Uuid topicId, int partition) {
        Set<CoordinateKey> toScan = new HashSet<>();
        toScan.add(new CoordinateKey(topicId, partition));
        int totalReleased = 0;

        while (!toScan.isEmpty()) {
            Set<Long> seen = new HashSet<>();
            List<ParsleyBufferStore.Entry<K, V>> releasable = new ArrayList<>();
            List<ParsleyWaitIndex.Candidate> stale = new ArrayList<>();

            for (CoordinateKey coord : toScan) {
                long coordOffset = frontier.offsetFor(coord.topicId(), coord.partition());
                for (ParsleyWaitIndex.Candidate candidate : waitIndex.findCandidates(coord.topicId(), coord.partition(), coordOffset)) {
                    if (!seen.add(candidate.recordId())) continue;
                    ParsleyBufferStore.Entry<K, V> entry = buffer.get(candidate.recordId());
                    if (entry == null) {
                        stale.add(candidate);
                    } else {
                        CausalDependencies deps = entry.dependencies().withoutSelfReference(
                                entry.record().sourceTopicId(),
                                entry.record().sourcePartitionIndex(),
                                entry.record().sourceOffset());
                        if (deps.isSatisfiedBy(frontier)) {
                            releasable.add(entry);
                        }
                    }
                }
            }

            stale.forEach(waitIndex::prune);
            toScan.clear();

            if (releasable.isEmpty()) break;

            for (ParsleyBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                advanceFrontier(entry.record());
                toScan.add(new CoordinateKey(entry.record().sourceTopicId(), entry.record().sourcePartitionIndex()));
                out.add(entry.record());
            }
            totalReleased += releasable.size();
        }

        if (totalReleased > 0) {
            log.debug("Released {} record(s) from buffer (depth now {})", totalReleased, buffer.size());
            metrics.recordReleased(totalReleased, buffer.size());
        }
    }

    private void advanceFrontier(ParsleyRecord<K, V> record) {
        frontier = frontier.advance(record.sourceTopicId(), record.sourcePartitionIndex(), record.sourceOffset());
        frontierListener.frontierAdvanced(frontier);
    }

    private void violate(ParsleyRecord<K, V> record, CausalViolationReason reason, CausalDependencies required) {
        CausalViolation violation = new CausalViolation(
                record.toConsumerRecord(), reason, frontier, required, required.findMissing(frontier));
        log.warn("Causal violation [{} on {}-{} @{}] gap: {}",
                reason, record.sourcePartition().topic(), record.sourcePartition().partition(),
                record.sourceOffset(), violation.gap());
        violationHandler.onViolation(violation);
        metrics.recordViolation();
    }

    private ParsleyRecord<K, V> withDlqHeaders(ParsleyRecord<K, V> record, CausalDependencies required) {
        List<CausalPosition> gap = required.findMissing(frontier);
        CausalDependencies gapAsDeps = CausalDependencies.empty();
        for (CausalPosition p : gap) {
            gapAsDeps = gapAsDeps.advance(p.topicId(), p.partition(), p.offset());
        }
        List<ParsleyHeader> h = new ArrayList<>(record.headers());
        h.add(new ParsleyHeader(CausalViolation.DLQ_REASON_HEADER,
                CausalViolationReason.LIMIT_REACHED.name().getBytes(UTF_8)));
        h.add(new ParsleyHeader(CausalViolation.DLQ_REQUIRED_DEPENDENCIES_HEADER, required.toBytes()));
        h.add(new ParsleyHeader(CausalViolation.DLQ_GAP_HEADER, gapAsDeps.toBytes()));
        return new ParsleyRecord<>(record.key(), record.value(), record.timestamp(), h);
    }

    private static CausalBufferLimit limitOf(CausalBufferPolicy policy) {
        return switch (policy) {
            case ParsleyForwardUnsafePolicy forwardUnsafe -> forwardUnsafe.limit();
            case ParsleyDropPolicy drop -> drop.limit();
            case ParsleyDeadLetterPolicy deadLetter -> deadLetter.limit();
        };
    }

    private void configureLimits(CausalBufferLimit limit) {
        switch (limit) {
            case ParsleyDurationLimit durationLimit ->
                    evictionInterval = durationLimit.duration();
            case ParsleySizeLimit sl ->
                    sizeLimit = sl.messages();
            case ParsleyFirstLimit firstLimit -> {
                for (CausalBufferLimit inner : firstLimit.limits()) {
                    configureLimits(inner);
                }
            }
        }
    }
}
