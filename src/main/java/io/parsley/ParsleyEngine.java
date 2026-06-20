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
import java.util.function.LongSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. The engine classifies each record (missing or unresolvable
 * dependencies → violation + policy applied; dependencies satisfied → forward; otherwise → buffer),
 * advances the causal frontier, and cascades releases from the buffer as the frontier moves.
 *
 * <p>The engine also owns policy-driven eviction: when a {@link CausalBufferLimit} fires it surrenders
 * the oldest buffered records needed to satisfy the limit and, per {@link CausalBufferPolicy}, forwards
 * them out-of-order, discards them, or routes them to the dead-letter sink — reporting a
 * {@link CausalViolation} (with the causal gap) for each. The action is determined per
 * {@link CausalViolationReason} via {@link CausalBufferPolicy#actionFor}.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierCallback} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine.
 *
 * <p><strong>Drain algorithm:</strong> the engine uses a {@link ParsleyPositionIndex} to avoid a full
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

    private record ParsleyPartition(Uuid topicId, int partition) {}

    private final CausalBufferPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ParsleyRecord<K, V>> deadLetterSink;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyPositionIndex positionIndex;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;
    private final LongSupplier clock;

    private CausalFrontier frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    ParsleyEngine(CausalBufferPolicy policy,
                 CausalViolationHandler violationHandler,
                 CausalFrontier initialFrontier,
                 Consumer<ParsleyRecord<K, V>> deadLetterSink,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyPositionIndex positionIndex,
                 ParsleyMetrics metrics) {
        this(policy, violationHandler, initialFrontier, deadLetterSink, frontierListener, buffer, positionIndex,
                metrics, System::currentTimeMillis);
    }

    ParsleyEngine(CausalBufferPolicy policy,
                 CausalViolationHandler violationHandler,
                 CausalFrontier initialFrontier,
                 Consumer<ParsleyRecord<K, V>> deadLetterSink,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyPositionIndex positionIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        if (policy.requiresDeadLetterSink() && deadLetterSink == null) {
            throw new IllegalArgumentException("Policy requires a dead-letter sink");
        }
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.frontier = initialFrontier;
        this.deadLetterSink = deadLetterSink;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.positionIndex = positionIndex;
        this.metrics = metrics;
        this.clock = clock;
        configureLimits(policy.limit());
        // Populate the position index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction.
        for (ParsleyBufferStore.Entry<K, V> entry : buffer.entries()) {
            positionIndex.index(entry.sequence(), entry.dependencies(), frontier);
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
            applyPolicyForRecord(record, CausalDependencies.empty(), CausalViolationReason.MISSING_HEADER, out);
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
            return out;
        }

        CausalDependencies dependencies;
        try {
            dependencies = CausalDependencies.fromBytes(encoded);
        } catch (Exception e) {
            violate(record, CausalViolationReason.UNRESOLVABLE_DEPENDENCIES, CausalDependencies.empty());
            advanceFrontier(record);
            applyPolicyForRecord(record, CausalDependencies.empty(), CausalViolationReason.UNRESOLVABLE_DEPENDENCIES, out);
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
            return out;
        }

        dependencies = effectiveDependencies(dependencies, record);

        if (dependencies.isSatisfiedBy(frontier)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    record.sourcePartition().topic(), record.sourcePartition().partition(),
                    record.sourceOffset());
            advanceFrontier(record);
            out.add(record);
            drainInto(out, record.sourceTopicId(), record.sourcePartitionIndex());
        } else {
            long seq = buffer.add(record, clock.getAsLong());
            positionIndex.index(seq, dependencies, frontier);
            int depth = buffer.size();
            if (log.isDebugEnabled()) {
                log.debug("Holding {}-{} @{} (buffer depth: {}, gap: {})",
                        record.sourcePartition().topic(), record.sourcePartition().partition(),
                        record.sourceOffset(), depth, dependencies.findMissing(frontier));
            }
            metrics.recordBuffered(depth);
            if (depth >= sizeLimit) {
                out.addAll(evictOverflow());
            }
        }
        return out;
    }

    /**
     * Evicts only the oldest buffered records needed to bring the buffer back under the
     * configured {@link ParsleySizeLimit}, leaving the rest held. Called inline from
     * {@link #onRecord} once buffer depth reaches the limit, and once more by
     * {@code ParsleyProcessor.init()} immediately after construction, to bring a buffer restored
     * from a changelog back under the currently configured limit (relevant after a
     * reconfiguration that lowers {@code ofSize(...)} before a restart).
     *
     * <p>Relies on {@link ParsleyBufferStore#entries()} being sorted oldest-first (see
     * {@link #evictExpired()}), so only the leading {@code buffer.size() - sizeLimit + 1}
     * entries need to be evicted to fit the record just admitted.
     *
     * @return records to forward downstream out-of-order; non-empty only when the
     *         {@link CausalViolationReason#LIMIT_REACHED} action is {@link CausalViolationAction#FORWARD_UNSAFE}
     */
    List<ParsleyRecord<K, V>> evictOverflow() {
        int overflow = buffer.size() - sizeLimit + 1;
        if (overflow <= 0) {
            return List.of();
        }
        List<ParsleyBufferStore.Entry<K, V>> all = buffer.entries();
        return evictEntries(new ArrayList<>(all.subList(0, Math.min(overflow, all.size()))));
    }

    /**
     * Evicts only the buffered records whose age exceeds the configured
     * {@link ParsleyDurationLimit}, leaving younger records held. Called by the duration
     * punctuator; a no-op when no duration limit is configured.
     *
     * <p>Relies on {@link ParsleyBufferStore#entries()} being sorted oldest-first (true for both
     * implementations, since insertion sequence tracks buffer-admission time on the single owning
     * thread), so the scan can stop at the first record that hasn't aged out yet.
     *
     * @return records to forward downstream out-of-order; non-empty only when the
     *         {@link CausalViolationReason#LIMIT_REACHED} action is {@link CausalViolationAction#FORWARD_UNSAFE}
     */
    List<ParsleyRecord<K, V>> evictExpired() {
        if (evictionInterval == null) {
            return List.of();
        }
        long cutoff = clock.getAsLong() - evictionInterval.toMillis();
        List<ParsleyBufferStore.Entry<K, V>> expired = new ArrayList<>();
        for (ParsleyBufferStore.Entry<K, V> entry : buffer.entries()) {
            if (entry.bufferedAt() > cutoff) break;
            expired.add(entry);
        }
        return evictEntries(expired);
    }

    private List<ParsleyRecord<K, V>> evictEntries(List<ParsleyBufferStore.Entry<K, V>> evicted) {
        if (evicted.isEmpty()) {
            return List.of();
        }
        log.warn("Evicting {} held record(s) (policy: {})", evicted.size(), policy);
        List<ParsleyRecord<K, V>> toForward = new ArrayList<>();
        for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
            violate(entry.record(), CausalViolationReason.LIMIT_REACHED, entry.dependencies());
            buffer.remove(entry.sequence());
            if (policy.actionFor(CausalViolationReason.LIMIT_REACHED) == CausalViolationAction.FORWARD_UNSAFE) {
                advanceFrontier(entry.record());
            }
            applyPolicyForRecord(entry.record(), entry.dependencies(), CausalViolationReason.LIMIT_REACHED, toForward);
        }
        metrics.recordEvicted(evicted.size());
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
     * Returns the interval at which the processor must call {@link #evictExpired}, if the policy's
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
        Set<ParsleyPartition> toScan = new HashSet<>();
        toScan.add(new ParsleyPartition(topicId, partition));
        int totalReleased = 0;

        while (!toScan.isEmpty()) {
            Set<Long> seen = new HashSet<>();
            List<ParsleyBufferStore.Entry<K, V>> releasable = new ArrayList<>();
            List<ParsleyPositionIndex.Candidate> stale = new ArrayList<>();

            for (ParsleyPartition coord : toScan) {
                long coordOffset = frontier.offsetFor(coord.topicId(), coord.partition());
                for (ParsleyPositionIndex.Candidate candidate : positionIndex.findCandidates(coord.topicId(), coord.partition(), coordOffset)) {
                    if (!seen.add(candidate.recordId())) continue;
                    ParsleyBufferStore.Entry<K, V> entry = buffer.get(candidate.recordId());
                    if (entry == null) {
                        stale.add(candidate);
                    } else {
                        CausalDependencies deps = effectiveDependencies(entry.dependencies(), entry.record());
                        if (deps.isSatisfiedBy(frontier)) {
                            releasable.add(entry);
                        }
                    }
                }
            }

            stale.forEach(positionIndex::prune);
            toScan.clear();

            if (releasable.isEmpty()) break;

            for (ParsleyBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                advanceFrontier(entry.record());
                toScan.add(new ParsleyPartition(entry.record().sourceTopicId(), entry.record().sourcePartitionIndex()));
                out.add(entry.record());
            }
            totalReleased += releasable.size();
        }

        if (totalReleased > 0) {
            log.debug("Released {} record(s) from buffer (depth now {})", totalReleased, buffer.size());
            metrics.recordReleased(totalReleased, buffer.size());
        }
    }

    /**
     * Returns {@code deps} with the record's <em>exact</em> source coordinate removed if present — a
     * record depending on its own {@code (topicId, partition, offset)} has, by being delivered, met
     * that dependency, so it must not wait on itself (this is what keeps the self-referential stamp on
     * a fused chain from deadlocking). Any other same-partition entry is retained: a backward dep
     * ({@code req < offset}) and a forward dep ({@code req > offset}, a later record on the partition)
     * are both satisfiable by waiting, so they flow through the normal frontier check unchanged.
     */
    private CausalDependencies effectiveDependencies(CausalDependencies deps, ParsleyRecord<K, V> record) {
        CausalPosition self = new CausalPosition(
                record.sourceTopicId(), record.sourcePartitionIndex(), record.sourceOffset());
        List<CausalPosition> all = deps.dependencies();
        if (!all.contains(self)) {
            return deps;
        }
        CausalDependencies.Builder builder = CausalDependencies.builder();
        for (CausalPosition pos : all) {
            if (!pos.equals(self)) {
                builder.require(pos);
            }
        }
        return builder.build();
    }

    private void advanceFrontier(ParsleyRecord<K, V> record) {
        frontier = frontier.observe(new CausalPosition(record.sourceTopicId(), record.sourcePartitionIndex(), record.sourceOffset()));
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

    private void applyPolicyForRecord(ParsleyRecord<K, V> record, CausalDependencies required,
                                      CausalViolationReason reason, List<ParsleyRecord<K, V>> out) {
        switch (policy.actionFor(reason)) {
            case FORWARD_UNSAFE -> out.add(record);
            case DROP           -> {}
            case DEAD_LETTER    -> deadLetterSink.accept(withDlqHeaders(record, required, reason));
        }
    }

    private ParsleyRecord<K, V> withDlqHeaders(ParsleyRecord<K, V> record, CausalDependencies required,
                                                CausalViolationReason reason) {
        List<CausalPosition> gap = required.findMissing(frontier);
        CausalDependencies.Builder gapBuilder = CausalDependencies.builder();
        for (CausalPosition p : gap) {
            gapBuilder.require(p);
        }
        CausalDependencies gapAsDeps = gapBuilder.build();
        List<ParsleyHeader> h = new ArrayList<>(record.headers());
        h.add(new ParsleyHeader(CausalViolation.DLQ_REASON_HEADER, reason.name().getBytes(UTF_8)));
        h.add(new ParsleyHeader(CausalViolation.DLQ_REQUIRED_DEPENDENCIES_HEADER, required.toBytes()));
        h.add(new ParsleyHeader(CausalViolation.DLQ_GAP_HEADER, gapAsDeps.toBytes()));
        return new ParsleyRecord<>(record.key(), record.value(), record.timestamp(), h);
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
