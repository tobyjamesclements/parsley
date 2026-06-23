package io.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. Every record is delivered — there is no drop, no diversion. A
 * record is delivered in causal order once the frontier satisfies its dependencies (whether
 * immediately, after a wait, or trivially — no dependencies claimed, or an undecodable header, both
 * treated as an empty, vacuously satisfied set).
 *
 * <p>The engine also owns limit-driven eviction: when a {@link CausalBufferLimit} fires it
 * surrenders the oldest buffered records needed to satisfy the limit and forwards them anyway (out
 * of causal order), logging the causal gap and counting a violation metric for each.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierCallback} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine.
 *
 * <p><strong>Drain algorithm:</strong> the engine uses a {@link ParsleyCandidateIndex} to avoid a full
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
        void frontierAdvanced(ParsleyClock frontier);
    }

    private final CausalBufferLimit limit;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;
    private final CausalAudit audit;
    private final LongSupplier clock;
    // When true (parsley.buffer.deserialization.failure.policy = continue), an undecodable held
    // record is dropped on the forward path and processing continues; when false (default = fail),
    // it is rethrown.
    private final boolean skipOnDecodeFailure;

    private ParsleyClock frontier;
    private int sizeLimit;
    // Set only for a duration-based limit; null for size/first limits (guarded at every read).
    private @Nullable Duration evictionInterval;

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics) {
        this(limit, initialFrontier, frontierListener, buffer, candidateIndex,
                metrics, CausalAudit.NOOP, System::currentTimeMillis, false);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(limit, initialFrontier, frontierListener, buffer, candidateIndex,
                metrics, CausalAudit.NOOP, clock, false);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure) {
        this.limit = limit;
        this.frontier = initialFrontier;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.metrics = metrics;
        this.audit = audit;
        this.clock = clock;
        this.skipOnDecodeFailure = skipOnDecodeFailure;
        this.sizeLimit = sizeLimitOf(limit).orElse(Integer.MAX_VALUE);
        this.evictionInterval = durationLimitOf(limit).orElse(null);
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path.
        for (ParsleyBufferStore.IndexEntry entry : buffer.indexEntries()) {
            candidateIndex.index(entry.sequence(), entry.dependencies(), frontier);
        }
    }

    /**
     * Processes one incoming record.
     *
     * @param message the record to process
     * @return the records to forward downstream, in order; possibly empty
     */
    List<ParsleyMessage<K, V>> onRecord(ParsleyMessage<K, V> message) {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();

        ParsleyClock dependencies = effectiveDependencies(message.dependencies(), message);

        if (frontier.dominates(dependencies)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            audit.recordForwarded(message.topic(), message.partition(), message.offset());
            advanceFrontier(message);
            out.add(message);
            drainInto(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            candidateIndex.index(seq, dependencies, frontier);
            int depth = buffer.size();
            ParsleyClock gap = dependencies.missing(frontier);
            log.debug("Holding {}-{} @{} (buffer depth: {}, gap: {})",
                    message.topic(), message.partition(), message.offset(), depth, gap);
            audit.recordHeld(message.topic(), message.partition(), message.offset(), depth, CausalDependencies.of(gap));
            metrics.recordBuffered();
            reportBufferState();
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
     * @return the evicted records, to forward downstream out-of-order
     */
    List<ParsleyMessage<K, V>> evictOverflow() {
        int overflow = buffer.size() - sizeLimit + 1;
        if (overflow <= 0) {
            return List.of();
        }
        List<ParsleyBufferStore.IndexEntry> all = orderedIndex();
        List<Long> oldest = new ArrayList<>();
        for (int i = 0; i < Math.min(overflow, all.size()); i++) {
            oldest.add(all.get(i).sequence());
        }
        return evictSequences(oldest);
    }

    /**
     * Evicts only the buffered records whose age exceeds the configured
     * {@link ParsleyDurationLimit}, leaving younger records held. Called by the duration
     * punctuator; a no-op when no duration limit is configured.
     *
     * <p>Iterates the oldest-first metadata index, so the scan can stop at the first record that
     * hasn't aged out yet — and never decodes a value to decide what to evict.
     *
     * @return the evicted records, to forward downstream out-of-order
     */
    List<ParsleyMessage<K, V>> evictExpired() {
        if (evictionInterval == null) {
            return List.of();
        }
        long cutoff = clock.getAsLong() - evictionInterval.toMillis();
        List<Long> expired = new ArrayList<>();
        for (ParsleyBufferStore.IndexEntry entry : orderedIndex()) {
            if (entry.bufferedAt() > cutoff) break;
            expired.add(entry.sequence());
        }
        return evictSequences(expired);
    }

    /** The buffer's metadata index, oldest-first (by insertion sequence); never decodes a value. */
    private List<ParsleyBufferStore.IndexEntry> orderedIndex() {
        List<ParsleyBufferStore.IndexEntry> all = new ArrayList<>(buffer.indexEntries());
        all.sort(java.util.Comparator.comparingLong(ParsleyBufferStore.IndexEntry::sequence));
        return all;
    }

    /**
     * Force-forwards the given buffered records (by sequence) out of causal order, logging the gap
     * and counting a violation for each. A record that cannot be deserialised is — in continue-mode —
     * dropped (logged, counted) instead of forwarded; in fail-fast mode the decode failure propagates
     * with the entry left in the buffer.
     */
    private List<ParsleyMessage<K, V>> evictSequences(List<Long> sequences) {
        if (sequences.isEmpty()) {
            return List.of();
        }
        log.warn("Evicting {} held record(s) (limit: {})", sequences.size(), limit);
        List<ParsleyMessage<K, V>> toForward = new ArrayList<>();
        int evicted = 0;
        for (long sequence : sequences) {
            ParsleyBufferStore.Entry<K, V> entry;
            try {
                entry = buffer.get(sequence);
            } catch (ParsleyBufferDeserializationException e) {
                if (tryDropPoison(e, sequence)) continue;  // skipped (logged + counted)
                throw e;                                   // fail fast
            }
            if (entry == null) continue;
            reportEviction(entry.record(), entry.dependencies());
            buffer.remove(sequence);
            advanceFrontier(entry.record());
            toForward.add(entry.record());
            evicted++;
        }
        if (evicted > 0) {
            metrics.recordEvicted(evicted);
            reportBufferState();
        }
        return toForward;
    }

    /**
     * Returns the current causal frontier.
     *
     * @return the frontier
     */
    ParsleyClock frontier() {
        return frontier;
    }

    /**
     * Returns the interval at which the processor must call {@link #evictExpired}, if the
     * configured {@link CausalBufferLimit} contains a {@link ParsleyDurationLimit ParsleyDurationLimit}.
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
    private void drainInto(List<ParsleyMessage<K, V>> out, Uuid topicId, int partition) {
        Map<Uuid, Set<Integer>> toScan = new HashMap<>();
        toScan.computeIfAbsent(topicId, k -> new HashSet<>()).add(partition);
        int totalReleased = 0;

        while (!toScan.isEmpty()) {
            Set<Long> seen = new HashSet<>();
            List<ParsleyBufferStore.Entry<K, V>> releasable = new ArrayList<>();
            List<ParsleyCandidateIndex.Candidate> stale = new ArrayList<>();

            for (Map.Entry<Uuid, Set<Integer>> coord : toScan.entrySet()) {
                Uuid coordTopicId = coord.getKey();
                for (int coordPartition : coord.getValue()) {
                    long coordOffset = frontier.offsetFor(coordTopicId, coordPartition);
                    for (ParsleyCandidateIndex.Candidate candidate : candidateIndex.findCandidates(coordTopicId, coordPartition, coordOffset)) {
                        if (!seen.add(candidate.recordId())) continue;
                        ParsleyBufferStore.Entry<K, V> entry;
                        try {
                            entry = buffer.get(candidate.recordId());
                        } catch (ParsleyBufferDeserializationException e) {
                            if (tryDropPoison(e, candidate.recordId())) continue;  // skipped
                            throw e;                                               // fail fast
                        }
                        if (entry == null) {
                            stale.add(candidate);
                        } else {
                            ParsleyClock deps = effectiveDependencies(entry.dependencies(), entry.record());
                            if (frontier.dominates(deps)) {
                                releasable.add(entry);
                            }
                        }
                    }
                }
            }

            stale.forEach(candidateIndex::prune);
            toScan = new HashMap<>();

            if (releasable.isEmpty()) break;

            for (ParsleyBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                audit.recordReleased(entry.record().topic(), entry.record().partition(),
                        entry.record().offset(), buffer.size());
                advanceFrontier(entry.record());
                toScan.computeIfAbsent(entry.record().topicId(), k -> new HashSet<>())
                        .add(entry.record().partition());
                out.add(entry.record());
            }
            totalReleased += releasable.size();
        }

        if (totalReleased > 0) {
            log.debug("Released {} record(s) from buffer (depth now {})", totalReleased, buffer.size());
            metrics.recordReleased(totalReleased);
            reportBufferState();
        }
    }

    /**
     * Reports the buffer's current depth and oldest-held-record timestamp to the wired {@link
     * ParsleyMetrics}. Called after every depth-changing event above, and by the owning processor's
     * periodic metrics-refresh punctuator, so the oldest-record gauge stays current even while the
     * buffer is idle between ticks.
     */
    void reportBufferState() {
        metrics.reportState(buffer.size(), buffer.oldestBufferedAt());
    }

    /**
     * Returns {@code deps} with the record's <em>exact</em> source coordinate removed if present — a
     * record depending on its own {@code (topicId, partition, offset)} has, by being delivered, met
     * that dependency, so it must not wait on itself (this is what keeps the self-referential stamp on
     * a fused chain from deadlocking). Any other same-partition entry is retained: a backward dep
     * ({@code req < offset}) and a forward dep ({@code req > offset}, a later record on the partition)
     * are both satisfiable by waiting, so they flow through the normal frontier check unchanged.
     */
    private ParsleyClock effectiveDependencies(ParsleyClock deps, ParsleyMessage<K, V> record) {
        if (deps.offsetFor(record.topicId(), record.partition()) == record.offset()) {
            return deps.without(record.topicId(), record.partition());
        }
        return deps;
    }

    private void advanceFrontier(ParsleyMessage<K, V> record) {
        frontier = frontier.observe(record.topicId(), record.partition(), record.offset());
        frontierListener.frontierAdvanced(frontier);
    }

    /**
     * Handles a held record that could not be deserialised on the forward path: always logs the
     * (payload-free) diagnostic and counts the error. In continue-mode (deserialization handler =
     * {@code LogAndContinue}) it drops the record — removes it, counts a violation — and returns
     * {@code true} so the caller skips it; in fail-fast mode it returns {@code false} so the caller
     * rethrows, leaving the entry in the buffer changelog for recovery.
     */
    private boolean tryDropPoison(ParsleyBufferDeserializationException e, long sequence) {
        metrics.recordDeserializationError();
        audit.recordDeserializationFailure(e.topic(), e.partition(), e.offset(), e.details(), skipOnDecodeFailure);
        if (skipOnDecodeFailure) {
            log.error("Dropping an undecodable buffered record "
                    + "(parsley.buffer.deserialization.failure.policy = continue). {}", e.details(), e);
            buffer.remove(sequence);
            metrics.recordViolation();
            return true;
        }
        log.error("Buffered record could not be deserialised "
                + "(parsley.buffer.deserialization.failure.policy = fail); failing fast. "
                + "It remains in the buffer changelog for recovery. {}", e.details(), e);
        return false;
    }

    private void reportEviction(ParsleyMessage<K, V> record, ParsleyClock required) {
        ParsleyClock gap = required.missing(frontier);
        log.warn("Causal violation [EVICTED on {}-{} @{}] gap: {}",
                record.topic(), record.partition(), record.offset(), gap);
        audit.recordViolation(record.topic(), record.partition(), record.offset(), CausalDependencies.of(gap));
        metrics.recordViolation();
    }

    /**
     * Resolves the configured {@link ParsleySizeLimit}, if any, from {@code limit} — including one
     * nested inside a {@link ParsleyFirstLimit}. Shared with {@code ParsleyProcessor}, which needs
     * the same resolution before a {@link ParsleyEngine} exists, to register the
     * {@code buffer-size-limit} metric.
     *
     * @param limit the configured buffer limit
     * @return the size limit's message count, or empty if no {@link ParsleySizeLimit} is configured
     */
    static Optional<Integer> sizeLimitOf(CausalBufferLimit limit) {
        return switch (limit) {
            case ParsleySizeLimit sl -> Optional.of(sl.messages());
            case ParsleyDurationLimit dl -> Optional.empty();
            case ParsleyFirstLimit fl -> fl.limits().stream()
                    .map(ParsleyEngine::sizeLimitOf)
                    .flatMap(Optional::stream)
                    .findFirst();
        };
    }

    /**
     * Resolves the configured {@link ParsleyDurationLimit}, if any, from {@code limit} — including
     * one nested inside a {@link ParsleyFirstLimit}. Shared with {@code ParsleyProcessor}, which
     * needs the same resolution before a {@link ParsleyEngine} exists, to register the
     * {@code buffer-duration-limit-ms} metric.
     *
     * @param limit the configured buffer limit
     * @return the duration limit, or empty if no {@link ParsleyDurationLimit} is configured
     */
    static Optional<Duration> durationLimitOf(CausalBufferLimit limit) {
        return switch (limit) {
            case ParsleyDurationLimit dl -> Optional.of(dl.duration());
            case ParsleySizeLimit sl -> Optional.empty();
            case ParsleyFirstLimit fl -> fl.limits().stream()
                    .map(ParsleyEngine::durationLimitOf)
                    .flatMap(Optional::stream)
                    .findFirst();
        };
    }
}
