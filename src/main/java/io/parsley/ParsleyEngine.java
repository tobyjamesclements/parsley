package io.parsley;

import org.apache.kafka.common.Uuid;
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
        void frontierAdvanced(ParsleyClock frontier);
    }

    private final CausalBufferLimit limit;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyPositionIndex positionIndex;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;
    private final LongSupplier clock;

    private ParsleyClock frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyPositionIndex positionIndex,
                 ParsleyMetrics metrics) {
        this(limit, initialFrontier, frontierListener, buffer, positionIndex,
                metrics, System::currentTimeMillis);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyPositionIndex positionIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this.limit = limit;
        this.frontier = initialFrontier;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.positionIndex = positionIndex;
        this.metrics = metrics;
        this.clock = clock;
        configureLimits(limit);
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
    List<ParsleyMessage<K, V>> onRecord(ParsleyMessage<K, V> message) {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();

        ParsleyClock dependencies = effectiveDependencies(message.dependencies(), message);

        if (frontier.dominates(dependencies)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            advanceFrontier(message);
            out.add(message);
            drainInto(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            positionIndex.index(seq, dependencies, frontier);
            int depth = buffer.size();
            if (log.isDebugEnabled()) {
                log.debug("Holding {}-{} @{} (buffer depth: {}, gap: {})",
                        message.topic(), message.partition(), message.offset(), depth,
                        dependencies.missing(frontier));
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
     * @return the evicted records, to forward downstream out-of-order
     */
    List<ParsleyMessage<K, V>> evictOverflow() {
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
     * @return the evicted records, to forward downstream out-of-order
     */
    List<ParsleyMessage<K, V>> evictExpired() {
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

    private List<ParsleyMessage<K, V>> evictEntries(List<ParsleyBufferStore.Entry<K, V>> evicted) {
        if (evicted.isEmpty()) {
            return List.of();
        }
        log.warn("Evicting {} held record(s) (limit: {})", evicted.size(), limit);
        List<ParsleyMessage<K, V>> toForward = new ArrayList<>();
        for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
            reportEviction(entry.record(), entry.dependencies());
            buffer.remove(entry.sequence());
            advanceFrontier(entry.record());
            toForward.add(entry.record());
        }
        metrics.recordEvicted(evicted.size());
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
            List<ParsleyPositionIndex.Candidate> stale = new ArrayList<>();

            for (Map.Entry<Uuid, Set<Integer>> coord : toScan.entrySet()) {
                Uuid coordTopicId = coord.getKey();
                for (int coordPartition : coord.getValue()) {
                    long coordOffset = frontier.offsetFor(coordTopicId, coordPartition);
                    for (ParsleyPositionIndex.Candidate candidate : positionIndex.findCandidates(coordTopicId, coordPartition, coordOffset)) {
                        if (!seen.add(candidate.recordId())) continue;
                        ParsleyBufferStore.Entry<K, V> entry = buffer.get(candidate.recordId());
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

            stale.forEach(positionIndex::prune);
            toScan = new HashMap<>();

            if (releasable.isEmpty()) break;

            for (ParsleyBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                advanceFrontier(entry.record());
                toScan.computeIfAbsent(entry.record().topicId(), k -> new HashSet<>())
                        .add(entry.record().partition());
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

    private void reportEviction(ParsleyMessage<K, V> record, ParsleyClock required) {
        log.warn("Causal violation [EVICTED on {}-{} @{}] gap: {}",
                record.topic(), record.partition(),
                record.offset(), required.missing(frontier));
        metrics.recordViolation();
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
