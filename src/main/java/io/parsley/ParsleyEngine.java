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
import java.util.function.LongSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned
 * records downstream, in order. Every record is delivered — there is no drop, no diversion — but
 * each is stamped with a {@link CausalResult}: {@link CausalResult#SATISFIED} if the frontier
 * satisfied its dependencies by delivery time (whether immediately, after a wait, or trivially —
 * no dependencies claimed, or an undecodable header, both treated as an empty, vacuously
 * satisfied set), or {@link CausalResult#EVICTED} if a {@link CausalBufferLimit} fired before
 * that happened.
 *
 * <p>The engine also owns limit-driven eviction: when a {@link CausalBufferLimit} fires it
 * surrenders the oldest buffered records needed to satisfy the limit, forwards them stamped
 * {@link CausalResult#EVICTED}, and logs the causal gap for each.
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

    private final CausalBufferLimit limit;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyPositionIndex positionIndex;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;
    private final LongSupplier clock;

    private CausalFrontier frontier;
    private int sizeLimit = Integer.MAX_VALUE;
    private Duration evictionInterval;

    ParsleyEngine(CausalBufferLimit limit,
                 CausalFrontier initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyPositionIndex positionIndex,
                 ParsleyMetrics metrics) {
        this(limit, initialFrontier, frontierListener, buffer, positionIndex,
                metrics, System::currentTimeMillis);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 CausalFrontier initialFrontier,
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
    List<ParsleyRecord<K, V>> onRecord(ParsleyRecord<K, V> record) {
        List<ParsleyRecord<K, V>> out = new ArrayList<>();

        CausalDependencies dependencies;
        byte[] encoded = record.encodedDependencies();
        if (encoded == null) {
            log.debug("No causal-dependencies header on {}-{} @{} — trivially satisfied",
                    record.sourcePartition().topic(), record.sourcePartition().partition(),
                    record.sourceOffset());
            dependencies = CausalDependencies.empty();
        } else {
            try {
                dependencies = CausalDependencies.fromBytes(encoded);
            } catch (Exception e) {
                log.warn("Unresolvable causal-dependencies header on {}-{} @{} — treating as trivially satisfied",
                        record.sourcePartition().topic(), record.sourcePartition().partition(),
                        record.sourceOffset());
                dependencies = CausalDependencies.empty();
            }
        }

        dependencies = effectiveDependencies(dependencies, record);

        if (dependencies.isSatisfiedBy(frontier)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    record.sourcePartition().topic(), record.sourcePartition().partition(),
                    record.sourceOffset());
            advanceFrontier(record);
            out.add(stamp(record, CausalResult.SATISFIED));
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
     * @return the evicted records, stamped {@link CausalResult#EVICTED}, to forward downstream
     *         out-of-order
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
     * @return the evicted records, stamped {@link CausalResult#EVICTED}, to forward downstream
     *         out-of-order
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
        log.warn("Evicting {} held record(s) (limit: {})", evicted.size(), limit);
        List<ParsleyRecord<K, V>> toForward = new ArrayList<>();
        for (ParsleyBufferStore.Entry<K, V> entry : evicted) {
            reportEviction(entry.record(), entry.dependencies());
            buffer.remove(entry.sequence());
            advanceFrontier(entry.record());
            toForward.add(stamp(entry.record(), CausalResult.EVICTED));
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
                out.add(stamp(entry.record(), CausalResult.SATISFIED));
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

    private void reportEviction(ParsleyRecord<K, V> record, CausalDependencies required) {
        log.warn("Causal violation [EVICTED on {}-{} @{}] gap: {}",
                record.sourcePartition().topic(), record.sourcePartition().partition(),
                record.sourceOffset(), required.findMissing(frontier));
        metrics.recordViolation();
    }

    private ParsleyRecord<K, V> stamp(ParsleyRecord<K, V> record, CausalResult result) {
        List<ParsleyHeader> h = new ArrayList<>(record.headers());
        h.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_RESULT, result.name().getBytes(UTF_8)));
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
