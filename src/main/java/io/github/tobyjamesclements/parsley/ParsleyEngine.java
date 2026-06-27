package io.github.tobyjamesclements.parsley;

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
 * <p>The engine also owns limit-driven eviction: when a {@link CausalBufferLimit} fires, the default
 * ({@code parsley.buffer.eviction.failure.policy = fail}) is to fail the task fast instead, leaving
 * the oldest qualifying record(s) buffered rather than forward them out of causal order — trading
 * availability for consistency. {@code continue} restores the original behaviour: it surrenders the
 * oldest buffered records needed to satisfy the limit and forwards them anyway (out of causal
 * order), logging the causal gap and counting a violation metric for each.
 *
 * <p><strong>The frontier is a contiguous watermark, not a running max.</strong> The engine does
 * not head-of-line block: a later-offset record on a partition may forward before an earlier one
 * still held on the same partition. So a coordinate's frontier offset must only ever advance once
 * every offset up to it has actually been forwarded — never past a gap, or a record elsewhere
 * depending on exactly the gapped offset would be released on bookkeeping alone, never having
 * actually observed it. {@link #extendContiguous} marks every forward in a {@link
 * ParsleyForwardedIndex} and walks forward from the current frontier to find the longest run of
 * consecutive forwarded offsets now achievable, advancing the frontier by that run and pruning the
 * absorbed entries. Release, eviction, and poison-drop all call it the same way — none are
 * special-cased.
 *
 * <p><strong>A coordinate's first offset need not be 0.</strong> Kafka delivers a partition's
 * records strictly in increasing offset order, but retention, compaction, or a fresh consumer group
 * can all mean the first offset this engine ever observes for a coordinate is well past 0. {@link
 * #establishBaselineIfFirstSeen} folds everything below the first-ever-observed offset into the
 * frontier the moment it's seen, so it is treated as outside the engine's purview rather than an
 * unfillable hole — without that, the contiguous walk above could never advance past {@code -1} for
 * such a coordinate.
 *
 * <p><strong>Frontier persistence ordering:</strong> the {@link FrontierCallback} fires for
 * every frontier advancement <em>before</em> the corresponding record is returned for
 * forwarding, so persisting the frontier in the listener is guaranteed to happen before the
 * record leaves the engine. It fires exactly once per record in the list {@link #onRecord} (or an
 * eviction method) returns, in the same order — {@code ParsleyProcessor} zips frontier snapshots to
 * returned records by position — so an advance with no corresponding out-bound record (a
 * poison-drop, or the baseline seed above) must go through the silent variants instead and never
 * notify the listener directly.
 *
 * <p><strong>Drain algorithm:</strong> the engine uses a {@link ParsleyCandidateIndex} to avoid a full
 * buffer scan on every frontier advance. When a coordinate advances, only records indexed on
 * that coordinate are checked for causal satisfaction. The cascade repeats for each newly
 * released record's source coordinate. A record is only ever released once this check — against
 * the real, contiguous frontier — passes; extending the frontier and checking for release are
 * always two separate steps, never one.
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
    // Which dependency coordinates this engine actually consumes and must wait on; a dependency on
    // any other coordinate — a partition this task does not own, or a topic outside its registered
    // buffers — is dropped before the satisfaction check and treated as vacuously satisfied.
    private final ParsleyClock.CoordinatePredicate inScope;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final ParsleyForwardedIndex forwardedIndex;
    private final FrontierCallback frontierListener;
    private final ParsleyMetrics metrics;
    private final CausalAudit audit;
    private final LongSupplier clock;
    // When true (parsley.buffer.deserialization.failure.policy = continue), an undecodable held
    // record is dropped on the forward path and processing continues; when false (default = fail),
    // it is rethrown.
    private final boolean skipOnDecodeFailure;
    // When true (parsley.buffer.eviction.failure.policy = fail, the default), a buffer-limit
    // eviction fails the task fast instead, leaving the record buffered; when false (continue),
    // it is evicted and forwarded out of causal order as before.
    private final boolean failOnEvictionLimit;

    private ParsleyClock frontier;
    // Coordinates seen at least once by onRecord this process; guards the one-time baseline seed in
    // establishBaselineIfFirstSeen (restart-safe: re-derived identically if lost, since it only ever
    // re-fires when the persisted frontier doesn't already cover the coordinate).
    private final Set<CoordKey> seenCoordinates = new HashSet<>();
    private int sizeLimit;
    // Set only for a duration-based limit; null for size/first limits (guarded at every read).
    private @Nullable Duration evictionInterval;

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics) {
        // Convenience constructor: mirrors ParsleyConfig's own production defaults — fail fast on
        // both an undecodable held record and a buffer-limit eviction — rather than silently
        // diverging from what a caller gets with no configuration at all.
        this(limit, initialFrontier, frontierListener, buffer, candidateIndex, forwardedIndex,
                metrics, CausalAudit.NOOP, System::currentTimeMillis, false, true);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(limit, initialFrontier, frontierListener, buffer, candidateIndex, forwardedIndex,
                metrics, CausalAudit.NOOP, clock, false, true);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit) {
        // Default scope: treat every coordinate as consumed (no vacuous-satisfaction filtering).
        // Production wiring always supplies a real scope via the constructor below; this overload
        // preserves the historical behaviour for the convenience constructors and direct test use.
        this(limit, initialFrontier, (topicId, partition) -> true, frontierListener, buffer,
                candidateIndex, forwardedIndex, metrics, audit, clock, skipOnDecodeFailure,
                failOnEvictionLimit);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 ParsleyClock.CoordinatePredicate inScope,
                 FrontierCallback frontierListener,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit) {
        this.limit = limit;
        this.inScope = inScope;
        this.frontier = initialFrontier;
        this.frontierListener = frontierListener;
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.forwardedIndex = forwardedIndex;
        this.metrics = metrics;
        this.audit = audit;
        this.clock = clock;
        this.skipOnDecodeFailure = skipOnDecodeFailure;
        this.failOnEvictionLimit = failOnEvictionLimit;
        this.sizeLimit = sizeLimitOf(limit).orElse(Integer.MAX_VALUE);
        this.evictionInterval = durationLimitOf(limit).orElse(null);
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path.
        for (ParsleyBufferStore.IndexEntry entry : buffer.indexEntries()) {
            candidateIndex.index(entry.sequence(),
                    effectiveDependencies(entry.dependencies(), entry.topicId(), entry.partition(), entry.offset()),
                    frontier);
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
        establishBaselineIfFirstSeen(out, message.topicId(), message.partition(), message.offset());

        ParsleyClock dependencies = effectiveDependencies(message.dependencies(), message);

        if (frontier.dominates(dependencies)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            audit.recordForwarded(message.topic(), message.partition(), message.offset());
            extendContiguous(message.topicId(), message.partition(), message.offset());
            out.add(message);
            drainInto(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            candidateIndex.index(seq, dependencies, frontier);
            int depth = buffer.size();
            ParsleyClock gap = dependencies.missing(frontier);
            log.debug("Holding {}-{} @{} (buffer depth: {}, deps: {}, frontier behind at: {})",
                    message.topicId(), message.partition(), message.offset(), depth, dependencies, gap);
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
     * @return the evicted records, to forward downstream out-of-order; empty under
     *         {@code parsley.buffer.eviction.failure.policy = fail} (the candidates are left
     *         buffered and a {@link ParsleyBufferEvictionLimitException} is thrown instead)
     */
    List<ParsleyMessage<K, V>> evictOverflow() {
        int overflow = buffer.size() - sizeLimit + 1;
        if (overflow <= 0) {
            return List.of();
        }
        List<ParsleyBufferStore.IndexEntry> all = orderedIndex();
        List<ParsleyBufferStore.IndexEntry> oldest = all.subList(0, Math.min(overflow, all.size()));
        return evictOrFail(oldest);
    }

    /**
     * Evicts only the buffered records whose age exceeds the configured
     * {@link ParsleyDurationLimit}, leaving younger records held. Called by the duration
     * punctuator; a no-op when no duration limit is configured.
     *
     * <p>Iterates the oldest-first metadata index, so the scan can stop at the first record that
     * hasn't aged out yet — and never decodes a value to decide what to evict.
     *
     * @return the evicted records, to forward downstream out-of-order; empty under
     *         {@code parsley.buffer.eviction.failure.policy = fail} (the candidates are left
     *         buffered and a {@link ParsleyBufferEvictionLimitException} is thrown instead)
     */
    List<ParsleyMessage<K, V>> evictExpired() {
        if (evictionInterval == null) {
            return List.of();
        }
        long cutoff = clock.getAsLong() - evictionInterval.toMillis();
        List<ParsleyBufferStore.IndexEntry> expired = new ArrayList<>();
        for (ParsleyBufferStore.IndexEntry entry : orderedIndex()) {
            if (entry.bufferedAt() > cutoff) break;
            expired.add(entry);
        }
        return evictOrFail(expired);
    }

    /**
     * Releases every buffered record whose effective dependencies (in-scope coordinates, self-ref
     * stripped) are already dominated by the current frontier. Called once, via the 1ms post-init
     * punctuator in {@link ParsleyProcessor}, to drain records that were satisfied between the last
     * committed frontier and the last committed buffer-removal — a window that exists under
     * {@code at_least_once} processing. On fresh starts (empty buffer) this returns empty.
     *
     * <p>Iterates {@link ParsleyBufferStore#entries()} (a snapshot in causal arrival order). The
     * {@link ParsleyBufferStore#get(long)} guard skips entries already removed by a {@link
     * #drainInto} cascade from an earlier step in this same pass.
     */
    List<ParsleyMessage<K, V>> drainRestoredSatisfied() {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        for (ParsleyBufferStore.Entry<K, V> entry : buffer.entries()) {
            if (buffer.get(entry.sequence()) == null) continue;
            ParsleyMessage<K, V> record = entry.record();
            ParsleyClock effective = effectiveDependencies(record.dependencies(), record);
            // Empty effective deps means all raw dependencies were filtered as out-of-scope —
            // vacuously satisfied. This arises when a topic UUID changes across a restart (the
            // old UUID leaves consumedTopicIds, so its coords are dropped from effectiveDeps).
            // Such a record must be released here; no future onRecord will trigger drainInto for
            // the dropped coordinate, so it would otherwise be stuck in the buffer indefinitely.
            if (effective.isEmpty() || frontier.dominates(effective)) {
                buffer.remove(entry.sequence());
                audit.recordForwarded(record.topic(), record.partition(), record.offset());
                extendContiguous(record.topicId(), record.partition(), record.offset());
                out.add(record);
                drainInto(out, record.topicId(), record.partition());
            }
        }
        return out;
    }

    /** The buffer's metadata index, oldest-first (by insertion sequence); never decodes a value. */
    private List<ParsleyBufferStore.IndexEntry> orderedIndex() {
        List<ParsleyBufferStore.IndexEntry> all = new ArrayList<>(buffer.indexEntries());
        all.sort(java.util.Comparator.comparingLong(ParsleyBufferStore.IndexEntry::sequence));
        return all;
    }

    /**
     * The shared branch point for both eviction triggers: under
     * {@code parsley.buffer.eviction.failure.policy = fail} (the default), fails the task fast on
     * the oldest candidate instead of evicting any of them — none are touched, so all remain
     * buffered for a future attempt. Under {@code continue}, delegates to {@link #evictSequences}
     * to evict and forward every candidate as before. Identifying the oldest candidate for the
     * exception never decodes the user-serde key/value — {@code toEvict} is index metadata only.
     */
    private List<ParsleyMessage<K, V>> evictOrFail(List<ParsleyBufferStore.IndexEntry> toEvict) {
        if (toEvict.isEmpty()) {
            return List.of();
        }
        if (failOnEvictionLimit) {
            ParsleyBufferStore.IndexEntry oldest = toEvict.get(0);
            ParsleyClock gap = oldest.dependencies().retaining(inScope).missing(frontier);
            CausalDependencies missing = CausalDependencies.of(gap);
            log.error("Buffer limit reached (limit: {}); failing fast on the oldest held record "
                    + "{}-{}@{} (parsley.buffer.eviction.failure.policy = fail). It remains buffered.",
                    limit, oldest.topic(), oldest.partition(), oldest.offset());
            audit.recordEvictionLimitExceeded(oldest.topic(), oldest.partition(), oldest.offset(), missing);
            metrics.recordEvictionLimitExceeded();
            throw new ParsleyBufferEvictionLimitException(oldest.topic(), oldest.topicId(),
                    oldest.partition(), oldest.offset(), limit, missing);
        }
        return evictSequences(toEvict.stream().map(ParsleyBufferStore.IndexEntry::sequence).toList());
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
                if (tryDropPoison(e, sequence)) {
                    // The dropped record's own coordinate may unblock something else waiting on it —
                    // a poison-drop is an accounted-for loss, same as an eviction. Silent: the dropped
                    // record itself is never added to toForward.
                    extendContiguousSilently(e.topicId(), e.partition(), e.offset());
                    drainInto(toForward, e.topicId(), e.partition());
                    continue;
                }
                throw e;                                   // fail fast
            }
            if (entry == null) continue;
            reportEviction(entry.record(), entry.dependencies());
            buffer.remove(sequence);
            extendContiguous(entry.record().topicId(), entry.record().partition(), entry.record().offset());
            toForward.add(entry.record());
            drainInto(toForward, entry.record().topicId(), entry.record().partition());
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
            // Coordinates to rescan on the next pass: fed both by records released below and by
            // poison-drops discovered during this pass (a drop's own coordinate may also unblock
            // something else, same as a release).
            Map<Uuid, Set<Integer>> nextScan = new HashMap<>();

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
                            if (tryDropPoison(e, candidate.recordId())) {
                                // Silent: the dropped record itself is never released into `out`.
                                extendContiguousSilently(e.topicId(), e.partition(), e.offset());
                                nextScan.computeIfAbsent(e.topicId(), k -> new HashSet<>()).add(e.partition());
                                continue;
                            }
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
            toScan = nextScan;

            for (ParsleyBufferStore.Entry<K, V> entry : releasable) {
                buffer.remove(entry.sequence());
                audit.recordReleased(entry.record().topic(), entry.record().partition(),
                        entry.record().offset(), buffer.size());
                extendContiguous(entry.record().topicId(), entry.record().partition(), entry.record().offset());
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
     * a fused chain from deadlocking). A backward same-partition dependency ({@code req < offset}) is
     * retained and flows through the normal frontier check unchanged — satisfiable by waiting, since
     * the frontier reaches {@code req} strictly before it reaches the record's own offset.
     *
     * <p>A <em>forward</em> same-partition dependency ({@code req > offset}, naming a later offset on
     * the record's own partition) is retained too, but is never satisfiable by waiting: a dependency
     * clock is always a snapshot of some producer's frontier at write time, so a producer can never
     * have legitimately observed an offset on its own output partition that doesn't exist yet — this
     * shape can only arise from a hand-constructed, causally impossible {@code CausalDependencies}.
     * Since the contiguous frontier cannot pass coordinate {@code (topicId, partition)} past {@code
     * req} without first passing through {@code offset} itself (the very record waiting on {@code
     * req}), it can only ever be resolved by eviction (forced, out of causal order, counted as a
     * violation) — never by a natural release.
     */
    private ParsleyClock effectiveDependencies(ParsleyClock deps, ParsleyMessage<K, V> record) {
        return effectiveDependencies(deps, record.topicId(), record.partition(), record.offset());
    }

    private ParsleyClock effectiveDependencies(ParsleyClock deps, Uuid topicId, int partition, long offset) {
        // Drop coordinates this engine does not consume first: a dependency on a partition this task
        // does not own, or a topic outside its registered buffers, is vacuously satisfied — there is
        // nothing here to wait on. The record's own source coordinate is always in scope, so the
        // self-reference strip below still applies.
        ParsleyClock scoped = deps.retaining(inScope);
        if (scoped.offsetFor(topicId, partition) == offset) {
            return scoped.without(topicId, partition);
        }
        return scoped;
    }

    /**
     * Marks {@code offset} as forwarded in the {@link ParsleyForwardedIndex} and, if this closes a
     * gap (or extends an already-closed one), advances the frontier by the longest contiguous run
     * now achievable on {@code (topicId, partition)} — never past a gap. Pure clock update: never
     * touches the buffer or candidate index. Callers remain responsible for driving {@link
     * #drainInto} for the coordinate afterward, so a record is only ever released once its
     * dependencies are checked against the resulting frontier — never on this update alone.
     *
     * <p>Unconditionally notifies the {@link FrontierCallback} — used only where {@code offset}
     * corresponds 1:1 to a record the caller is about to add to its own out-bound list, matching
     * {@code ParsleyProcessor}'s per-record frontier-stamping snapshot, which zips against the
     * returned record list by position. For an offset with no corresponding out-bound record (a
     * poison-drop, or the one-time baseline seed below) use {@link #extendContiguousSilently}
     * instead — any record it cascades into releasing via the caller's own {@code drainInto} still
     * goes through this method, and so still fires normally.
     */
    private void extendContiguous(Uuid topicId, int partition, long offset) {
        frontier = frontier.observe(topicId, partition, mergeForward(topicId, partition, offset));
        frontierListener.frontierAdvanced(frontier);
    }

    /** As {@link #extendContiguous}, but never notifies the listener. See its Javadoc for when. */
    private void extendContiguousSilently(Uuid topicId, int partition, long offset) {
        frontier = frontier.observe(topicId, partition, mergeForward(topicId, partition, offset));
    }

    /**
     * Marks {@code offset} forwarded and returns the longest contiguous run now achievable on {@code
     * (topicId, partition)} — {@code offset} itself if nothing else is pending, further if earlier
     * forwards were waiting on a gap this one just closed, or the unchanged current frontier offset
     * if a gap remains immediately above it.
     */
    private long mergeForward(Uuid topicId, int partition, long offset) {
        forwardedIndex.mark(topicId, partition, offset);
        long watermark = frontier.offsetFor(topicId, partition);
        long extended = watermark;
        for (long candidate : forwardedIndex.forwardedAfter(topicId, partition, watermark)) {
            if (candidate != extended + 1) break;        // gap — stop extending here
            forwardedIndex.unmark(topicId, partition, candidate);
            extended = candidate;
        }
        return extended;
    }

    /**
     * Establishes the contiguous frontier's starting point for {@code (topicId, partition)} the very
     * first time this engine observes it. Kafka delivers a partition's records strictly in
     * increasing offset order, so the first offset this engine ever sees for a coordinate is, by
     * construction, wherever consumption actually began — which need not be 0 (finite retention, a
     * fresh consumer group, etc.). Anything below it is outside this engine's purview, not a real
     * gap: folding it into the frontier (rather than treating it as an unfillable hole) may
     * immediately satisfy other records depending on it, so this drives {@link #drainInto} too.
     *
     * <p>A no-op on every subsequent record for the same coordinate ({@code seenCoordinates} guards
     * it), and a no-op after a restart if the persisted frontier already reflects the coordinate —
     * in which case the engine's own prior progress is authoritative, not a fresh first-offset guess.
     * Sets the frontier directly rather than through {@link #mergeForward} — {@code offset - 1}
     * names no real marked record, just a floor — and never notifies the listener directly for the
     * same reason {@link #extendContiguousSilently} doesn't: this is not itself an out-bound record,
     * but anything it cascades into releasing via {@code drainInto} still fires the listener normally.
     */
    private void establishBaselineIfFirstSeen(List<ParsleyMessage<K, V>> out, Uuid topicId, int partition, long offset) {
        // Mark the coordinate seen unconditionally, on the very first record regardless of its
        // offset — otherwise a held (not yet forwarded) first record at offset 0 would leave the
        // coordinate looking unseen, and a later record on the same partition could wrongly trigger
        // the seed below, treating the still-held offset 0 as outside the engine's purview.
        if (!seenCoordinates.add(new CoordKey(topicId, partition))) return;
        if (offset <= 0) return; // nothing below 0 to fold in
        if (frontier.offsetFor(topicId, partition) >= 0) return;
        frontier = frontier.observe(topicId, partition, offset - 1);
        drainInto(out, topicId, partition);
    }

    private record CoordKey(Uuid topicId, int partition) {}

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
        ParsleyClock gap = required.retaining(inScope).missing(frontier);
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
            case ParsleyUnboundedLimit ul -> Optional.empty();
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
            case ParsleyUnboundedLimit ul -> Optional.empty();
            case ParsleyDurationLimit dl -> Optional.of(dl.duration());
            case ParsleySizeLimit sl -> Optional.empty();
            case ParsleyFirstLimit fl -> fl.limits().stream()
                    .map(ParsleyEngine::durationLimitOf)
                    .flatMap(Optional::stream)
                    .findFirst();
        };
    }
}
