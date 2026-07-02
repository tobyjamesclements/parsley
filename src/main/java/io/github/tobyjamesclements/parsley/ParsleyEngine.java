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
 * record is forwarded only after its declared dependencies have been satisfied by the frontier
 * (whether immediately, after a wait, or trivially — no dependencies claimed, or an undecodable
 * header, both treated as a vacuously satisfied set).
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
 * actually observed it. {@link ParsleyFrontier#deliver} marks every forward in a {@link
 * ParsleyForwardedIndex} and walks forward from the current frontier to find the longest run of
 * consecutive forwarded offsets now achievable, advancing the frontier by that run and pruning the
 * absorbed entries. Release, eviction, and poison-drop all call it the same way — none are
 * special-cased.
 *
 * <p><strong>A coordinate's first offset need not be 0.</strong> Kafka delivers a partition's
 * records strictly in increasing offset order, but retention, compaction, or a fresh consumer group
 * can all mean the first offset this engine ever observes for a coordinate is well past 0. {@link
 * ParsleyFrontier#seedIfFirstSeen} folds everything below the first-ever-observed offset into the
 * frontier the moment it's seen, so it is treated as outside the engine's purview rather than an
 * unfillable hole — without that, the contiguous walk above could never advance past {@code -1} for
 * such a coordinate.
 *
 * <p><strong>Frontier persistence ordering:</strong> {@link ParsleyFrontier} self-persists its
 * single {@code "f"} value on every advance — inside {@link ParsleyFrontier#deliver} and
 * {@link ParsleyFrontier#seedIfFirstSeen}, before control returns to the engine and the record is
 * added to the out-bound list — so the frontier is persisted before the record leaves the engine.
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

    private final CausalBufferLimit limit;
    // Which coordinates this engine consumes; used for eviction-gap diagnostics and restore-time
    // pruning. It is NOT a delivery filter — the gate waits for every input channel to confirm every
    // dependency (see completeness()).
    private final ParsleyClock.CoordinatePredicate inScope;
    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final ParsleyMetrics metrics;
    private final CausalAudit audit;
    private final LongSupplier clock;
    private final boolean skipOnDecodeFailure;
    private final boolean failOnEvictionLimit;

    // The single owner of all persisted causal metadata: the contiguous frontier clock, the channel
    // clocks, and the forwarded-offset index. completeness() and channel state live here.
    private final ParsleyFrontier frontier;
    // The topology epoch's startsAt bounds, read live at gate time; NONE (every coordinate unbounded)
    // when epoch bounding is disabled, so the strip step is then a no-op.
    private final ParsleyEpoch.View epochView;
    private int sizeLimit;
    // Set only for a duration-based limit; null for size/first limits (guarded at every read).
    private @Nullable Duration evictionInterval;

    // --- Convenience constructors: build an in-memory ParsleyFrontier from an initial clock and a
    // forwarded index. Production and restart-style callers pass a pre-built ParsleyFrontier to the
    // full constructor below (so channel + frontier state can be shared/persisted). ---

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics) {
        // Mirrors ParsleyConfig's production defaults — fail fast on both an undecodable held record
        // and a buffer-limit eviction.
        this(limit, initialFrontier, buffer, candidateIndex, forwardedIndex, metrics,
                CausalAudit.NOOP, System::currentTimeMillis, false, true);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(limit, initialFrontier, buffer, candidateIndex, forwardedIndex, metrics,
                CausalAudit.NOOP, clock, false, true);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit) {
        this(limit, new ParsleyFrontier(initialFrontier, forwardedIndex, false), (topicId, partition) -> true,
                buffer, candidateIndex, metrics, audit, clock, skipOnDecodeFailure, failOnEvictionLimit);
    }

    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyClock initialFrontier,
                 ParsleyClock.CoordinatePredicate inScope,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit) {
        this(limit, new ParsleyFrontier(initialFrontier, forwardedIndex, false), inScope,
                buffer, candidateIndex, metrics, audit, clock, skipOnDecodeFailure, failOnEvictionLimit);
    }

    /**
     * Full constructor without an epoch view — delegates to the epoch-aware constructor with
     * {@link ParsleyEpoch.View#NONE} (no stripping). Used by every caller that does not bound the
     * causal domain (all tests, the low-level {@code CausalProcessors} path).
     */
    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyFrontier frontier,
                 ParsleyClock.CoordinatePredicate inScope,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit) {
        this(limit, frontier, inScope, buffer, candidateIndex, metrics, audit, clock,
                skipOnDecodeFailure, failOnEvictionLimit, ParsleyEpoch.View.NONE);
    }

    /**
     * Full constructor. Takes a pre-built {@link ParsleyFrontier} — the single owner of the frontier
     * clock, channel clocks, and forwarded index — so callers control its persistence (a store-backed
     * frontier in production, an in-memory one in tests) — and a {@link ParsleyEpoch.View} supplying
     * the topology epoch's {@code startsAt} bounds, consulted by the gate to strip out-of-domain
     * dependencies ({@link ParsleyEpoch.View#NONE} disables stripping).
     */
    ParsleyEngine(CausalBufferLimit limit,
                 ParsleyFrontier frontier,
                 ParsleyClock.CoordinatePredicate inScope,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean skipOnDecodeFailure,
                 boolean failOnEvictionLimit,
                 ParsleyEpoch.View epochView) {
        this.limit = limit;
        this.frontier = frontier;
        this.epochView = epochView;
        this.inScope = inScope;
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.metrics = metrics;
        this.audit = audit;
        this.clock = clock;
        this.skipOnDecodeFailure = skipOnDecodeFailure;
        this.failOnEvictionLimit = failOnEvictionLimit;
        this.sizeLimit = ParsleyLimits.sizeLimitOf(limit).orElse(Integer.MAX_VALUE);
        this.evictionInterval = ParsleyLimits.durationLimitOf(limit).orElse(null);
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path.
        for (ParsleyBufferStore.IndexEntry entry : buffer.indexEntries()) {
            candidateIndex.index(entry.sequence(),
                    withoutSelfReference(entry.dependencies(), entry.topicId(), entry.partition(), entry.offset()),
                    completeness());
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

        // Receipt-time channel-clock update (BEFORE the gate). A record's carried frontier is the
        // upstream branch's *proven* completeness, valid the moment it arrives regardless of whether
        // this node has delivered it. Recording it here lets the completeness min advance as soon as a
        // channel advertises a coordinate, and prevents a mutual deadlock where two sibling records
        // each depending on a shared ancestor S@k each wait for the other's channel to confirm S@k.
        ParsleyClock channelBefore = frontier.channelGet(message.topicId(), message.partition());
        boolean channelAdvanced = !channelBefore.dominates(message.dependencies());
        frontier.channelUpdate(message.topicId(), message.partition(), message.dependencies());

        if (frontier.seedIfFirstSeen(message.topicId(), message.partition(), message.offset())) {
            propagate(out, message.topicId(), message.partition());
        }

        ParsleyClock deps = withoutSelfReference(message.dependencies(),
                message.topicId(), message.partition(), message.offset());

        if (isDeliverable(message)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            audit.recordForwarded(message.topic(), message.partition(), message.offset());
            frontier.deliver(message.topicId(), message.partition(), message.offset());
            frontier.channelUpdate(message.topicId(), message.partition(), message.dependencies());
            out.add(message);
            propagate(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            candidateIndex.index(seq, deps, completeness());
            int depth = buffer.size();
            ParsleyClock comp = completeness();
            ParsleyClock gap = deps.missing(comp);
            log.debug("Holding {}-{} @{} (buffer depth: {}, deps: {}, completeness: {})",
                    message.topicId(), message.partition(), message.offset(), depth, deps, comp);
            audit.recordHeld(message.topic(), message.partition(), message.offset(), depth, CausalDependencies.of(gap));
            metrics.recordBuffered();
            reportBufferState();
            if (depth >= sizeLimit) {
                out.addAll(evictOverflow());
            }
        }

        // A channel-clock advance can satisfy the completeness gate for records already held that are
        // waiting on a coordinate this channel just advertised. That release is not reachable through
        // the candidate index that drives propagate() (which keys on frontier advances), so re-scan
        // the buffer. Bounded to a fan-in (>1 channel) that actually advanced: a single-channel
        // processor's completeness is its own frontier, fully covered by propagate.
        if (channelAdvanced && frontier.channelCount() > 1) {
            out.addAll(drainSatisfied());
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
     * Releases every buffered record that passes the two-part causal gate against the current
     * frontier and channel-clock state. Used on two call paths:
     * <ol>
     *   <li>Via {@link #drainRestoredSatisfied()} — once, from the 1ms post-init punctuator in
     *       {@link ParsleyProcessor}, to drain records that were satisfied between the last
     *       committed frontier and the last committed buffer-removal (the at-least-once window).
     *       On fresh starts (empty buffer) this returns empty.
     *   <li>Via {@link #onWatermark(Uuid, int, ParsleyClock)} — after a watermark advances a
     *       channel clock, which can unlock records held by the cross-channel ancestor check (part
     *       2 of the gate) even when the in-scope frontier has not advanced.
     * </ol>
     *
     * <p>This is an O(buffer-depth) full scan by design — correctness-first choice; the
     * candidate-index fast path in {@link #propagate} handles the common frontier-advance case.
     *
     * <p>Iterates {@link #orderedIndex()} (index metadata in causal arrival order) and gates each
     * entry on its dependency clock alone — never decoding the user value — so a held, undecodable
     * record that is not releasable is skipped without deserialisation. Only a record that passes the
     * gate is fetched (and, if its value is undecodable on that forward path, handled by the same
     * poison policy as {@link #propagate}). The {@link ParsleyBufferStore#get(long)} {@code null}
     * guard skips entries already removed by a propagate cascade earlier in this same pass.
     */
    private List<ParsleyMessage<K, V>> drainSatisfied() {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        for (ParsleyBufferStore.IndexEntry meta : orderedIndex()) {
            // The completeness gate, on metadata only: every declared coordinate (self-cycle stripped)
            // is within the per-coordinate min across all input channels. A record whose dependencies
            // name a coordinate no input channel will ever confirm stays held — the topology contract,
            // not a special case to short-circuit.
            if (!isDeliverable(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                continue;
            }
            ParsleyBufferStore.Entry<K, V> entry;
            try {
                entry = buffer.get(meta.sequence());
            } catch (ParsleyBufferDeserializationException e) {
                if (tryDropPoison(e, meta.sequence())) {
                    frontier.deliver(e.topicId(), e.partition(), e.offset());
                    continue;
                }
                throw e;                                                   // fail fast
            }
            if (entry == null) continue;                                   // removed by a cascade this pass
            ParsleyMessage<K, V> record = entry.record();
            buffer.remove(meta.sequence());
            audit.recordForwarded(record.topic(), record.partition(), record.offset());
            frontier.deliver(record.topicId(), record.partition(), record.offset());
            frontier.channelUpdate(record.topicId(), record.partition(), record.dependencies());
            out.add(record);
            propagate(out, record.topicId(), record.partition());
        }
        return out;
    }

    /**
     * Delegates to {@link #drainSatisfied()}. Called once, via the 1ms post-init punctuator in
     * {@link ParsleyProcessor}, to drain records that were satisfied between the last committed
     * frontier and the last committed buffer-removal.
     */
    List<ParsleyMessage<K, V>> drainRestoredSatisfied() {
        return drainSatisfied();
    }

    /**
     * Handles a received protocol watermark: updates the per-channel clock for the source channel
     * with the carried frontier, then performs a full-buffer drain to release any buffered records
     * that the cross-channel ancestor check (part 2 of the two-part gate) now permits.
     *
     * <p>The drain is O(buffer-depth). Records released here are delivered via the normal
     * {@link ParsleyProcessor#deliver} path. The caller ({@link ParsleyProcessor}) subsequently
     * emits a downstream watermark carrying the updated {@link #completeness()} frontier to
     * propagate progress to the next layer.
     *
     * @param sourceTopicId  the topic UUID of the watermark's source channel
     * @param sourcePartition the partition of the watermark's source channel
     * @param frontierClock  the completeness frontier carried by the watermark
     * @return the records released from the buffer by the drain; possibly empty
     */
    List<ParsleyMessage<K, V>> onWatermark(Uuid sourceTopicId, int sourcePartition, ParsleyClock frontierClock) {
        frontier.channelUpdate(sourceTopicId, sourcePartition, frontierClock);
        return drainSatisfied();
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
            ParsleyClock gap = oldest.dependencies().retaining(inScope).missing(frontier.snapshot());
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
                    frontier.deliver(e.topicId(), e.partition(), e.offset());
                    propagate(toForward, e.topicId(), e.partition());
                    continue;
                }
                throw e;                                   // fail fast
            }
            if (entry == null) continue;
            reportEviction(entry.record(), entry.dependencies());
            buffer.remove(sequence);
            frontier.deliver(entry.record().topicId(), entry.record().partition(), entry.record().offset());
            frontier.channelUpdate(entry.record().topicId(), entry.record().partition(), entry.record().dependencies());
            toForward.add(entry.record());
            propagate(toForward, entry.record().topicId(), entry.record().partition());
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
        return frontier.snapshot();
    }

    /**
     * Returns the causal completeness frontier: for each coordinate, the greatest offset that
     * <em>every</em> input channel has confirmed.
     *
     * <p>It is the per-coordinate {@link ParsleyClock#intersectMin(ParsleyClock) intersection-minimum}
     * across all input channels. Each channel contributes the dependencies its records and watermarks
     * have advertised, plus its own contiguous delivered position (the {@link ParsleyFrontier} offset
     * for its coordinate), so the channel that owns a coordinate supplies that coordinate's delivered
     * value. A coordinate that any input channel has not observed is absent from the result entirely —
     * a dependency on it is therefore not satisfied until that channel advertises it. This is the
     * delivery gate and the outbound stamp.
     *
     * <p>The model is strict by design: a dependency on coordinate {@code K} is satisfied only when
     * all input channels have observed {@code K}, because the protocol cannot prove a causally-earlier
     * message will not later arrive on a channel that has not yet advertised past {@code K}. The
     * topology must therefore make every input branch observe (consume and watermark) every coordinate
     * any branch depends on; otherwise records depending on an unobserved coordinate are held
     * indefinitely. See {@code docs/internals/causal-consistency.md}.
     *
     * <p>When no channel clocks have been recorded, this returns {@link #frontier()} unchanged. The
     * computation lives on {@link ParsleyFrontier}, which owns both the frontier clock and the channel
     * clocks; this method delegates.
     *
     * @return the completeness frontier; never {@code null}
     */
    ParsleyClock completeness() {
        return frontier.completeness();
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
     * Propagates a frontier advancement: releases every buffered record that became causally
     * satisfiable because {@code (topicId, partition)} just advanced, then cascades — each
     * released record advances its own source coordinate, which may satisfy further records.
     * This is Lamport's transitivity rule: if A → B and A has been delivered, B can now be
     * delivered; and if B → C, C follows in the same pass.
     */
    private void propagate(List<ParsleyMessage<K, V>> out, Uuid topicId, int partition) {
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
                    long coordOffset = frontier.snapshot().offsetFor(coordTopicId, coordPartition);
                    for (ParsleyCandidateIndex.Candidate candidate : candidateIndex.findCandidates(coordTopicId, coordPartition, coordOffset)) {
                        if (!seen.add(candidate.recordId())) continue;
                        ParsleyBufferStore.Entry<K, V> entry;
                        try {
                            entry = buffer.get(candidate.recordId());
                        } catch (ParsleyBufferDeserializationException e) {
                            if (tryDropPoison(e, candidate.recordId())) {
                                // Silent: the dropped record itself is never released into `out`.
                                frontier.deliver(e.topicId(), e.partition(), e.offset());
                                nextScan.computeIfAbsent(e.topicId(), k -> new HashSet<>()).add(e.partition());
                                continue;
                            }
                            throw e;                                               // fail fast
                        }
                        if (entry == null) {
                            stale.add(candidate);
                        } else if (isDeliverable(entry.record())) {
                            releasable.add(entry);
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
                frontier.deliver(entry.record().topicId(), entry.record().partition(), entry.record().offset());
                frontier.channelUpdate(entry.record().topicId(), entry.record().partition(), entry.record().dependencies());
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

    /** The buffer's current depth — the number of held records awaiting satisfied dependencies. */
    int bufferSize() {
        return buffer.size();
    }

    /**
     * The causal delivery gate: every coordinate {@code record} depends on (its own self-cycle
     * stripped) is within the {@link #completeness()} frontier — i.e. confirmed by every input
     * channel. This is the single source of truth for "may this record be delivered now", used on
     * every release path ({@link #onRecord}, {@link #drainSatisfied}, {@link #propagate}).
     */
    private boolean isDeliverable(ParsleyMessage<K, V> record) {
        return isDeliverable(record.dependencies(), record.topicId(), record.partition(), record.offset());
    }

    /**
     * Metadata overload of the gate: evaluates deliverability from a record's dependency clock and
     * source coordinate alone, without decoding its user value. Used by {@link #drainSatisfied} so a
     * held, undecodable record that is not releasable is never deserialised.
     *
     * <p>Two dependency preprocessing steps run before the completeness check, both view-only (they
     * never rewrite recorded state): the record's exact self-cycle is removed
     * ({@link #withoutSelfReference}), and any dependency below its coordinate's topology-epoch
     * {@code startsAt} bound is stripped ({@link ParsleyClock#strippedBelow}) — an out-of-domain
     * reference to a prior, closed epoch that no channel in this epoch will ever confirm. With epoch
     * bounding disabled the strip is a no-op (every bound is {@link Long#MAX_VALUE}).
     */
    private boolean isDeliverable(ParsleyClock dependencies, Uuid topicId, int partition, long offset) {
        ParsleyClock effective = withoutSelfReference(dependencies, topicId, partition, offset)
                .strippedBelow(epochView);
        return completeness().dominates(effective);
    }

    /**
     * Returns {@code deps} with the record's <em>exact</em> source coordinate removed if present — a
     * record depending on its own {@code (topicId, partition, offset)} has, by being delivered, met
     * that dependency, so it must not wait on itself (this keeps a self-referential stamp on a fused
     * chain from deadlocking). A backward same-partition dependency ({@code req < offset}) is an
     * intra-topic dependency like any other and flows through the gate unchanged. This exact self-cycle
     * strip is the <em>only</em> dependency preprocessing: there is no in-scope filtering — a
     * dependency on any other coordinate must be confirmed by every input channel (see
     * {@link #completeness()}).
     */
    private ParsleyClock withoutSelfReference(ParsleyClock deps, Uuid topicId, int partition, long offset) {
        if (deps.offsetFor(topicId, partition) == offset) {
            return deps.without(topicId, partition);
        }
        return deps;
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
        ParsleyClock gap = required.retaining(inScope).missing(frontier.snapshot());
        log.warn("Causal violation [EVICTED on {}-{} @{}] gap: {}",
                record.topic(), record.partition(), record.offset(), gap);
        audit.recordViolation(record.topic(), record.partition(), record.offset(), CausalDependencies.of(gap));
        metrics.recordViolation();
    }

}
