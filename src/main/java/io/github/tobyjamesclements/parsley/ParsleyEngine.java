package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The causal buffering engine.
 *
 * <p>The processor feeds incoming records to {@link #onRecord} and forwards the returned records
 * downstream, in order. Delivery is strictly fail-closed: a record is forwarded only after its
 * declared dependencies have been satisfied by the completeness frontier. There is no eviction, no
 * buffer limit, and no timeout — a record whose dependencies are not yet satisfied stays buffered
 * (the buffer is a changelog-backed state store, so it spills to disk rather than growing in memory).
 * The only record that ever leaves the buffer without satisfied dependencies is one whose
 * dependencies are <em>proven impossible</em> — an undecodable payload or dependency header, or a
 * dependent of one — which is dead-lettered (removed from the causal execution path via {@link
 * Outcome#deadLettered()}), never forwarded downstream as if it were causally valid. Dead-lettering is
 * enabled per instance (see {@link #ParsleyEngine(ParsleyFrontier, ParsleyBufferStore,
 * ParsleyCandidateIndex, ParsleyMetrics, CausalAudit, LongSupplier, boolean) the full constructor}); when
 * disabled, a poison/proven-impossible record still fails the task fast, exactly as before dead-lettering
 * existed.
 *
 * <p>A record whose dependencies name a coordinate this node has no input channel for at all — an
 * undeclared topic, or a partition a different task instance owns — is rejected before it ever enters
 * the buffer ({@link DeadLetter.Reason#UNREACHABLE_DEPENDENCY}), the same way as proven-impossible.
 * This is deliberately fail-closed rather than a shortcut: this node can prove it has no way to confirm
 * such a coordinate, never that the coordinate is genuinely irrelevant, so it is never silently treated
 * as satisfied. Checked independently of whether dead-lettering is enabled — with no dead-letter sink
 * configured, it still fails the task fast rather than delivering on an unproven premise.
 *
 * <p><strong>The frontier is a contiguous watermark, not a running max.</strong> The engine does
 * not head-of-line block: a later-offset record on a partition may forward before an earlier one
 * still held on the same partition. So a coordinate's frontier offset must only ever advance once
 * every offset up to it has actually been forwarded — never past a gap, or a record elsewhere
 * depending on exactly the gapped offset would be released on bookkeeping alone, never having
 * actually observed it. {@link ParsleyFrontier#deliver} marks every forward in a {@link
 * ParsleyForwardedIndex} and walks forward from the current frontier to find the longest run of
 * consecutive forwarded offsets now achievable, advancing the frontier by that run and pruning the
 * absorbed entries. Every release calls it the same way — none are special-cased. A dead-lettered
 * offset is never marked forwarded, so the frontier's contiguous walk can never cross it: the
 * coordinate's frontier is permanently frozen at {@code offset - 1} on this node, which is exactly why
 * a dead-lettered coordinate must be recorded ({@link ParsleyOrphanIndex}) and its dependents proactively
 * cascaded, rather than left to buffer forever against a gate that can now never be satisfied.
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
 * For the same reason, every release path calls {@link ParsleyFrontier#deliver} (and {@link
 * ParsleyFrontier#channelUpdate}) <em>before</em> removing the record from {@link #buffer}: the
 * buffer and the frontier/forwarded-index are separate changelog topics with no cross-store
 * atomicity, so a crash between the two writes must always tear toward "buffer still holds a record
 * the frontier already delivered" (harmless — {@link #drainAfterRestore} redelivers it as an
 * at-least-once duplicate) and never the reverse, which would strand that coordinate's frontier
 * permanently.
 *
 * <p><strong>Drain algorithm:</strong> the engine uses a {@link ParsleyCandidateIndex} to avoid a full
 * buffer scan on every frontier advance. When a coordinate advances, only records indexed on
 * that coordinate are checked for causal satisfaction. The cascade repeats for each newly
 * released record's source coordinate. A record is only ever released once this check — against
 * the real, contiguous frontier — passes; extending the frontier and checking for release are
 * always two separate steps, never one.
 *
 * <p><strong>Orphan cascade (local to this node's buffer):</strong> when a record is dead-lettered, its
 * coordinate is recorded in the {@link ParsleyOrphanIndex} the owning {@link ParsleyFrontier} carries —
 * that coordinate's frontier can never legitimately advance again. {@link #orphan} then worklist-scans
 * this node's own buffer (mirroring {@link #propagate}, in the opposite direction — via {@link
 * ParsleyCandidateIndex#findCandidatesRequiringAtLeast}) for any record whose dependencies require that
 * coordinate at or beyond the new floor: such a record can never be satisfied either, so it is
 * dead-lettered too, and its own coordinate orphaned in turn. This does not reach a <em>different</em>
 * node still buffering on the same doomed coordinate — that node sees only a channel that stopped
 * advancing, indistinguishable from ordinary lag, until a DAG-wide mechanism (forced epoch-floor
 * advance) resolves it. Every dead-letter path runs this cascade — the three that fail inside the
 * engine ({@link #onRecord}, {@link #propagate}, {@link #drainSatisfied}) via {@link #deadLetterRoot},
 * and the one that fails before a record ever becomes engine state — an ingest-time undecodable
 * causal-dependencies header — via {@link #deadLetterAtIngest}.
 *
 * <p><strong>Dead-letter persistence ordering:</strong> mirroring the frontier-persistence rule above,
 * every dead-letter path calls {@link ParsleyOrphanIndex#markOrphaned} on a victim's own coordinate
 * <em>before</em> removing it from {@link #buffer} — never the reverse. The orphan index and the buffer
 * are separate changelog topics with no cross-store atomicity, so a crash between the two writes must
 * always tear toward "orphan floor recorded, victim still buffered" (harmless — the next {@link
 * #drainSatisfied}/{@link #drainAfterRestore} pass finds the record still there, already proven
 * impossible by the durably-recorded floor, and dead-letters it again as a harmless duplicate) and never
 * the reverse, which would leave the record gone with no floor recorded — permanently stranding anything
 * depending on that exact coordinate, the same wedge shape {@link #deadLetterAtIngest} exists to close on
 * the ingest side. {@link #fetchForDeadLetter} enforces this for its two callers ({@link
 * #drainSatisfied}'s proven-impossible branch and {@link #orphan}'s own cascade loop); the poison-on-
 * fetch branches of {@link #propagate}/{@link #drainSatisfied} and {@link #propagate}'s proven-impossible
 * branch do it inline, since they already hold the record needed to know its own coordinate.
 *
 * <p><strong>The "always" above is literal, not just likely.</strong> {@link CausalTopology#assemble}
 * requires {@code processing.guarantee=exactly_once_v2} unconditionally, which wraps every state-store
 * changelog write, every produced record, and the consumer offset commit for one commit interval into a
 * single Kafka transaction — so the two writes in each ordering above either both land or neither does;
 * a crash genuinely cannot tear one from the other. The write orderings themselves remain, as
 * defense-in-depth and because the reasoning above is what makes {@link #drainAfterRestore}'s "resolves
 * as a harmless duplicate" claim correct in the first place, but they no longer carry the whole guarantee
 * alone the way they would under at-least-once.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleyEngine<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyEngine.class);

    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final ParsleyMetrics metrics;
    private final CausalAudit audit;
    private final LongSupplier clock;

    // The single owner of all persisted causal metadata: the contiguous frontier clock, the channel
    // clocks, and the forwarded-offset index. completeness() and channel state live here, floored to
    // the topology epoch's lower bounds (frontier.epoch(), also read at the gate to strip a record's
    // out-of-domain dependencies; NONE when epoch bounding is disabled, so the floor is a no-op).
    private final ParsleyFrontier frontier;

    // The dead-letter cascade's durable "this coordinate can never advance again" record, owned by
    // frontier alongside its forwarded index; reached directly here since the cascade is engine logic.
    private final ParsleyOrphanIndex orphanIndex;

    // Whether a proven-impossible record is dead-lettered (removed from the causal path via Outcome,
    // cascading to dependents) or fails the task fast, exactly as before dead-lettering existed. Off by
    // default in every convenience constructor so existing callers/tests are unaffected unless they opt
    // in via the full constructor.
    private final boolean deadLetterEnabled;

    // Coordinates this node could ever genuinely confirm — a registered input channel, on the
    // partition this task owns. A dependency outside this scope (an undeclared topic, or a partition
    // a different task instance owns) can never be observed here no matter how long it waits — but
    // that does not make it safe to disregard: this node can prove it cannot check the coordinate,
    // never that the coordinate is genuinely irrelevant. So it is fail-closed, not vacuously
    // satisfied — see isUnreachableDependency and DeadLetter.Reason#UNREACHABLE_DEPENDENCY. Defaults
    // to "everything in scope" so existing callers/tests that never construct with an explicit
    // predicate are unaffected; ParsleyProcessor passes its real per-task predicate.
    private final ParsleyClock.CoordinatePredicate inScope;

    // --- Convenience constructors: build an in-memory ParsleyFrontier from an initial clock and a
    // forwarded/orphan index. Production and restart-style callers pass a pre-built ParsleyFrontier to
    // the full constructor below (so channel + frontier state can be shared/persisted). Dead-lettering
    // is off in every convenience constructor — pass a ParsleyFrontier explicitly and opt in via the
    // full 7-arg constructor to exercise it. ---

    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyOrphanIndex orphanIndex,
                 ParsleyMetrics metrics) {
        this(initialFrontier, buffer, candidateIndex, forwardedIndex, orphanIndex, metrics,
                CausalAudit.NOOP, System::currentTimeMillis);
    }

    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyOrphanIndex orphanIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(initialFrontier, buffer, candidateIndex, forwardedIndex, orphanIndex, metrics, CausalAudit.NOOP, clock);
    }

    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyOrphanIndex orphanIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock) {
        this(initialFrontier, buffer, candidateIndex, forwardedIndex, orphanIndex, metrics, audit, clock, false);
    }

    /**
     * As above, with dead-lettering explicitly enabled or disabled — for tests exercising the dead-letter
     * cascade without hand-building a {@link ParsleyFrontier}.
     */
    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyOrphanIndex orphanIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean deadLetterEnabled) {
        this(new ParsleyFrontier(initialFrontier, forwardedIndex, orphanIndex, false),
                buffer, candidateIndex, metrics, audit, clock, deadLetterEnabled);
    }

    /**
     * As {@link #ParsleyEngine(ParsleyFrontier, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyMetrics, CausalAudit, LongSupplier, boolean) the full constructor}, with dead-lettering off.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock) {
        this(frontier, buffer, candidateIndex, metrics, audit, clock, false);
    }

    /**
     * As {@link #ParsleyEngine(ParsleyFrontier, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyMetrics, CausalAudit, LongSupplier, boolean) the full constructor}, with every coordinate
     * in scope — for callers/tests that do not need to exercise per-task partition scoping.
     *
     * @param deadLetterEnabled whether a proven-impossible record (poison, or a dependent of an orphaned
     *                          coordinate) is dead-lettered via {@link Outcome#deadLettered()} rather
     *                          than failing the task fast. {@link ParsleyProcessor} derives this directly
     *                          from whether a dead-letter sink is configured — one source of truth, so a
     *                          caller that has not wired a DLQ never silently loses a record it can't
     *                          route anywhere.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean deadLetterEnabled) {
        this(frontier, buffer, candidateIndex, metrics, audit, clock, deadLetterEnabled, (topicId, partition) -> true);
    }

    /**
     * Full constructor. Takes a pre-built {@link ParsleyFrontier} — the single owner of the frontier
     * clock, channel clocks, forwarded index, and orphan index — so callers control its persistence (a
     * store-backed frontier in production, an in-memory one in tests).
     *
     * @param deadLetterEnabled whether a proven-impossible record (poison, or a dependent of an orphaned
     *                          coordinate) is dead-lettered via {@link Outcome#deadLettered()} rather
     *                          than failing the task fast. {@link ParsleyProcessor} derives this directly
     *                          from whether a dead-letter sink is configured — one source of truth, so a
     *                          caller that has not wired a DLQ never silently loses a record it can't
     *                          route anywhere.
     * @param inScope           coordinates this node could ever genuinely confirm (a registered input
     *                          channel, on the partition this task owns). A dependency outside this
     *                          scope fails closed (dead-lettered, or a hard task failure without a
     *                          dead-letter sink) rather than being silently treated as satisfied — see
     *                          {@link DeadLetter.Reason#UNREACHABLE_DEPENDENCY}.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 CausalAudit audit,
                 LongSupplier clock,
                 boolean deadLetterEnabled,
                 ParsleyClock.CoordinatePredicate inScope) {
        this.frontier = frontier;
        this.orphanIndex = frontier.orphanIndex();
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.metrics = metrics;
        this.audit = audit;
        this.clock = clock;
        this.deadLetterEnabled = deadLetterEnabled;
        this.inScope = inScope;
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path.
        for (ParsleyBufferStore.IndexEntry entry : buffer.indexEntries()) {
            candidateIndex.index(entry.sequence(),
                    effectiveDependencies(entry.dependencies(), entry.topicId(), entry.partition(), entry.offset()),
                    completeness());
        }
    }

    /**
     * The engine's per-call result: every record released for delivery, in order, and every record
     * removed from the causal path onto the dead-letter sink instead — disjoint outcomes for the same
     * call, never both for the same record.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record Outcome<K, V>(List<ParsleyMessage<K, V>> delivered, List<DeadLetter<K, V>> deadLettered) {
    }

    /**
     * A record removed from the causal execution path onto the dead-letter sink, because its
     * dependencies are proven impossible to satisfy — never because of pressure or time.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    sealed interface DeadLetter<K, V> {

        /** Why a record was dead-lettered. */
        enum Reason {
            /** A held record's value could no longer be deserialised on the forward path. */
            POISON,
            /** An inbound record's causal-dependencies header could not be decoded at ingest. */
            UNRESOLVABLE_CLOCK,
            /**
             * The record's own dependencies require a coordinate that a poison or unresolvable-clock
             * record elsewhere already proved can never advance again.
             */
            ORPHAN_CASCADE,
            /**
             * The record's own dependencies name a coordinate this node has no input channel for — an
             * undeclared topic, or a partition a different task instance owns — so it can never be
             * confirmed here no matter how long it waits. Fail-closed rather than vacuously satisfied:
             * this node can prove it cannot check the coordinate, not that the coordinate is genuinely
             * irrelevant.
             */
            UNREACHABLE_DEPENDENCY
        }

        Reason reason();

        String topic();

        Uuid topicId();

        int partition();

        long offset();

        /**
         * A dead-lettered record whose key and value decoded successfully — an unresolvable-clock
         * record (only its dependencies header failed) or an orphan-cascade victim that is not itself
         * undecodable.
         */
        record Decoded<K, V>(ParsleyMessage<K, V> record, Reason reason) implements DeadLetter<K, V> {
            @Override
            public String topic() {
                return record.topic();
            }

            @Override
            public Uuid topicId() {
                return record.topicId();
            }

            @Override
            public int partition() {
                return record.partition();
            }

            @Override
            public long offset() {
                return record.offset();
            }
        }

        /**
         * A dead-lettered record whose value could not be decoded at all — a poison record, or an
         * orphan-cascade victim that is also poison. {@link #rawKey()}/{@link #rawValue()} are the raw
         * bytes the user's own serializer produced (recovered before the failed decode attempt), carried
         * onto the dead-letter topic since the record's own {@code V} could never be reconstructed.
         */
        record Undecodable<K, V>(String topic, Uuid topicId, int partition, long offset, long timestamp,
                                 List<ParsleyHeader> headers, byte @Nullable [] rawKey, byte @Nullable [] rawValue,
                                 Reason reason) implements DeadLetter<K, V> {
        }
    }

    /**
     * Processes one incoming record.
     *
     * @param message the record to process
     * @return the records to forward downstream (in order) and any dead-lettered in the process
     */
    Outcome<K, V> onRecord(ParsleyMessage<K, V> message) {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        List<DeadLetter<K, V>> deadLetters = new ArrayList<>();
        boolean channelAdvanced = false;

        if (frontier.seedIfFirstSeen(message.topicId(), message.partition(), message.offset())) {
            propagate(out, deadLetters, message.topicId(), message.partition());
        }

        // A record whose dependencies name a coordinate this node has no channel for at all can never
        // be checked here no matter how long it waits. Fail-closed rather than vacuously satisfied:
        // this node can prove it cannot check the coordinate, never that the coordinate is irrelevant.
        if (isUnreachableDependency(message)) {
            if (deadLetterEnabled) {
                log.warn("Dead-lettering {}-{} @{} (depends on a coordinate this node has no channel for)",
                        message.topic(), message.partition(), message.offset());
                deadLetterRoot(deadLetters, new DeadLetter.Decoded<>(message, DeadLetter.Reason.UNREACHABLE_DEPENDENCY));
            } else {
                throw failUnreachableDependency(message.topic(), message.topicId(), message.partition(), message.offset());
            }
        // A record whose dependencies already require a coordinate this node has proven can never
        // advance again is provably undeliverable — dead-letter it immediately rather than buffer it
        // forever against a gate that can now never be satisfied.
        } else if (isProvenImpossible(message)) {
            log.warn("Dead-lettering {}-{} @{} (depends on a coordinate proven never to advance again)",
                    message.topic(), message.partition(), message.offset());
            deadLetterRoot(deadLetters, new DeadLetter.Decoded<>(message, DeadLetter.Reason.ORPHAN_CASCADE));
        } else {
            ParsleyClock deps = effectiveDependencies(message.dependencies(),
                    message.topicId(), message.partition(), message.offset());

            // Every record is checked against this node's own current, already-proven state — never
            // against a stamp this same record just supplied. A message's declared dependencies are
            // only trustworthy once genuinely delivered somewhere; merging them in before that check
            // would let a record prove its own prerequisite by merely asserting it. This is the
            // uniform receive() predicate causal broadcast specifies: every message, including one
            // this node itself might be the sole witness to, is evaluated identically against the
            // receiver's actual current knowledge, never pre-loaded with its own claim.
            if (isDeliverable(message)) {
                log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                        message.topic(), message.partition(), message.offset());
                audit.recordForwarded(message.topic(), message.partition(), message.offset());
                frontier.deliver(message.topicId(), message.partition(), message.offset());
                // The channel-clock update happens only now, at genuine gated delivery — the message
                // has just proven its own claim against real, prior state, so folding its declared
                // deps in here is safe and lets other channels benefit from what it genuinely proved.
                ParsleyClock channelBefore = frontier.channelGet(message.topicId(), message.partition());
                channelAdvanced = !channelBefore.dominates(message.dependencies());
                frontier.channelUpdate(message.topicId(), message.partition(), message.dependencies());
                out.add(message);
                propagate(out, deadLetters, message.topicId(), message.partition());
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
            }
        }

        // A channel-clock advance can satisfy the completeness gate for records already held that are
        // waiting on a coordinate this channel just advertised. That release is not reachable through
        // the candidate index that drives propagate() (which keys on frontier advances), so re-scan
        // the buffer. Bounded to a fan-in (>1 channel) that actually advanced: a single-channel
        // processor's completeness is its own frontier, fully covered by propagate.
        if (channelAdvanced && frontier.channelCount() > 1) {
            drainSatisfied(out, deadLetters);
        }
        // A normal delivery can advance completeness past a pending epoch boundary's floor, closing the
        // transition window; a raised floor can then strip a held replay record's below-floor deps.
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out, deadLetters);
        }
        return new Outcome<>(out, deadLetters);
    }

    /**
     * Handles a received epoch-boundary marker: records the marker on its source channel in the
     * {@link ParsleyEpochState}, then closes the transition window if it is now ready (marker on every
     * channel and the delivered frontier dominating the new floor), draining any records the raised
     * floor releases. Mirrors {@link #onWatermark}. The marker itself is never delivered or buffered;
     * the caller ({@link ParsleyProcessor}) emits a downstream watermark so the completeness change
     * propagates, but never re-emits the marker (the coordinator broadcasts it to every channel).
     *
     * @param boundary        the decoded boundary (epoch id + new lower bounds)
     * @param channelTopicId  the topic UUID of the channel the marker arrived on
     * @param channelPartition the partition of that channel
     * @return the records released by a resulting window close, and any dead-lettered in the process
     */
    Outcome<K, V> onEpochBoundary(ParsleyEpochBoundary boundary, Uuid channelTopicId, int channelPartition) {
        frontier.recordEpochMarker(boundary.epochId(), boundary.lowerBounds(), channelTopicId, channelPartition);
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        List<DeadLetter<K, V>> deadLetters = new ArrayList<>();
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out, deadLetters);
        }
        return new Outcome<>(out, deadLetters);
    }

    /**
     * Releases every buffered record that passes the causal gate against the current frontier and
     * channel-clock state, and dead-letters every buffered record proven never to be satisfiable (its
     * dependencies require a coordinate {@link #orphanIndex} already recorded as unreachable). Used on
     * two call paths:
     * <ol>
     *   <li>Via {@link #drainAfterRestore()} — once, from the 1ms post-init punctuator in
     *       {@link ParsleyProcessor}, to drain records that were satisfied between the last
     *       committed frontier and the last committed buffer-removal (the at-least-once window), and
     *       to complete any orphan cascade interrupted by a crash mid-pass. On fresh starts (empty
     *       buffer) this returns empty.
     *   <li>Via {@link #onWatermark(Uuid, int, ParsleyClock)} — after a watermark advances a
     *       channel clock, which can unlock records held by the cross-channel ancestor check (part
     *       2 of the gate) even when the in-scope frontier has not advanced.
     * </ol>
     *
     * <p>This is an O(buffer-depth) full scan by design — correctness-first choice; the
     * candidate-index fast path in {@link #propagate} handles the common frontier-advance case, and
     * {@link #orphan} handles the common orphan-floor-advance case.
     *
     * <p>Iterates {@link #orderedIndex()} (index metadata in causal arrival order) and gates each
     * entry on its dependency clock alone — never decoding the user value — so a held, undecodable
     * record that is not releasable/dead-letterable is skipped without deserialisation. Only a record
     * that passes one of the two checks is fetched (and, if its value is undecodable on that forward
     * path, handled by the same disposition {@link #propagate} would apply). The {@link
     * ParsleyBufferStore#get(long)} {@code null} guard skips entries already removed by an earlier step
     * in this same pass (a release or a dead-letter cascade).
     */
    private void drainSatisfied(List<ParsleyMessage<K, V>> out, List<DeadLetter<K, V>> deadLetters) {
        for (ParsleyBufferStore.IndexEntry meta : orderedIndex()) {
            // Normally unreachable: onRecord already rejects an unreachable-dependency record before
            // it is ever buffered. This only catches a record buffered by an older binary version
            // (before this check existed) surviving a restart onto this one.
            if (isUnreachableDependency(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                if (deadLetterEnabled) {
                    DeadLetter<K, V> letter = fetchForDeadLetter(meta.sequence(), DeadLetter.Reason.UNREACHABLE_DEPENDENCY);
                    if (letter != null) {
                        deadLetterRoot(deadLetters, letter);
                    }
                } else {
                    throw failUnreachableDependency(meta.topic(), meta.topicId(), meta.partition(), meta.offset());
                }
                continue;
            }
            if (isProvenImpossible(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                DeadLetter<K, V> letter = fetchForDeadLetter(meta.sequence(), DeadLetter.Reason.ORPHAN_CASCADE);
                if (letter != null) {
                    deadLetterRoot(deadLetters, letter);
                }
                continue;
            }
            // The completeness gate, on metadata only: every declared coordinate (self-cycle stripped,
            // out-of-scope already rejected above) is within the max-merge completeness frontier — a
            // single genuine witness among this node's input channels suffices.
            if (!isDeliverable(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                continue;
            }
            ParsleyBufferStore.Entry<K, V> entry;
            try {
                entry = buffer.get(meta.sequence());
            } catch (ParsleyBufferDeserializationException e) {
                if (deadLetterEnabled) {
                    DeadLetter<K, V> letter = undecodable(e, DeadLetter.Reason.POISON);
                    // Durably orphan this coordinate before removing the record from the buffer — see
                    // the class Javadoc's "Dead-letter persistence ordering" — so a crash between the
                    // two writes always tears toward "orphan floor recorded, victim still buffered".
                    orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
                    buffer.remove(meta.sequence());
                    deadLetterRoot(deadLetters, letter);
                    continue;
                }
                failPoison(e);                                             // fail closed
                throw e;
            }
            if (entry == null) continue;                                   // removed by a cascade this pass
            ParsleyMessage<K, V> record = entry.record();
            // Persist the frontier/forwarded-index advance before removing from the buffer: if a crash
            // tears these two changelog writes apart, the buffer still holds a record the frontier has
            // already recorded as delivered, which drainAfterRestore redelivers as a harmless
            // at-least-once duplicate — never the reverse (buffer gone, frontier never advanced), which
            // would permanently strand this coordinate's frontier.
            frontier.deliver(record.topicId(), record.partition(), record.offset());
            frontier.channelUpdate(record.topicId(), record.partition(), record.dependencies());
            buffer.remove(meta.sequence());
            audit.recordForwarded(record.topic(), record.partition(), record.offset());
            out.add(record);
            propagate(out, deadLetters, record.topicId(), record.partition());
        }
    }

    /**
     * Delegates to {@link #drainSatisfied}. Called once, via the 1ms post-init punctuator in
     * {@link ParsleyProcessor}, to drain records that were satisfied between the last committed
     * frontier and the last committed buffer-removal, and to complete any orphan cascade an earlier
     * crash interrupted mid-pass.
     *
     * @return the records released, and any dead-lettered in the process
     */
    Outcome<K, V> drainAfterRestore() {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        List<DeadLetter<K, V>> deadLetters = new ArrayList<>();
        drainSatisfied(out, deadLetters);
        return new Outcome<>(out, deadLetters);
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
     * @return the records released from the buffer by the drain, and any dead-lettered in the process
     */
    Outcome<K, V> onWatermark(Uuid sourceTopicId, int sourcePartition, ParsleyClock frontierClock) {
        frontier.channelUpdate(sourceTopicId, sourcePartition, frontierClock);
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        List<DeadLetter<K, V>> deadLetters = new ArrayList<>();
        drainSatisfied(out, deadLetters);
        // A watermark advances completeness, which can close a pending epoch transition window.
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out, deadLetters);
        }
        return new Outcome<>(out, deadLetters);
    }

    /** The buffer's metadata index, oldest-first (by insertion sequence); never decodes a value. */
    private List<ParsleyBufferStore.IndexEntry> orderedIndex() {
        List<ParsleyBufferStore.IndexEntry> all = new ArrayList<>(buffer.indexEntries());
        all.sort(java.util.Comparator.comparingLong(ParsleyBufferStore.IndexEntry::sequence));
        return all;
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
     * Returns the causal completeness clock: this node's own delivered frontier, max-merged with every
     * input channel's advertised dependencies.
     *
     * <p>A single genuine witness is enough for a foreign coordinate to count — there is no requirement
     * that every input channel independently confirm the same coordinate. This node's own coordinates
     * always win the merge against a channel's view of the same coordinate: delivering a channel's
     * record already required the gate to dominate its stamp, so the frontier can never be behind what
     * any channel could tell it. This is structurally a per-process vector clock (Birman-Schiper-
     * Stephenson CBCAST, instantiated on Kafka's own {@code (topicId, partition)} coordinates): the
     * topology must still make every coordinate a message could ever depend on reachable to this node,
     * directly or via a genuine relay, but no channel needs to independently corroborate a fact another
     * channel has already genuinely reported. This is the delivery gate and the outbound stamp.
     *
     * <p>When no channel clocks have been recorded, this returns {@link #frontier()} unchanged. The
     * computation lives on {@link ParsleyFrontier}, which owns both the frontier clock and the channel
     * clocks; this method delegates.
     *
     * @return the completeness clock; never {@code null}
     */
    ParsleyClock completeness() {
        return frontier.completeness();
    }

    /**
     * Propagates a frontier advancement: releases every buffered record that became causally
     * satisfiable because {@code (topicId, partition)} just advanced, then cascades — each
     * released record advances its own source coordinate, which may satisfy further records.
     * This is Lamport's transitivity rule: if A → B and A has been delivered, B can now be
     * delivered; and if B → C, C follows in the same pass. A candidate whose value cannot be
     * decoded on this forward attempt is dead-lettered (when enabled) rather than failing the task,
     * which starts its own orphan cascade via {@link #deadLetterRoot}.
     *
     * <p>Each candidate is committed to its final disposition — delivered or dead-lettered — the
     * moment that disposition is decided, never staged for a later batch step. A candidate is only
     * ever evaluated once per level (the {@code seen} guard), but a <em>different</em> candidate
     * later in the same scan can still dead-letter it first via an orphan cascade
     * ({@link #deadLetterRoot} → {@link #orphan} → {@link #fetchForDeadLetter}); committing
     * immediately, rather than collecting a batch of "releasable" entries to remove afterwards,
     * ensures that once a candidate is delivered it is already gone from the buffer, so a
     * same-pass cascade reaching it afterwards finds nothing there (the existing, harmless "stale"
     * handling) instead of a still-present entry it can dead-letter out from under the delivery
     * that already happened. This is what keeps {@link Outcome#delivered()} and
     * {@link Outcome#deadLettered()} disjoint.
     */
    private void propagate(List<ParsleyMessage<K, V>> out, List<DeadLetter<K, V>> deadLetters,
                           Uuid topicId, int partition) {
        Map<Uuid, Set<Integer>> toScan = new HashMap<>();
        toScan.computeIfAbsent(topicId, k -> new HashSet<>()).add(partition);
        int totalReleased = 0;

        while (!toScan.isEmpty()) {
            Set<Long> seen = new HashSet<>();
            List<ParsleyCandidateIndex.Candidate> stale = new ArrayList<>();
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
                            if (deadLetterEnabled) {
                                DeadLetter<K, V> letter = undecodable(e, DeadLetter.Reason.POISON);
                                // Durably orphan this coordinate before removing the record from the
                                // buffer — see the class Javadoc's "Dead-letter persistence ordering".
                                orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
                                buffer.remove(candidate.recordId());
                                deadLetterRoot(deadLetters, letter);
                                continue;
                            }
                            failPoison(e);                                     // fail closed
                            throw e;
                        }
                        if (entry == null) {
                            stale.add(candidate);
                            continue;
                        }
                        // Normally unreachable here — onRecord already rejects an unreachable-dependency
                        // record before it is ever buffered — but a record buffered by an older binary
                        // (before this check existed) can still surface it after a restart.
                        if (isUnreachableDependency(entry.record())) {
                            if (deadLetterEnabled) {
                                DeadLetter<K, V> letter = new DeadLetter.Decoded<>(
                                        entry.record(), DeadLetter.Reason.UNREACHABLE_DEPENDENCY);
                                orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
                                buffer.remove(entry.sequence());
                                deadLetterRoot(deadLetters, letter);
                                continue;
                            }
                            throw failUnreachableDependency(entry.record().topic(), entry.record().topicId(),
                                    entry.record().partition(), entry.record().offset());
                        }
                        // Proven-impossible takes precedence over deliverable, exactly as it does in
                        // onRecord and drainSatisfied: a candidate's own dependency can be orphaned by
                        // an unrelated cascade without ever being indexed against that coordinate here
                        // (it looked satisfied, via cross-channel header advertisement, at buffer
                        // time), so orphan()'s cascade for that coordinate never reaches it directly.
                        if (isProvenImpossible(entry.record())) {
                            DeadLetter<K, V> letter =
                                    new DeadLetter.Decoded<>(entry.record(), DeadLetter.Reason.ORPHAN_CASCADE);
                            // See the class Javadoc's "Dead-letter persistence ordering": mark orphaned
                            // before the buffer removal, not after.
                            orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
                            buffer.remove(entry.sequence());
                            deadLetterRoot(deadLetters, letter);
                            continue;
                        }
                        if (!isDeliverable(entry.record())) continue;

                        // See drainSatisfied: persist the frontier/forwarded-index advance before
                        // removing from the buffer, so a torn crash always strands the buffer with an
                        // already-delivered record (a benign duplicate on restart) rather than the
                        // reverse (a permanently stranded frontier).
                        frontier.deliver(entry.record().topicId(), entry.record().partition(), entry.record().offset());
                        frontier.channelUpdate(entry.record().topicId(), entry.record().partition(), entry.record().dependencies());
                        buffer.remove(entry.sequence());
                        audit.recordReleased(entry.record().topic(), entry.record().partition(),
                                entry.record().offset(), buffer.size());
                        nextScan.computeIfAbsent(entry.record().topicId(), k -> new HashSet<>())
                                .add(entry.record().partition());
                        out.add(entry.record());
                        totalReleased++;
                    }
                }
            }

            stale.forEach(candidateIndex::prune);
            toScan = nextScan;
        }

        if (totalReleased > 0) {
            log.debug("Released {} record(s) from buffer (depth now {})", totalReleased, buffer.size());
            metrics.recordReleased(totalReleased);
            reportBufferState();
        }
    }

    /**
     * Marks {@code (topicId, partition)} orphaned from {@code floor} onward — its frontier can never
     * legitimately advance past {@code floor - 1} again — then cascades: worklist-scans this node's
     * buffer for candidates requiring that coordinate at or beyond {@code floor} via {@link
     * ParsleyCandidateIndex#findCandidatesRequiringAtLeast}, dead-letters each (reason {@link
     * DeadLetter.Reason#ORPHAN_CASCADE}), and enqueues its own source coordinate in turn — Lamport
     * transitivity in reverse: if A can never be delivered, anything depending on A can never be
     * delivered either. Iterative (an explicit worklist, not recursion) so an arbitrarily long buffered
     * dependency chain cannot grow the call stack. A no-op if dead-lettering is disabled, or if the
     * coordinate is already orphaned at a floor {@code <= floor} (idempotent — a cascade an earlier
     * crash interrupted is safely resumed by a later {@link #drainSatisfied} pass finding nothing new
     * to do for the coordinates it already reached).
     *
     * <p>Deliberately does not eagerly prune a dead-lettered victim's <em>other</em> candidate-index
     * entries (for coordinates besides the one that triggered its removal) — mirrors {@link #propagate}'s
     * existing lazy-pruning philosophy: a stale entry is harmless, cleaned up the next time that other
     * coordinate happens to advance (or orphan) and finds {@link ParsleyBufferStore#get} return
     * {@code null}. That "next time" never arrives for an <em>orphaned</em> coordinate's own candidate
     * entry, though: unlike a normal coordinate, an orphaned one never advances again, so {@link
     * #propagate}/{@link #drainSatisfied} never revisit it — a stale entry {@code findCandidatesRequiringAtLeast}
     * turns up here (a candidate already removed by an earlier step this same pass) is therefore pruned
     * immediately, in the {@code letter == null} branch below, rather than left to a revisit that can't
     * happen.
     *
     * <p>{@code markOrphaned} is called unconditionally for every task, including one whose coordinate a
     * caller (or an earlier task in this same worklist, via {@link #fetchForDeadLetter}) already marked
     * orphaned before ever reaching here — see the class Javadoc's "Dead-letter persistence ordering".
     * That makes the call idempotent in the common case, so scanning for dependents cannot be gated on
     * its return value the way it used to be (a re-mark of an already-orphaned coordinate would then
     * always report "nothing new" and skip the scan, truncating the cascade at depth one). Scanning is
     * instead gated on {@code lowestScannedFloor}, local to this call: a coordinate can be discovered
     * more than once in one cascade, via different parent coordinates, at <em>different</em> floors (the
     * FIFO worklist gives no ordering guarantee across branches) — a task at a higher floor than one
     * already scanned for this coordinate would, under a plain "scanned once" set, wrongly skip the
     * range {@code [thisFloor, previouslyScannedFloor)} forever. Recording the lowest floor scanned so
     * far and rescanning whenever a new task's floor is strictly lower closes that gap: {@link
     * ParsleyCandidateIndex#findCandidatesRequiringAtLeast} is monotonically broader as the floor drops,
     * so a rescan at a lower floor is a strict superset of any narrower scan already done, and a task at
     * a floor already covered is skipped as before — strictly safer than a single boolean, just
     * occasionally redundant, harmless work.
     */
    private void orphan(List<DeadLetter<K, V>> deadLetters, Uuid topicId, int partition, long floor) {
        if (!deadLetterEnabled) {
            return;
        }
        Deque<OrphanTask> worklist = new ArrayDeque<>();
        worklist.add(new OrphanTask(topicId, partition, floor));
        Set<Long> seenRecords = new HashSet<>();
        Map<Coord, Long> lowestScannedFloor = new HashMap<>();
        while (!worklist.isEmpty()) {
            OrphanTask task = worklist.poll();
            orphanIndex.markOrphaned(task.topicId(), task.partition(), task.floor());
            Coord coord = new Coord(task.topicId(), task.partition());
            Long recorded = lowestScannedFloor.get(coord);
            if (recorded != null && task.floor() >= recorded) {
                continue;                                                 // already scanned at this floor or lower
            }
            lowestScannedFloor.put(coord, task.floor());
            for (ParsleyCandidateIndex.Candidate candidate : candidateIndex.findCandidatesRequiringAtLeast(
                    task.topicId(), task.partition(), task.floor())) {
                if (!seenRecords.add(candidate.recordId())) continue;
                DeadLetter<K, V> letter = fetchForDeadLetter(candidate.recordId(), DeadLetter.Reason.ORPHAN_CASCADE);
                if (letter == null) {
                    // Removed by an earlier step this pass. Unlike propagate()'s lazy-pruning (see this
                    // method's Javadoc), an orphaned coordinate never advances again, so nothing else
                    // will ever revisit and prune this stale entry — do it now.
                    candidateIndex.prune(candidate);
                    continue;
                }
                recordDeadLetter(deadLetters, letter);
                // The victim's own offset — not offset + 1 — is the first unreachable point: the
                // victim itself will never be marked delivered, so a dependent requiring exactly this
                // offset (not merely something later) must also be caught.
                worklist.add(new OrphanTask(letter.topicId(), letter.partition(), letter.offset()));
            }
        }
    }

    private record OrphanTask(Uuid topicId, int partition, long floor) {
    }

    /** A bare coordinate, ignoring offset — used by {@link #orphan} to track which floor this call has
     * already scanned for dependents on each coordinate, independent of {@link
     * ParsleyOrphanIndex#markOrphaned}'s return value (see that method's Javadoc for why they must be
     * tracked separately). */
    private record Coord(Uuid topicId, int partition) {
    }

    /**
     * Fetches the buffered entry for {@code sequence}, durably orphans its own coordinate, and removes
     * it, producing the {@link DeadLetter} it becomes: {@link DeadLetter.Decoded} if it still
     * deserialises fine, {@link DeadLetter.Undecodable} (from the caught exception, same {@code reason})
     * if the fetch itself now fails — recursion into the poison disposition, unified rather than
     * special-cased. Returns {@code null} if the entry was already removed by an earlier step in this
     * same pass (stale, tolerated exactly like {@link #propagate}'s stale-candidate handling).
     *
     * <p>{@code markOrphaned} runs on the victim's own coordinate <em>before</em> {@code buffer.remove} —
     * see the class Javadoc's "Dead-letter persistence ordering" — so a crash between the two writes
     * always tears toward "orphan floor recorded, victim still buffered", never the reverse.
     */
    private @Nullable DeadLetter<K, V> fetchForDeadLetter(long sequence, DeadLetter.Reason reason) {
        ParsleyBufferStore.Entry<K, V> entry;
        try {
            entry = buffer.get(sequence);
        } catch (ParsleyBufferDeserializationException e) {
            DeadLetter<K, V> letter = undecodable(e, reason);
            orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
            buffer.remove(sequence);
            return letter;
        }
        if (entry == null) {
            return null;
        }
        DeadLetter<K, V> letter = new DeadLetter.Decoded<>(entry.record(), reason);
        orphanIndex.markOrphaned(letter.topicId(), letter.partition(), letter.offset());
        buffer.remove(entry.sequence());
        return letter;
    }

    /** Builds a {@link DeadLetter.Undecodable} from a caught {@link ParsleyBufferDeserializationException}. */
    private DeadLetter.Undecodable<K, V> undecodable(ParsleyBufferDeserializationException e, DeadLetter.Reason reason) {
        return new DeadLetter.Undecodable<>(e.topic(), e.topicId(), e.partition(), e.offset(), e.timestamp(),
                e.headers(), e.rawKey(), e.rawValue(), reason);
    }

    /**
     * Records a single top-level dead-letter event (not one found mid-cascade by {@link #orphan}
     * itself, which enqueues directly onto its own worklist to stay iterative): appends {@code letter}
     * to the outcome, audits it, then starts a cascade from its own coordinate onward — the record that
     * was just dead-lettered can itself never be delivered, so anything depending on it is provably
     * doomed too.
     */
    private void deadLetterRoot(List<DeadLetter<K, V>> deadLetters, DeadLetter<K, V> letter) {
        recordDeadLetter(deadLetters, letter);
        // The dead-lettered record's own offset is the first offset its coordinate's frontier can
        // never legitimately reach (see #orphan) — not offset + 1.
        orphan(deadLetters, letter.topicId(), letter.partition(), letter.offset());
    }

    /**
     * The ingest-time counterpart to {@link #deadLetterRoot}, for a record dead-lettered before it ever
     * became engine state — an {@code UNRESOLVABLE_CLOCK} record, whose causal-dependencies header could
     * not even be decoded, so {@link #onRecord} is never called for it and it has no candidate-index or
     * frontier footprint of its own to unwind. The caller ({@link ParsleyProcessor#onUnresolvableClock})
     * has already recorded and forwarded the root record itself; this only runs the {@link #orphan}
     * cascade — marking {@code (topicId, partition)} permanently unable to advance past {@code offset}
     * and dead-lettering any already-buffered dependent — so the coordinate's contiguous frontier is not
     * left frozen forever with nothing durably recording why.
     *
     * @param topicId   the topic UUID of the record's own coordinate
     * @param partition the partition of the record's own coordinate
     * @param offset    the record's own offset — the first point its coordinate can never reach
     * @return an outcome whose {@code delivered} is always empty (orphaning never releases anything) and
     *         whose {@code deadLettered} holds every buffered dependent the cascade caught, if any
     */
    Outcome<K, V> deadLetterAtIngest(Uuid topicId, int partition, long offset) {
        List<DeadLetter<K, V>> deadLetters = new ArrayList<>();
        orphan(deadLetters, topicId, partition, offset);
        return new Outcome<>(List.of(), deadLetters);
    }

    private void recordDeadLetter(List<DeadLetter<K, V>> deadLetters, DeadLetter<K, V> letter) {
        deadLetters.add(letter);
        audit.recordDeadLetter(letter.topic(), letter.partition(), letter.offset(), letter.reason().name());
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
     * stripped) is within the {@link #completeness()} frontier — i.e. confirmed by at least one input
     * channel (max-merge; a single genuine witness suffices, see {@link #completeness()}). This is the
     * single source of truth for "may this record be delivered now", used on every release path
     * ({@link #onRecord}, {@link #drainSatisfied}, {@link #propagate}).
     */
    private boolean isDeliverable(ParsleyMessage<K, V> record) {
        return isDeliverable(record.dependencies(), record.topicId(), record.partition(), record.offset());
    }

    /**
     * Metadata overload of the gate: evaluates deliverability from a record's dependency clock and
     * source coordinate alone, without decoding its user value. Used by {@link #drainSatisfied} so a
     * held, undecodable record that is not releasable is never deserialised.
     *
     * <p>The completeness check runs against {@link #effectiveDependencies}, never the raw clock — see
     * that method for the preprocessing steps. Only ever called on a record already proven reachable
     * ({@link #isUnreachableDependency} false) — {@code effectiveDependencies} does not itself filter
     * out-of-scope coordinates, so this would otherwise wait forever on one.
     */
    private boolean isDeliverable(ParsleyClock dependencies, Uuid topicId, int partition, long offset) {
        return completeness().dominates(effectiveDependencies(dependencies, topicId, partition, offset));
    }

    /**
     * Returns {@code true} if any coordinate {@code message} depends on ({@link #effectiveDependencies},
     * the same preprocessing {@link #isDeliverable} applies) names a coordinate outside {@link #inScope}
     * — this node has no input channel for it at all, so it can never be confirmed here no matter how
     * long it waits. Checked independently of {@link #deadLetterEnabled}: unlike {@link
     * #isProvenImpossible}, scope is a static, structural fact known regardless of whether a dead-letter
     * sink is configured, so this must still fail closed (a hard task failure) rather than silently pass
     * when there is nowhere to divert the record.
     */
    private boolean isUnreachableDependency(ParsleyMessage<K, V> message) {
        return isUnreachableDependency(message.dependencies(), message.topicId(), message.partition(), message.offset());
    }

    private boolean isUnreachableDependency(ParsleyClock dependencies, Uuid topicId, int partition, long offset) {
        ParsleyClock effective = effectiveDependencies(dependencies, topicId, partition, offset);
        boolean[] unreachable = {false};
        effective.forEach((depTopicId, depPartition, requiredOffset) -> {
            if (!inScope.test(depTopicId, depPartition)) {
                unreachable[0] = true;
            }
        });
        return unreachable[0];
    }

    /**
     * Builds (but does not throw) the exception for a record whose dependencies name a coordinate this
     * node has no channel for, with no dead-letter sink configured to divert it instead. Records the
     * failure via {@link #metrics}/{@link #audit} first, mirroring {@link #failPoison}, then returns the
     * exception for the caller to throw — never buffering the record and never treating the unreachable
     * coordinate as satisfied.
     */
    private ParsleyUnreachableDependencyException failUnreachableDependency(
            String topic, Uuid topicId, int partition, long offset) {
        metrics.recordUnreachableDependencyError();
        audit.recordUnreachableDependencyFailure(topic, partition, offset,
                "depends on a coordinate this node has no channel for");
        log.error("{}-{} @{} depends on a coordinate this node has no channel for; failing fast "
                + "(fail-closed). The record was not forwarded and is reprocessed on restart.",
                topic, partition, offset);
        return new ParsleyUnreachableDependencyException(topic, topicId, partition, offset);
    }

    /**
     * Returns {@code true} if any coordinate {@code message} depends on ({@link #effectiveDependencies},
     * the same preprocessing {@link #isDeliverable} applies) requires an offset this node has already
     * proven that coordinate can never legitimately reach — i.e. an orphan floor at or below the
     * required offset is recorded in {@link #orphanIndex}. Always {@code false} when dead-lettering
     * is disabled, so the check costs nothing and changes nothing for a caller that hasn't opted in.
     * Only ever called on a record already proven reachable — see {@link #isUnreachableDependency}.
     */
    private boolean isProvenImpossible(ParsleyMessage<K, V> message) {
        return isProvenImpossible(message.dependencies(), message.topicId(), message.partition(), message.offset());
    }

    private boolean isProvenImpossible(ParsleyClock dependencies, Uuid topicId, int partition, long offset) {
        if (!deadLetterEnabled) {
            return false;
        }
        ParsleyClock effective = effectiveDependencies(dependencies, topicId, partition, offset);
        boolean[] impossible = {false};
        effective.forEach((depTopicId, depPartition, requiredOffset) -> {
            long floor = orphanIndex.orphanFloor(depTopicId, depPartition);
            if (floor >= 0 && requiredOffset >= floor) {
                impossible[0] = true;
            }
        });
        return impossible[0];
    }

    /**
     * The dependency clock actually checked by the gate: two view-only preprocessing steps, neither of
     * which ever rewrites recorded state or the outbound stamp (which is completeness(), computed
     * separately and carrying the raw, unfiltered picture for transitive downstream propagation).
     * <ol>
     *   <li>The record's exact self-cycle is removed ({@link #withoutSelfReference}) — a record
     *       depending on its own {@code (topicId, partition, offset)} has, by being delivered, met that
     *       dependency, so it must not wait on itself (this keeps a self-referential stamp on a fused
     *       chain from deadlocking). A backward same-partition dependency ({@code req < offset}) is an
     *       intra-topic dependency like any other and flows through unchanged.</li>
     *   <li>Any dependency below its coordinate's topology-epoch {@code startsAt} bound is stripped
     *       ({@link ParsleyClock#strippedBelow}) — an out-of-domain reference to a prior, closed epoch
     *       that no channel in this epoch will ever confirm. A no-op with epoch bounding disabled.</li>
     * </ol>
     * <p>Unlike an earlier version of this method, a coordinate outside {@link #inScope} is
     * <strong>not</strong> dropped here — this node cannot prove such a coordinate is safe to disregard,
     * only that it cannot check it, so it is never silently treated as satisfied. {@link
     * #isUnreachableDependency} checks for one before this result is ever handed to {@link
     * #isDeliverable} or {@link #isProvenImpossible}, and fails closed (dead-letter, or a hard task
     * failure with no dead-letter sink configured) instead.
     */
    private ParsleyClock effectiveDependencies(ParsleyClock deps, Uuid topicId, int partition, long offset) {
        return withoutSelfReference(deps, topicId, partition, offset)
                .strippedBelow(frontier.epoch());
    }

    private ParsleyClock withoutSelfReference(ParsleyClock deps, Uuid topicId, int partition, long offset) {
        if (deps.offsetFor(topicId, partition) == offset) {
            return deps.without(topicId, partition);
        }
        return deps;
    }

    /**
     * Records a held record that could not be deserialised on the forward path: logs the
     * (payload-free) diagnostic and counts the error, then leaves the caller to rethrow. Delivery is
     * fail-closed — the record is not dropped and not forwarded; it remains in the buffer changelog and
     * the task fails, so it can be recovered once the schema is fixed or rolled back. Called only when
     * {@link #deadLetterEnabled} is {@code false}; with a dead-letter sink configured, {@link
     * #propagate}/{@link #drainSatisfied} divert the record instead (see {@link #undecodable}).
     */
    private void failPoison(ParsleyBufferDeserializationException e) {
        metrics.recordDeserializationError();
        audit.recordDeserializationFailure(e.topic(), e.partition(), e.offset(), e.details());
        log.error("Buffered record could not be deserialised; failing fast (fail-closed). "
                + "It remains in the buffer changelog for recovery. {}", e.details(), e);
    }

}
