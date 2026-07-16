package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The causal buffering engine.
 *
 * <p><strong>Vocabulary.</strong> This class is the receive-and-deliver half of the classic causal
 * broadcast algorithm (Birman-Schiper-Stephenson CBCAST; see {@link #completeness()}) — broadcast itself
 * is layered underneath, not reimplemented here. <em>Broadcast</em> is Kafka's own {@code
 * ProducerRecord}/{@code context.forward()} (a partition's total order and replication are already a
 * reliable-broadcast substrate) plus the causal-metadata attachment classic algorithms add atop plain
 * broadcast ({@link CausalDependencies#stamp} at the edge, {@link ParsleyProcessorContext} internally).
 * <em>Receive</em> is {@link #receive}: a record arrives on an input channel and is either delivered at
 * once or buffered until it can be. <em>Deliver</em> is {@link ParsleyFrontier#deliver} — where the
 * causal-order guarantee is actually granted, surfaced here via {@link Outcome} and handed to the user's
 * delegate by {@code ParsleyProcessor#deliver}. {@link #onWatermark} and {@code ParsleyProcessor}'s
 * epoch-marker handlers sit on top of this algorithm as a liveness/coordination layer — protocol
 * extensions, not part of the CBCAST core.
 *
 * <p>The processor feeds incoming records to {@link #receive} and forwards the returned records
 * downstream, in order. Delivery is strictly fail-closed: a record is forwarded only after its
 * declared dependencies have been satisfied by this node's <em>own</em> contiguous frontier — the
 * positions this node has itself delivered, never a position a peer merely claims to have delivered
 * (see {@link #isDeliverable}). There is no eviction, no
 * buffer limit, and no timeout — a record whose dependencies are not yet satisfied stays buffered
 * (the buffer is a changelog-backed state store, so it spills to disk rather than growing in memory).
 * A record whose dependencies can be proven impossible to ever satisfy — an undecodable payload or
 * dependency header, or a dependency naming a coordinate this node has no input channel for at all —
 * unconditionally fails the task fast, rather than being forwarded downstream as if it were causally
 * valid or silently discarded. There is no diversion sink: this is the single, simple failure model —
 * any error is a hard stop, never a partial or best-effort continuation.
 *
 * <p><strong>The frontier is a contiguous watermark, not a running max.</strong> The engine does
 * not head-of-line block: a later-offset record on a partition may forward before an earlier one
 * still held on the same partition. So a coordinate's frontier offset must only ever advance once
 * every offset up to it has actually been forwarded — never past a gap, or a record elsewhere
 * depending on exactly the gapped offset would be released on bookkeeping alone, never having
 * actually observed it. {@link ParsleyFrontier#deliver} marks every forward in a {@link
 * ParsleyForwardedIndex} and walks forward from the current frontier to find the longest run of
 * consecutive forwarded offsets now achievable, advancing the frontier by that run and pruning the
 * absorbed entries. Every release calls it the same way — none are special-cased.
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
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleyEngine<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyEngine.class);

    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final ParsleyMetrics metrics;
    private final LongSupplier clock;

    // The single owner of all persisted causal metadata: the contiguous frontier clock, the channel
    // clocks, and the forwarded-offset index. completeness() and channel state live here, floored to
    // the topology epoch's lower bounds (frontier.epoch(), also read at the gate to strip a record's
    // out-of-domain dependencies; NONE when epoch bounding is disabled, so the floor is a no-op).
    private final ParsleyFrontier frontier;

    // Coordinates this node could ever genuinely confirm — a registered input channel, on the
    // partition this task owns. A dependency outside this scope (an undeclared topic, or a partition
    // a different task instance owns) can never be observed here no matter how long it waits — but
    // that does not make it safe to disregard: this node can prove it cannot check the coordinate,
    // never that the coordinate is genuinely irrelevant. So it is fail-closed, not vacuously
    // satisfied — see isUnreachableDependency. Defaults to "everything in scope" so existing
    // callers/tests that never construct with an explicit predicate are unaffected; ParsleyProcessor
    // passes its real per-task predicate.
    private final ParsleyClock.CoordinatePredicate inScope;

    // Coordinates for a topic THIS NODE ITSELF produces (a registered sink). Stripped from any inbound
    // dependency clock or marker-carried completeness before every gate check — see
    // effectiveDependencies and onWatermark — never merely marked "in scope". This is sound, and a
    // narrower, different case from the general out-of-scope rule above: a claim naming this node's own
    // coordinate can only ever have arisen from something THIS node itself already produced (nothing
    // else can ever advance it), so the claim is either already, trivially known here or could not have
    // legitimately arisen at all — there is nothing to verify, unlike a genuinely foreign coordinate
    // this node has no independent way to confirm. Concretely, this closes two related gaps: (1) a
    // downstream node's stamp reflecting this node's own coordinate back toward it (e.g. in a topology
    // cycle) would otherwise fail closed as "unreachable", even though the coordinate is this node's
    // own and therefore never actually unverifiable; and (2) a node that also directly consumes its own
    // sink (the tightest possible cycle) would otherwise never converge — every watermark it receives on
    // that channel carries its own ever-advancing self-position, which the plain out-of-scope logic
    // cannot distinguish from genuine foreign progress, so channelAdvanced never settles false and the
    // marker never stops relaying. Defaults to "nothing is ever this node's own sink" so existing
    // callers/tests that never construct with an explicit predicate are unaffected; ParsleyProcessor
    // passes its real per-stage sink-topic predicate.
    private final ParsleyClock.CoordinatePredicate ownSinkTopics;

    // --- Convenience constructors: build an in-memory ParsleyFrontier from an initial clock and a
    // forwarded index. Production and restart-style callers pass a pre-built ParsleyFrontier to the
    // full constructor below (so channel + frontier state can be shared/persisted). ---

    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics) {
        this(initialFrontier, buffer, candidateIndex, forwardedIndex, metrics, System::currentTimeMillis);
    }

    ParsleyEngine(ParsleyClock initialFrontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyForwardedIndex forwardedIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(new ParsleyFrontier(initialFrontier, forwardedIndex, false, ParsleyEpoch.NONE),
                buffer, candidateIndex, metrics, clock);
    }

    /**
     * As {@link #ParsleyEngine(ParsleyFrontier, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyMetrics, LongSupplier, ParsleyClock.CoordinatePredicate,
     * ParsleyClock.CoordinatePredicate) the full constructor}, with every coordinate in scope and
     * nothing treated as this node's own sink — for callers/tests that do not need to exercise
     * per-task partition scoping or own-coordinate stripping.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock) {
        this(frontier, buffer, candidateIndex, metrics, clock,
                (topicId, partition) -> true, (topicId, partition) -> false);
    }

    /**
     * As {@link #ParsleyEngine(ParsleyFrontier, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyMetrics, LongSupplier, ParsleyClock.CoordinatePredicate,
     * ParsleyClock.CoordinatePredicate) the full constructor}, with nothing ever treated as this node's
     * own sink — for callers/tests that do not need to exercise own-coordinate stripping.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock,
                 ParsleyClock.CoordinatePredicate inScope) {
        this(frontier, buffer, candidateIndex, metrics, clock, inScope, (topicId, partition) -> false);
    }

    /**
     * Full constructor. Takes a pre-built {@link ParsleyFrontier} — the single owner of the frontier
     * clock, channel clocks, and forwarded index — so callers control its persistence (a store-backed
     * frontier in production, an in-memory one in tests).
     *
     * @param inScope       coordinates this node could ever genuinely confirm (a registered input
     *                      channel, on the partition this task owns). A dependency outside this scope
     *                      fails the task fast rather than being silently treated as satisfied.
     * @param ownSinkTopics coordinates for a topic this node itself produces. Stripped from any
     *                      inbound dependency or marker clock before every gate check — see this
     *                      field's own Javadoc for the soundness argument.
     */
    ParsleyEngine(ParsleyFrontier frontier,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock,
                 ParsleyClock.CoordinatePredicate inScope,
                 ParsleyClock.CoordinatePredicate ownSinkTopics) {
        this.frontier = frontier;
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.metrics = metrics;
        this.clock = clock;
        this.inScope = inScope;
        this.ownSinkTopics = ownSinkTopics;
        List<ParsleyBufferStore.IndexEntry> restored = buffer.indexEntries();
        // Replay receive()'s first-sighting seed for every restored held record's source coordinate,
        // at its lowest held offset, BEFORE anything else can. ParsleyFrontier's "seen" guard is
        // in-memory and does not survive a restart, so without this a post-restart record arriving on
        // a coordinate whose only prior activity is a still-held record (frontier entry absent —
        // possible only when that record sits at offset 0, whose seed is a no-op) would re-trigger
        // the baseline seed and fold the held record's offset into the frontier as "outside the
        // engine's purview" — releasing records that depend on it before it has ever been delivered
        // (an effect-before-cause delivery). Seeding at the lowest held offset reproduces exactly
        // what the first in-run sighting did: marks the coordinate seen, and never seeds past a held
        // record. Out-of-scope coordinates are skipped — a held record on one fails the drain fast
        // anyway, and re-seeding an entry pruneToScope just removed would resurrect it.
        Map<Uuid, Map<Integer, Long>> lowestHeld = new HashMap<>();
        for (ParsleyBufferStore.IndexEntry entry : restored) {
            lowestHeld.computeIfAbsent(entry.topicId(), k -> new HashMap<>())
                    .merge(entry.partition(), entry.offset(), Math::min);
        }
        lowestHeld.forEach((topicId, byPartition) -> byPartition.forEach((partition, offset) -> {
            if (inScope.test(topicId, partition)) {
                frontier.seedIfFirstSeen(topicId, partition, offset);
            }
        }));
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path. Indexed against the
        // contiguous frontier — the same clock the delivery gate checks — never completeness: a
        // coordinate a channel merely claims (satisfied by completeness but not by the frontier)
        // must stay indexed, or the frontier advance that eventually genuinely proves it would find
        // no candidate to release.
        for (ParsleyBufferStore.IndexEntry entry : restored) {
            candidateIndex.index(entry.sequence(),
                    effectiveDependencies(entry.dependencies(), entry.topicId(), entry.partition(), entry.offset()),
                    frontier.snapshot());
        }
    }

    /**
     * The engine's per-call result: every record released for delivery, in order.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record Outcome<K, V>(List<ParsleyMessage<K, V>> delivered) {
    }

    /**
     * {@link #onWatermark}'s result: the ordinary {@link Outcome}, plus whether the marker's carried
     * clock genuinely taught this node something it did not already know on that channel. A marker's own
     * delivery must never itself count as a reason to relay further — only a genuine change does — or a
     * cyclic topology (a marker-only passthrough channel included) would ping-pong the same marker
     * forever; see {@link ParsleyProcessor}'s marker handlers, which gate their own downstream relay on
     * {@link #channelAdvanced}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record WatermarkOutcome<K, V>(Outcome<K, V> outcome, boolean channelAdvanced) {
    }

    /**
     * {@link #onEpochBoundary}'s result: the ordinary {@link Outcome}, plus whether the marker was
     * <em>newly recorded</em> for its epoch on its source channel. Unlike a watermark — whose relay is
     * gated on the carried clock teaching the channel something new — a boundary marker must relay on
     * its channel's first sight of it even when the carried completeness is nothing new (the idle,
     * quiesced round: the boundary carries the same completeness the preceding snapshot already
     * advertised). Gating boundary relay on this flag propagates the boundary to every channel exactly
     * once while a duplicate on an already-seen channel records nothing new and does not relay, so a
     * cyclic topology still cannot ping-pong it; see {@link ParsleyProcessor}'s marker handlers.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record BoundaryOutcome<K, V>(Outcome<K, V> outcome, boolean markerWasNew) {
    }

    /**
     * The causal broadcast <em>receive</em> event (see this class's Javadoc): admits one incoming record,
     * delivering it at once if its dependencies are already satisfied, buffering it otherwise.
     *
     * @param message the record to process
     * @return the records to forward downstream, in order
     */
    Outcome<K, V> receive(ParsleyMessage<K, V> message) {
        // A record whose dependencies name a coordinate this node has no channel for at all can never
        // be checked here no matter how long it waits. Fail-closed rather than vacuously satisfied:
        // this node can prove it cannot check the coordinate, never that the coordinate is irrelevant.
        // Checked FIRST, before any state mutation: seedIfFirstSeen persists a seeded frontier and
        // propagate can deliver, persist, and remove records from the buffer, so throwing after them
        // would leave the frontier advanced and the released records (discarded with the unwound `out`)
        // gone from the buffer. Under EOS the whole batch rolls back, but an in-memory engine has no
        // rollback; failing before mutating keeps the persisted state consistent either way. The check
        // reads only the dependency clock and the settled epoch floor, neither of which seedIfFirstSeen
        // affects, so hoisting it changes nothing but the failure's timing.
        if (isUnreachableDependency(message)) {
            throw failUnreachableDependency(message.topic(), message.topicId(), message.partition(), message.offset());
        }

        List<ParsleyMessage<K, V>> out = new ArrayList<>();

        if (frontier.seedIfFirstSeen(message.topicId(), message.partition(), message.offset())) {
            propagate(out, message.topicId(), message.partition());
        }

        // Bridge the offsets a read_committed consumer skipped on this channel below this record — a
        // transaction marker or aborted record no business record will ever fill — so the contiguous walk
        // does not wedge at the hole (ParsleyFrontier.bridge). If it advanced the frontier, cascade: a held
        // record may have been waiting on exactly a bridged offset, and this node's held branch below runs
        // no propagate of its own.
        if (frontier.bridge(message.topicId(), message.partition(), message.offset())) {
            propagate(out, message.topicId(), message.partition());
        }

        ParsleyClock deps = effectiveDependencies(message.dependencies(),
                message.topicId(), message.partition(), message.offset());

        // Every record is checked against this node's own delivered frontier — never against a stamp
        // this same record just supplied, and never against a position a peer merely claims to have
        // delivered (see isDeliverable). This is the classic causal-broadcast receive() predicate:
        // every message is evaluated identically against the receiver's own delivery history.
        if (isDeliverable(message)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            frontier.deliver(message.topicId(), message.partition(), message.offset());
            // The channel-clock fold feeds only the outbound stamp (completeness()) — transitive
            // ancestry a downstream node's own gate will verify for itself — never this node's own
            // delivery gate. It happens only at genuine gated delivery, so the stamp never carries
            // a claim sourced from a record that has not actually been forwarded.
            frontier.channelUpdate(message.topicId(), message.partition(), advertised(message.dependencies()));
            out.add(message);
            propagate(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            candidateIndex.index(seq, deps, frontier.snapshot());
            int depth = buffer.size();
            ParsleyClock frontierNow = frontier.snapshot();
            log.debug("Holding {}-{} @{} (buffer depth: {}, deps: {}, frontier: {})",
                    message.topicId(), message.partition(), message.offset(), depth, deps,
                    frontierNow.retaining((topicId, partition) -> deps.offsetFor(topicId, partition) >= 0));
            metrics.recordBuffered();
            reportBufferState();
        }

        // A normal delivery can advance the frontier past a pending epoch boundary's floor, closing the
        // transition window; a raised floor can then strip a held replay record's below-floor deps.
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out);
        }
        return new Outcome<>(out);
    }

    /**
     * Handles a received epoch-boundary marker: records the marker on its source channel in the
     * {@link ParsleyEpochState}, then closes the transition window if it is now ready (marker on every
     * channel and the delivered frontier dominating the new floor), draining any records the raised
     * floor releases. Mirrors {@link #onWatermark}. The marker itself is never delivered or buffered;
     * the caller ({@link ParsleyProcessor}) relays the marker downstream so the boundary propagates
     * edge by edge — but only when it was {@link BoundaryOutcome#markerWasNew() newly recorded} here, so
     * a cyclic topology cannot ping-pong it.
     *
     * @param boundary        the decoded boundary (epoch id + new lower bounds)
     * @param channelTopicId  the topic UUID of the channel the marker arrived on
     * @param channelPartition the partition of that channel
     * @return the records released by a resulting window close, plus whether the marker was newly recorded
     */
    BoundaryOutcome<K, V> onEpochBoundary(ParsleyEpochBoundary boundary, Uuid channelTopicId, int channelPartition) {
        boolean markerWasNew =
                frontier.recordEpochMarker(boundary.epochId(), boundary.lowerBounds(), channelTopicId, channelPartition);
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out);
        }
        return new BoundaryOutcome<>(new Outcome<>(out), markerWasNew);
    }

    /**
     * Releases every buffered record that passes the causal gate against the current frontier. Used
     * on two call paths:
     * <ol>
     *   <li>Via {@link #drainAfterRestore()} — once, from the 1ms post-init punctuator in
     *       {@link ParsleyProcessor}, to drain records that were satisfied between the last
     *       committed frontier and the last committed buffer-removal (the at-least-once window). On
     *       fresh starts (empty buffer) this returns empty.
     *   <li>After {@link ParsleyFrontier#tryAdvanceEpoch} closes an epoch-transition window — the
     *       raised floor can strip a held record's below-floor dependencies, satisfying it with no
     *       frontier advance for {@link #propagate}'s candidate index to key on.
     * </ol>
     *
     * <p>This is an O(buffer-depth) full scan by design — correctness-first choice; the
     * candidate-index fast path in {@link #propagate} handles the common frontier-advance case.
     *
     * <p>Iterates {@link #orderedIndex()} (index metadata in causal arrival order) and gates each
     * entry on its dependency clock alone — never decoding the user value — so a held, undecodable
     * record that is not releasable is skipped without deserialisation. Only a record that passes the
     * deliverability check is fetched. The {@link ParsleyBufferStore#get(long)} {@code null} guard
     * skips entries already removed by an earlier step in this same pass.
     */
    private void drainSatisfied(List<ParsleyMessage<K, V>> out) {
        for (ParsleyBufferStore.IndexEntry meta : orderedIndex()) {
            // Normally unreachable: receive already rejects an unreachable-dependency record before
            // it is ever buffered. This only catches a record buffered by an older binary version
            // (before this check existed) surviving a restart onto this one.
            if (isUnreachableDependency(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                throw failUnreachableDependency(meta.topic(), meta.topicId(), meta.partition(), meta.offset());
            }
            // The delivery gate, on metadata only: every declared coordinate (self-cycle stripped,
            // out-of-scope already rejected above) is within this node's own contiguous frontier.
            if (!isDeliverable(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                continue;
            }
            ParsleyBufferStore.Entry<K, V> entry;
            try {
                entry = buffer.get(meta.sequence());
            } catch (ParsleyBufferDeserializationException e) {
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
            frontier.channelUpdate(record.topicId(), record.partition(), advertised(record.dependencies()));
            buffer.remove(meta.sequence());
            out.add(record);
            propagate(out, record.topicId(), record.partition());
        }
    }

    /**
     * Delegates to {@link #drainSatisfied}. Called once, via the 1ms post-init punctuator in
     * {@link ParsleyProcessor}, to drain records that were satisfied between the last committed
     * frontier and the last committed buffer-removal.
     *
     * @return the records released
     */
    Outcome<K, V> drainAfterRestore() {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        drainSatisfied(out);
        return new Outcome<>(out);
    }

    /**
     * Handles a received protocol watermark: marks the marker's own position genuinely delivered
     * (unconditionally, exactly like a business record's own coordinate — see below), updates the
     * per-channel clock for the source channel with the carried frontier, and releases any buffered
     * records the advance now permits.
     *
     * <p>The marker's own {@code (sourceTopicId, sourcePartition, offset)} is delivered the same way
     * {@link #receive} delivers a genuine business record's own coordinate — {@link
     * #seedIfFirstSeen} then {@link ParsleyFrontier#deliver} then {@link #propagate} — so a marker-only
     * (passthrough) channel's own frontier still advances even though no business record ever flows on
     * it; without this, such a channel would be stuck at its seed offset forever, and this node's own
     * completeness could never include it.
     *
     * <p><strong>The carried clock never releases anything by itself.</strong> It is folded into the
     * channel clock for the outbound stamp only — a peer's claim that a coordinate was delivered
     * <em>there</em> is not proof it was delivered <em>here</em>, and the delivery gate
     * ({@link #isDeliverable}) checks this node's own contiguous frontier exclusively. Releases on this
     * path come only from the marker's own offset advancing its channel's frontier ({@link #propagate})
     * or from a resulting epoch-window close. Gating on the max-merged completeness here used to let a
     * watermark claiming a sibling channel's coordinate release a held record before this node had
     * itself delivered that cause — an effect-before-cause delivery to the delegate.
     *
     * <p>Records released here are delivered via the normal {@link ParsleyProcessor#deliver} path. The
     * returned {@link WatermarkOutcome#channelAdvanced()} tells the caller ({@link ParsleyProcessor})
     * whether this marker taught the channel anything genuinely new — the caller relays a downstream
     * watermark only when it did, since a marker's own delivery must never itself be treated as a
     * reason to relay further (that would ping-pong forever around any cycle in the topology).
     *
     * <p>Before anything else, {@code frontierClock} has this node's own {@link #ownSinkTopics}
     * coordinates stripped (see that field's Javadoc) — without this, a node whose own produced
     * coordinate is reflected back to it (directly, by consuming its own sink, or indirectly, via a
     * downstream peer's stamp in a topology cycle) would see that coordinate as perpetually "new":
     * receiving it always advances this node's own frontier for that channel by construction (a fresh
     * offset each time), which then shows up in the very next stamp this node emits — so {@code
     * channelAdvanced} would never settle {@code false} and the marker would never stop relaying.
     *
     * @param sourceTopicId  the topic UUID of the watermark's source channel
     * @param sourcePartition the partition of the watermark's source channel
     * @param offset         the watermark record's own offset on its source channel
     * @param frontierClock  the completeness frontier carried by the watermark
     * @return the records released in the process, plus whether the channel's carried clock genuinely
     *         advanced
     */
    WatermarkOutcome<K, V> onWatermark(Uuid sourceTopicId, int sourcePartition, long offset, ParsleyClock frontierClock) {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();
        ParsleyClock strippedFrontierClock = advertised(frontierClock);

        if (frontier.seedIfFirstSeen(sourceTopicId, sourcePartition, offset)) {
            propagate(out, sourceTopicId, sourcePartition);
        }

        // A watermark's own channel is transactional too (Parsley forwards it under EOS), so it carries the
        // same commit-marker holes; bridge them before delivering the marker's own offset. Bridging touches
        // only the frontier and forwarded index, never the channels map, so channelAdvanced below is
        // unaffected by it.
        if (frontier.bridge(sourceTopicId, sourcePartition, offset)) {
            propagate(out, sourceTopicId, sourcePartition);
        }

        ParsleyClock channelBefore = frontier.channelGet(sourceTopicId, sourcePartition);
        boolean channelAdvanced = !channelBefore.dominates(strippedFrontierClock);
        frontier.channelUpdate(sourceTopicId, sourcePartition, strippedFrontierClock);
        frontier.deliver(sourceTopicId, sourcePartition, offset);
        propagate(out, sourceTopicId, sourcePartition);

        // A watermark's own delivery advances the frontier, which can close a pending epoch
        // transition window. Its carried clock, by contrast, feeds only the stamp and the
        // channelAdvanced relay signal — never the gate (see the method Javadoc).
        if (frontier.tryAdvanceEpoch()) {
            drainSatisfied(out);
        }
        return new WatermarkOutcome<>(new Outcome<>(out), channelAdvanced);
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
     * <p><strong>This is the outbound stamp, not the delivery gate.</strong> The merge carries
     * transitive ancestry — coordinates a channel has advertised that this node may not itself have
     * delivered yet — downstream, where each receiver's own gate verifies them against its own
     * delivery history. The gate here ({@link #isDeliverable}) checks the node's own contiguous
     * frontier exclusively: in Birman-Schiper-Stephenson terms the delivery condition is over the
     * receiving process's own delivered vector, and a peer's claim is never a substitute for local
     * delivery of the cause.
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
     * delivered; and if B → C, C follows in the same pass.
     *
     * <p>A candidate whose value cannot be decoded on this forward attempt fails the task fast — the
     * same unconditional failure {@link #drainSatisfied} applies. A candidate is only ever evaluated
     * once per level (the {@code seen} guard).
     */
    private void propagate(List<ParsleyMessage<K, V>> out, Uuid topicId, int partition) {
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
                            failPoison(e);                                     // fail closed
                            throw e;
                        }
                        if (entry == null) {
                            stale.add(candidate);
                            continue;
                        }
                        // Normally unreachable here — receive already rejects an unreachable-dependency
                        // record before it is ever buffered — but a record buffered by an older binary
                        // (before this check existed) can still surface it after a restart.
                        if (isUnreachableDependency(entry.record())) {
                            throw failUnreachableDependency(entry.record().topic(), entry.record().topicId(),
                                    entry.record().partition(), entry.record().offset());
                        }
                        if (!isDeliverable(entry.record())) continue;

                        // See drainSatisfied: persist the frontier/forwarded-index advance before
                        // removing from the buffer, so a torn crash always strands the buffer with an
                        // already-delivered record (a benign duplicate on restart) rather than the
                        // reverse (a permanently stranded frontier).
                        frontier.deliver(entry.record().topicId(), entry.record().partition(), entry.record().offset());
                        frontier.channelUpdate(entry.record().topicId(), entry.record().partition(),
                                advertised(entry.record().dependencies()));
                        buffer.remove(entry.sequence());
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
     * stripped) is within this node's <em>own</em> contiguous delivered frontier. A dependency is
     * satisfied only by this node having itself delivered the cause — never by a channel's advertised
     * claim that some peer delivered it ({@link #completeness()} is the outbound stamp, not this
     * gate). This is the single source of truth for "may this record be delivered now", used on every
     * release path ({@link #receive}, {@link #drainSatisfied}, {@link #propagate}).
     */
    private boolean isDeliverable(ParsleyMessage<K, V> record) {
        return isDeliverable(record.dependencies(), record.topicId(), record.partition(), record.offset());
    }

    /**
     * Metadata overload of the gate: evaluates deliverability from a record's dependency clock and
     * source coordinate alone, without decoding its user value. Used by {@link #drainSatisfied} so a
     * held, undecodable record that is not releasable is never deserialised.
     *
     * <p>The dominance check runs against {@link #effectiveDependencies}, never the raw clock — see
     * that method for the preprocessing steps. Only ever called on a record already proven reachable
     * ({@link #isUnreachableDependency} false) — {@code effectiveDependencies} does not itself filter
     * out-of-scope coordinates, so this would otherwise wait forever on one.
     */
    private boolean isDeliverable(ParsleyClock dependencies, Uuid topicId, int partition, long offset) {
        return frontier.snapshot().dominates(effectiveDependencies(dependencies, topicId, partition, offset));
    }

    /**
     * Returns {@code true} if any coordinate {@code message} depends on ({@link #effectiveDependencies},
     * the same preprocessing {@link #isDeliverable} applies) names a coordinate outside {@link #inScope}
     * — this node has no input channel for it at all, so it can never be confirmed here no matter how
     * long it waits.
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
     * node has no channel for. Records the failure via {@link #metrics} first, mirroring
     * {@link #failPoison}, then returns the exception for the caller to throw — never buffering the
     * record and never treating the unreachable coordinate as satisfied.
     */
    private ParsleyUnreachableDependencyException failUnreachableDependency(
            String topic, Uuid topicId, int partition, long offset) {
        metrics.recordUnreachableDependencyError();
        log.error("{}-{} @{} depends on a coordinate this node has no channel for; failing fast "
                + "(fail-closed). The record was not forwarded and is reprocessed on restart.",
                topic, partition, offset);
        return new ParsleyUnreachableDependencyException(topic, topicId, partition, offset);
    }

    /**
     * The dependency clock actually checked by the gate: three view-only preprocessing steps, none of
     * which ever rewrites recorded state or the outbound stamp (which is completeness(), computed
     * separately from the channel clocks {@link #advertised} feeds).
     * <ol>
     *   <li>The record's exact self-cycle is removed ({@link #withoutSelfReference}) — a record
     *       depending on its own {@code (topicId, partition, offset)} has, by being delivered, met that
     *       dependency, so it must not wait on itself (this keeps a self-referential stamp on a fused
     *       chain from deadlocking). A backward same-partition dependency ({@code req < offset}) is an
     *       intra-topic dependency like any other and flows through unchanged.</li>
     *   <li>Any coordinate belonging to a topic this node itself produces ({@link #ownSinkTopics}) is
     *       stripped — see that field's Javadoc for why this is sound and different from the
     *       out-of-scope case below, not a relaxation of it.</li>
     *   <li>Any dependency below its coordinate's topology-epoch {@code startsAt} bound is stripped
     *       ({@link ParsleyClock#strippedBelow}) — an out-of-domain reference to a prior, closed epoch
     *       that no channel in this epoch will ever confirm. A no-op with epoch bounding disabled.</li>
     * </ol>
     * <p>A coordinate outside {@link #inScope} is <strong>not</strong> dropped here — this node cannot
     * prove such a coordinate is safe to disregard, only that it cannot check it, so it is never
     * silently treated as satisfied. {@link #isUnreachableDependency} checks for one before this result
     * is ever handed to {@link #isDeliverable}, and fails the task fast instead.
     */
    private ParsleyClock effectiveDependencies(ParsleyClock deps, Uuid topicId, int partition, long offset) {
        return withoutSelfReference(deps, topicId, partition, offset)
                .retaining((depTopicId, depPartition) -> !ownSinkTopics.test(depTopicId, depPartition))
                .strippedBelow(frontier.epoch());
    }

    /**
     * The clock folded into a channel's advertised view (feeding the outbound stamp): the raw clock
     * with this node's own produced coordinates stripped — see {@link #ownSinkTopics}. Own-sink
     * coordinates only ever appear in an inbound clock by reflection around a topology cycle (a
     * producer cannot know its own output's offsets at stamp time), so re-advertising them would only
     * echo this node's own position back at itself.
     */
    private ParsleyClock advertised(ParsleyClock clock) {
        return clock.retaining((depTopicId, depPartition) -> !ownSinkTopics.test(depTopicId, depPartition));
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
     * the task fails, so it can be recovered once the schema is fixed or rolled back.
     */
    private void failPoison(ParsleyBufferDeserializationException e) {
        metrics.recordDeserializationError();
        log.error("Buffered record could not be deserialised; failing fast (fail-closed). "
                + "It remains in the buffer changelog for recovery. {}", e.details(), e);
    }

}
