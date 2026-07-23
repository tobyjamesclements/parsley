package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.processor.api.Record;
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
 * <strong>L2 — causal broadcast: the CBCAST receive/deliver core.</strong> The causal delivery
 * algorithm of Birman–Schiper–Stephenson (ISIS CBCAST), over the reliable FIFO channels
 * {@link ParsleyChannels} adapts Kafka topic-partitions into. Presented in the CGR module style
 * ({@code package-info} states the two Parsley-wide deviations from the textbook presentation —
 * pulled indications, and the sender's clock increment performed by the broker's offset assignment,
 * which is why {@link #broadcast} attaches a stamp but cannot include the record's own coordinate):
 *
 * <pre>
 * requests:   broadcast(record) → stamped record       attach the outbound vector timestamp
 *                                                      completeness ∪ ownOutputs (D2; the BSS
 *                                                      timestamp-assignment half — the underlying
 *                                                      point-to-point send is Kafka's produce),
 *                                                      after the crossing wait and the ack fold
 *             receive(message) → Outcome               the BSS receive: gate → deliver-or-hold →
 *                                                      cascade; the returned ordered list IS the
 *                                                      deliver() indication
 * queries:    completeness() → ParsleyVectorClock      the delivered/advertised boundary (the stamp
 *                                                      is ParsleyChannels.stamp(): this ∪ ownOutputs)
 *             frontier() → ParsleyVectorClock          the delivered vector VT(p)
 * properties: I1 (causal delivery — a record reaches the delegate only after every dependency has
 *             been locally, contiguously delivered; no timeout, no eviction), I2 (stamp transitive
 *             completeness — the stamp dominates the dependency clocks of every delivered event),
 *             I9 (unconditional merge, stamp-side)
 * </pre>
 *
 * <p><em>Receive</em> is {@link #receive}: a record arrives on an input channel and is either delivered
 * at once or buffered until it can be. <em>Deliver</em> is {@link ParsleyChannels#delivered} — where the
 * causal-order guarantee is actually granted, surfaced here via {@link Outcome} and handed to the user's
 * delegate by {@code ParsleyProcessor#deliver}. <em>Broadcast</em> is {@link #broadcast}: the single
 * stamping site every outbound record — a delegate's business forward and a protocol marker alike —
 * passes through; the send itself is Kafka's own {@code ProducerRecord}/{@code context.forward()} (a
 * partition's total order and replication are already a reliable-broadcast substrate). The L3
 * gossip module ({@link ParsleyGossip}) sits on top of this algorithm as a liveness layer — a
 * protocol extension, not part of the CBCAST core — through the package-private seam
 * {@link #propagate} / {@link #recordReflectedClaims} / {@link #channels()}.
 *
 * <p>The processor feeds incoming records to {@link #receive} and forwards the returned records
 * downstream, in order. The delivery gate is the two-branch dispatch of D1, evaluated per
 * dependency coordinate: a dependency on a coordinate this node <em>consumes</em> (an input
 * channel of this task, on the partition this task owns) must be satisfied by this node's own
 * contiguous frontier — the positions this node has itself delivered, never a position a peer
 * merely claims to have delivered (see {@link #isDeliverable}); a dependency on any other
 * coordinate is <em>ignored</em>, unconditionally — counted by the out-of-scope-ignored metric,
 * never a failure. The ignore branch is sound by the transitivity theorem (D1/D7): with
 * transitively complete stamps (I2) carried by unconditional merges (I9), any consumed causal
 * ancestor of a record is claimed directly in that record's own clock, so an unconsumed entry
 * only ever proxies ancestry the clock already states explicitly — ignoring it loses no ordering
 * observable at this node. On the consumed branch there is no eviction, no buffer limit, and no
 * timeout — a record whose dependencies are not yet satisfied stays buffered (the buffer is a
 * changelog-backed state store, so it spills to disk rather than growing in memory). A record
 * that can be proven impossible to ever evaluate — an undecodable payload or dependency header —
 * unconditionally fails the task fast, rather than being forwarded downstream as if it were
 * causally valid or silently discarded. There is no diversion sink: this is the single, simple
 * failure model — any error is a hard stop, never a partial or best-effort continuation.
 *
 * <p><strong>The frontier is a contiguous high-water mark, not a running max.</strong> This class does
 * not head-of-line block: a later-offset record on a partition may forward before an earlier one
 * still held on the same partition. So a coordinate's frontier offset must only ever advance once
 * every offset up to it has actually been forwarded — never past a gap, or a record elsewhere
 * depending on exactly the gapped offset would be released on bookkeeping alone, never having
 * actually observed it. {@link ParsleyChannels#delivered} marks every forward in a {@link
 * ParsleyForwardedIndex} and walks forward from the current frontier to find the longest run of
 * consecutive forwarded offsets now achievable, advancing the frontier by that run and pruning the
 * absorbed entries. Every release calls it the same way — none are special-cased.
 *
 * <p><strong>A coordinate's first offset need not be 0.</strong> Kafka delivers a partition's
 * records strictly in increasing offset order, but retention, compaction, or a fresh consumer group
 * can all mean the first offset this node ever observes for a coordinate is well past 0. {@link
 * ParsleyChannels#seedIfFirstSeen} folds everything below the first-ever-observed offset into the
 * frontier the moment it's seen, so it is treated as outside this module's purview rather than an
 * unfillable hole — without that, the contiguous walk above could never advance past {@code -1} for
 * such a coordinate.
 *
 * <p><strong>Frontier persistence ordering:</strong> {@link ParsleyChannels} self-persists its
 * single {@code "f"} value on every advance — inside {@link ParsleyChannels#delivered} and
 * {@link ParsleyChannels#seedIfFirstSeen}, before control returns to this class and the record is
 * added to the out-bound list — so the frontier is persisted before the record leaves this class.
 * For the same reason, every release path calls {@link ParsleyChannels#delivered} (and {@link
 * ParsleyChannels#channelUpdate}) <em>before</em> removing the record from {@link #buffer}: the
 * buffer and the frontier/forwarded-index are separate changelog topics with no cross-store
 * atomicity, so a crash between the two writes must always tear toward "buffer still holds a record
 * the frontier already delivered" (harmless — {@link #drainAfterRestore} redelivers it as an
 * at-least-once duplicate) and never the reverse, which would strand that coordinate's frontier
 * permanently.
 *
 * <p><strong>Drain algorithm:</strong> this class uses a {@link ParsleyCandidateIndex} to avoid a full
 * buffer scan on every frontier advance. When a coordinate advances, only records indexed on
 * that coordinate are checked for causal satisfaction. The cascade repeats for each newly
 * released record's source coordinate. A record is only ever released once this check — against
 * the real, contiguous frontier — passes; extending the frontier and checking for release are
 * always two separate steps, never one.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleyCausalBroadcast<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ParsleyCausalBroadcast.class);

    private final ParsleyBufferStore<K, V> buffer;
    private final ParsleyCandidateIndex candidateIndex;
    private final ParsleyMetrics metrics;
    private final LongSupplier clock;
    // The stalled-dependency count last reported by reportHeldDependencyStalls (A9), so the WARN log
    // fires on change rather than repeating every tick while a stall persists.
    private int lastReportedStalls;

    // The single owner of all persisted causal metadata: the contiguous frontier clock, the channel
    // clocks, and the forwarded-offset index. completeness() and channel state live here;
    // channels.normalize strips the self-cycle from every inbound dependency clock before the gate
    // sees it (I5).
    private final ParsleyChannels channels;

    // The consumed(c) predicate of the two-branch gate (D1): a registered input channel of this
    // task, on the partition this task owns. A dependency on a consumed coordinate must be
    // satisfied by this node's own contiguous frontier (local delivery, never hearsay); a
    // dependency on any other coordinate falls to the gate's IGNORE branch — counted by the
    // out-of-scope-ignored metric, never a failure (the I7 fail-fast is retired, D7). The ignore
    // is unconditional and sound by the transitivity theorem: I2 + I9 guarantee any consumed
    // causal ancestor of a record is claimed directly in that record's own clock, so an unconsumed
    // entry only ever proxies ancestry the clock already states. Defaults to "everything is
    // consumed" so existing callers/tests that never construct with an explicit predicate are
    // unaffected; ParsleyProcessor passes its real per-task predicate.
    private final ParsleyVectorClock.CoordinatePredicate consumed;

    // Coordinates for a topic THIS NODE ITSELF produces (a registered sink). Feeds only the I8
    // reflected-claim diagnostic (recordReflectedClaims): an inbound claim on an own-sink
    // coordinate ABOVE the ownOutputs clock means the own-output view is stale or a peer's stamp
    // untruthful — worth seeing, never worth failing over. The gate treats a reflected claim like
    // any other dependency (consumed → gated on local delivery; unconsumed → ignored): the
    // historical gate-side strip this predicate used to drive died at T3.1 with the two-branch
    // dispatch — its vacuous satisfaction of claims about OTHER producers' records on a shared
    // sink topic was finding (iii) — and the stamp-side strip died at T2.3 (#22); relay settling
    // rests on the I6 knowledge-based rule (ParsleyGossip), where a reflected own claim is
    // dominated by ownOutputs and so teaches nothing. Defaults to "nothing is ever this node's
    // own sink"; ParsleyProcessor passes its real per-stage sink-topic predicate.
    private final ParsleyVectorClock.CoordinatePredicate ownSinkTopics;

    /**
     * The one constructor. Takes a pre-built {@link ParsleyChannels} — the single owner of the
     * frontier clock, channel clocks, and forwarded index — so callers control its persistence
     * (the task's changelog-backed store in production, an in-memory store double in tests; the
     * test fixture factory provides the defaulted convenience shapes tests use).
     *
     * <p><strong>Held-record disposition.</strong> Every restored held record is classified by its
     * source coordinate before anything is seeded or indexed:
     * <ul>
     *   <li><strong>Consumed</strong> (a current input on this task's partition) — restored
     *       unchanged: seed replay, then candidate re-index.</li>
     *   <li><strong>Destroyed</strong> (its source topicId is in {@code destroyedSources} — a
     *       recreated input's old incarnation) — purged from the buffer, with an INFO log carrying
     *       the count and coordinates. E1's precedent: a destroyed UUID's entries are the one
     *       removal I9 permits from stamp-feeding state; the incarnation that produced these
     *       records is deleted, and delivering them would both forward a record from a dead
     *       incarnation and re-enter its destroyed coordinate into the frontier, which rescope
     *       just purged. Purging keeps recreation-across-a-restart self-healing: history loss,
     *       never reordering.</li>
     *   <li><strong>Out of scope but alive</strong> (a removed input's records) — init fails
     *       loudly, naming the topics and per-record counts. Silently dropping them would violate
     *       the fail-closed contract; delivering them is impossible (the removed topic has no
     *       registered serde) and undesirable (the operator removed the input). The remedies are
     *       in the message: redeclare the input so the records drain through ordinary delivery, or
     *       perform a full reset.</li>
     * </ul>
     *
     * @param consumed         the consumed(c) predicate of the two-branch gate (D1): a registered
     *                         input channel of this task, on the partition this task owns. A
     *                         dependency on a consumed coordinate is gated on this node's own
     *                         contiguous frontier; a dependency on any other coordinate is ignored,
     *                         with a metric — see this field's own Javadoc.
     * @param ownSinkTopics    coordinates for a topic this node itself produces. Feeds only the I8
     *                         reflected-claim diagnostic — never the gate, the channel folds, or the
     *                         stamp; see this field's own Javadoc.
     * @param destroyedSources the old topic UUIDs of inputs recreated across the restart (same
     *                         name, new UUID — the destroyed incarnations whose channel state
     *                         {@link ParsleyChannels#rescope} purges); drives the held-record
     *                         disposition above
     */
    ParsleyCausalBroadcast(ParsleyChannels channels,
                 ParsleyBufferStore<K, V> buffer,
                 ParsleyCandidateIndex candidateIndex,
                 ParsleyMetrics metrics,
                 LongSupplier clock,
                 ParsleyVectorClock.CoordinatePredicate consumed,
                 ParsleyVectorClock.CoordinatePredicate ownSinkTopics,
                 Set<Uuid> destroyedSources) {
        this.channels = channels;
        this.buffer = buffer;
        this.candidateIndex = candidateIndex;
        this.metrics = metrics;
        this.clock = clock;
        this.consumed = consumed;
        this.ownSinkTopics = ownSinkTopics;
        // Held-record disposition (see the constructor Javadoc): destroyed incarnations are purged,
        // a removed-but-alive input's records fail init, and only consumed records go on to the
        // seed replay and candidate re-index below.
        List<ParsleyBufferStore.IndexEntry> kept = new ArrayList<>();
        List<String> purgedCoordinates = new ArrayList<>();
        Map<String, Integer> outOfScope = new java.util.TreeMap<>();
        for (ParsleyBufferStore.IndexEntry entry : buffer.indexEntries()) {
            if (destroyedSources.contains(entry.topicId())) {
                buffer.remove(entry.sequence());
                purgedCoordinates.add(entry.topic() + "-" + entry.partition() + "@" + entry.offset());
            } else if (consumed.test(entry.topicId(), entry.partition())) {
                kept.add(entry);
            } else {
                outOfScope.merge(entry.topic(), 1, Integer::sum);
            }
        }
        if (!outOfScope.isEmpty()) {
            throw new IllegalStateException("the restored buffer holds " + outOfScope.values().stream()
                    .mapToInt(Integer::intValue).sum() + " record(s) from input topic(s) no longer "
                    + "declared (records per topic: " + outOfScope + "). They can neither be "
                    + "delivered (no registered source) nor silently discarded (fail-closed). "
                    + "Either redeclare the input topic(s) so the held records drain through "
                    + "ordinary causal delivery (if a topic was also deleted, redeclaring its "
                    + "recreated successor purges the held records as destroyed history instead), "
                    + "or perform a full application reset.");
        }
        if (!purgedCoordinates.isEmpty()) {
            log.info("Purged {} held record(s) produced by destroyed source incarnation(s) — the "
                    + "recreated input's old records are deleted history, never delivered (E1): {}",
                    purgedCoordinates.size(), purgedCoordinates);
        }
        // Replay receive()'s first-sighting seed for every restored held record's source coordinate,
        // at its lowest held offset, BEFORE anything else can. ParsleyChannels's "seen" guard is
        // in-memory and does not survive a restart, so without this a post-restart record arriving on
        // a coordinate whose only prior activity is a still-held record (frontier entry absent —
        // possible only when that record sits at offset 0, whose seed is a no-op) would re-trigger
        // the baseline seed and fold the held record's offset into the frontier as "outside the
        // module's purview" — releasing records that depend on it before it has ever been delivered
        // (an effect-before-cause delivery). Seeding at the lowest held offset reproduces exactly
        // what the first in-run sighting did: marks the coordinate seen, and never seeds past a held
        // record. Only consumed coordinates remain after the disposition above, so every kept
        // record's coordinate seeds.
        Map<Uuid, Map<Integer, Long>> lowestHeld = new HashMap<>();
        for (ParsleyBufferStore.IndexEntry entry : kept) {
            lowestHeld.computeIfAbsent(entry.topicId(), k -> new HashMap<>())
                    .merge(entry.partition(), entry.offset(), Math::min);
        }
        lowestHeld.forEach((topicId, byPartition) -> byPartition.forEach((partition, offset) ->
                channels.seedIfFirstSeen(topicId, partition, offset)));
        // Populate the candidate index for any records already in the buffer (e.g., restored from
        // a state store after a restart). This is a one-time O(n) pass at construction. It decodes
        // only the dependency clock (never the user-serde key/value), so a record whose value can no
        // longer be deserialised — e.g. an incompatible Schema Registry change while buffered — does
        // not block startup; that failure surfaces later, on the forward path. Indexed against the
        // contiguous frontier — the same clock the delivery gate checks — never completeness: a
        // coordinate a channel merely claims (satisfied by completeness but not by the frontier)
        // must stay indexed, or the frontier advance that eventually genuinely proves it would find
        // no candidate to release.
        for (ParsleyBufferStore.IndexEntry entry : kept) {
            candidateIndex.index(entry.sequence(),
                    consumedDependencies(entry.dependencies(), entry.topicId(), entry.partition(), entry.offset()),
                    channels.frontier());
        }
    }

    /**
     * The per-call receive result: every record released for delivery, in order.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record Outcome<K, V>(List<ParsleyMessage<K, V>> delivered) {
    }

    /**
     * The causal broadcast <em>receive</em> event (see this class's Javadoc): admits one incoming record,
     * delivering it at once if its dependencies are already satisfied, buffering it otherwise.
     *
     * @param message the record to process
     * @return the records to forward downstream, in order
     */
    Outcome<K, V> receive(ParsleyMessage<K, V> message) {
        // Replay skip guard: a record this node has already delivered (at or below the contiguous
        // frontier, or still marked in the forwarded index) must not reach the delegate a second
        // time. Routine while an added input's re-fetched prefix replays past the carried-ancestry
        // seed (ParsleyChannels#rescope — "skip what you already ignored", T3.0 A5); otherwise the
        // fail-safe net for a redelivery exactly_once_v2 should make impossible (A11). Checked
        // first: an already-delivered record must skip before any of the ordinary receive
        // bookkeeping below re-evaluates it. The L1 receive bookkeeping still runs (it keeps
        // highestReceived exact for bridge()'s skip detection; on an already-delivered offset it
        // cannot advance the frontier past anything undelivered).
        if (channels.alreadyDelivered(message.topicId(), message.partition(), message.offset())) {
            List<ParsleyMessage<K, V>> out = new ArrayList<>();
            if (channels.receive(message.topicId(), message.partition(), message.offset())) {
                propagate(out, message.topicId(), message.partition());
            }
            metrics.recordReplaySkipped();
            log.debug("Skipping {}-{} @{} (already delivered; replay)",
                    message.topic(), message.partition(), message.offset());
            return new Outcome<>(out);
        }

        recordReflectedClaims(message.dependencies());

        List<ParsleyMessage<K, V>> out = new ArrayList<>();

        // The L1 receive request: seed the channel's density baseline on first sighting, and bridge the
        // offsets a read_committed consumer skipped below this record — a transaction marker or aborted
        // record no business record will ever fill — so the contiguous walk does not wedge at the hole
        // (ParsleyChannels.receive). If either advanced the frontier, cascade: a held record may have
        // been waiting on exactly a seeded or bridged offset, and this node's held branch below runs no
        // propagate of its own.
        if (channels.receive(message.topicId(), message.partition(), message.offset())) {
            propagate(out, message.topicId(), message.partition());
        }

        // The gate's ignore branch (D1), counted once per received record: every normalised
        // dependency coordinate this node does not consume is ignored — sound by I2 + I9 (a
        // consumed causal ancestor is always claimed directly in this same clock, so the entry is
        // a proxy for ancestry the clock already states) — and surfaces only as a metric, never a
        // failure (the I7 fail-fast is retired, D7).
        ParsleyVectorClock normalized = channels.normalize(message.dependencies(),
                message.topicId(), message.partition(), message.offset());
        ParsleyVectorClock deps = normalized.retaining(consumed);
        int ignored = normalized.size() - deps.size();
        if (ignored > 0) {
            metrics.recordOutOfScopeIgnored(ignored);
            log.debug("Ignoring {} out-of-scope dependency coordinate(s) on {}-{} @{} — consumed "
                    + "ancestry is claimed directly by the same clock (I2/I9)",
                    ignored, message.topic(), message.partition(), message.offset());
        }

        // Every record is checked against this node's own delivered frontier — never against a stamp
        // this same record just supplied, and never against a position a peer merely claims to have
        // delivered (see isDeliverable). This is the classic causal-broadcast receive() predicate:
        // every message is evaluated identically against the receiver's own delivery history.
        if (isDeliverable(message)) {
            log.debug("Forwarding {}-{} @{} (satisfied immediately)",
                    message.topic(), message.partition(), message.offset());
            channels.delivered(message.topicId(), message.partition(), message.offset());
            // The channel-clock fold feeds only the outbound stamp (completeness()) — transitive
            // ancestry a downstream node's own gate will verify for itself — never this node's own
            // delivery gate. It happens only at genuine gated delivery, so the stamp never carries
            // a claim sourced from a record that has not actually been forwarded. The WHOLE clock is
            // folded — own-sink coordinates included (I9: the merge may not strip; the old stamp-side
            // strip erased real ancestors for third parties sharing the sink, #22).
            channels.channelUpdate(message.topicId(), message.partition(), message.dependencies());
            out.add(message);
            propagate(out, message.topicId(), message.partition());
        } else {
            long seq = buffer.add(message, clock.getAsLong());
            candidateIndex.index(seq, deps, channels.frontier());
            int depth = buffer.size();
            ParsleyVectorClock frontierNow = channels.frontier();
            log.debug("Holding {}-{} @{} (buffer depth: {}, deps: {}, frontier: {})",
                    message.topicId(), message.partition(), message.offset(), depth, deps,
                    frontierNow.retaining((topicId, partition) -> deps.offsetFor(topicId, partition) >= 0));
            metrics.recordBuffered();
            reportBufferState();
        }

        return new Outcome<>(out);
    }

    /**
     * Releases every buffered record that passes the causal gate against the current frontier. Called
     * via {@link #drainAfterRestore()} — once, from the 1ms post-init punctuator in
     * {@link ParsleyProcessor}, to drain records that were satisfied between the last
     * committed frontier and the last committed buffer-removal (the at-least-once window). On
     * fresh starts (empty buffer) this returns empty.
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
            // The delivery gate, on metadata only: every consumed declared coordinate (self-cycle
            // stripped, unconsumed coordinates ignored per D1) is within this node's own
            // contiguous frontier.
            if (!isDeliverable(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset())) {
                continue;
            }
            ParsleyBufferStore.Entry<K, V> entry;
            try {
                entry = buffer.get(meta.sequence());
            } catch (CausalBufferDeserializationException e) {
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
            channels.delivered(record.topicId(), record.partition(), record.offset());
            channels.channelUpdate(record.topicId(), record.partition(), record.dependencies());
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
     * The L1 module this core runs over — the L3 seam: {@link ParsleyGossip} folds a received null
     * message's offset and carried clock into the same channel state this core gates and stamps
     * from, and takes the I6 comparison against {@link ParsleyChannels#stamp()}.
     */
    ParsleyChannels channels() {
        return channels;
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
    ParsleyVectorClock frontier() {
        return channels.frontier();
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
     * computation lives on {@link ParsleyChannels}, which owns both the frontier clock and the channel
     * clocks; this method delegates.
     *
     * @return the completeness clock; never {@code null}
     */
    ParsleyVectorClock completeness() {
        return channels.completeness();
    }

    /**
     * The causal broadcast <em>broadcast</em> request over an unknowable destination — the form every
     * business forward takes ({@link ParsleyProcessorContext#forward}): the sink's partitioner runs
     * downstream of {@code forward()}, so the record's destination coordinate cannot be named at
     * stamp time and the crossing wait conservatively excludes nothing (full quiescence). Over-waiting
     * only ever folds <em>more</em> acked positions into the stamp — monotone-sound (I8) — where a
     * mispredicted same-partition exemption would silently under-claim. See
     * {@link #broadcast(Record, Set)}.
     *
     * @param record the outbound record to stamp; its headers are not mutated (a fresh header set is
     *               built and applied via {@link Record#withHeaders})
     * @param <KOut> the outbound key type
     * @param <VOut> the outbound value type
     * @return the same record with the stamp header attached
     */
    <KOut, VOut> Record<KOut, VOut> broadcast(Record<KOut, VOut> record) {
        return broadcast(record, Set.of());
    }

    /**
     * The causal broadcast <em>broadcast</em> request (see the class Javadoc's module box): stamps
     * {@code record} with this node's current outbound vector timestamp —
     * {@code completeness ∪ ownOutputs} ({@link ParsleyChannels#stamp()}, D2), read live at stamp
     * time — replacing any {@link ParsleyHeader#CAUSAL_CLOCK} header already present. The
     * send itself is not performed here (it is Kafka's produce, via {@code context.forward()}); in
     * Birman–Schiper–Stephenson terms this is the timestamp-assignment half of {@code broadcast(m)},
     * and the sender's own clock increment is the broker's offset assignment, learned only
     * asynchronously — which is why the stamp cannot include the record's own coordinate
     * ({@code package-info} states this Parsley-wide deviation once).
     *
     * <p>Two steps precede every stamp, in order, both structural to this single site:
     * <ol>
     *   <li><strong>The crossing wait</strong> ({@link ParsleyChannels#awaitOwnOutputQuiescence} —
     *       O1, per-partition granularity per T3.0 A7): block until no own-sink send outside
     *       {@code destinations} is unacknowledged, so a send that process-order-precedes this
     *       record cannot be missing from the clock the stamp carries. On timeout or ack failure
     *       the wait throws and the EOS transaction dies — never stamp-and-proceed (A8). Uniform
     *       across business forwards and null messages (O4; the recorded null-message exemption is
     *       deliberately not taken).</li>
     *   <li><strong>The ack fold</strong> ({@link ParsleyChannels#foldAcknowledgedOutputs} —
     *       "folded before each stamp", D2/O1): drain the producer-ack registry into the
     *       {@code ownOutputs} clock the stamp merges.</li>
     * </ol>
     *
     * <p>This is the <strong>single stamping site</strong> for every outbound record: a delegate's
     * business forwards ({@link ParsleyProcessorContext#forward}) and {@code ParsleyProcessor}'s
     * protocol markers both route through here, so the stamp's content cannot diverge between the
     * two paths by construction.
     *
     * @param record       the outbound record to stamp; its headers are not mutated (a fresh header
     *                     set is built and applied via {@link Record#withHeaders})
     * @param destinations the destination coordinates this stamped record will be sent to, excluded
     *                     from the crossing wait (a pending send to the record's own destination is
     *                     covered by partition FIFO plus I3, O1; for a marker fanned out to several
     *                     sinks the whole set is excludable under O4's recorded null-message
     *                     exemption). Empty when the destination is unknowable — the conservative
     *                     full-quiescence wait
     * @param <KOut>       the outbound key type
     * @param <VOut>       the outbound value type
     * @return the same record with the stamp header attached
     */
    <KOut, VOut> Record<KOut, VOut> broadcast(Record<KOut, VOut> record, Set<TopicPartition> destinations) {
        channels.awaitOwnOutputQuiescence(destinations);
        channels.foldAcknowledgedOutputs();
        return record.withHeaders(
                ParsleyHeader.replacingClock(record.headers(), channels.stamp().toBytes()));
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
     *
     * <p>Package-private for L3 ({@link ParsleyGossip}), which delivers a null message's own offset
     * into its channel's frontier and cascades the releases through this same path.
     */
    void propagate(List<ParsleyMessage<K, V>> out, Uuid topicId, int partition) {
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
                    long coordOffset = channels.frontier().offsetFor(coordTopicId, coordPartition);
                    for (ParsleyCandidateIndex.Candidate candidate : candidateIndex.findCandidates(coordTopicId, coordPartition, coordOffset)) {
                        if (!seen.add(candidate.recordId())) continue;
                        ParsleyBufferStore.Entry<K, V> entry;
                        try {
                            entry = buffer.get(candidate.recordId());
                        } catch (CausalBufferDeserializationException e) {
                            failPoison(e);                                     // fail closed
                            throw e;
                        }
                        if (entry == null) {
                            stale.add(candidate);
                            continue;
                        }
                        if (!isDeliverable(entry.record())) continue;

                        // See drainSatisfied: persist the frontier/forwarded-index advance before
                        // removing from the buffer, so a torn crash always strands the buffer with an
                        // already-delivered record (a benign duplicate on restart) rather than the
                        // reverse (a permanently stranded frontier).
                        channels.delivered(entry.record().topicId(), entry.record().partition(), entry.record().offset());
                        channels.channelUpdate(entry.record().topicId(), entry.record().partition(),
                                entry.record().dependencies());
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
     * The causal delivery gate — the two-branch dispatch of D1: every coordinate {@code record}
     * depends on is either <em>consumed</em> here (an input channel of this task, on the partition
     * this task owns), in which case it must be within this node's <em>own</em> contiguous
     * delivered frontier, or it is not, in which case it is ignored
     * ({@link #consumedDependencies}). A consumed dependency is satisfied only by this node having
     * itself delivered the cause — never by a channel's advertised claim that some peer delivered
     * it ({@link #completeness()} is the outbound stamp, not this gate). This is the single source
     * of truth for "may this record be delivered now", used on every release path
     * ({@link #receive}, {@link #drainSatisfied}, {@link #propagate}).
     */
    private boolean isDeliverable(ParsleyMessage<K, V> record) {
        return isDeliverable(record.dependencies(), record.topicId(), record.partition(), record.offset());
    }

    /**
     * Metadata overload of the gate: evaluates deliverability from a record's dependency clock and
     * source coordinate alone, without decoding its user value. Used by {@link #drainSatisfied} so a
     * held, undecodable record that is not releasable is never deserialised.
     *
     * <p>The dominance check runs against {@link #consumedDependencies}, never the raw clock — see
     * that method for the two-branch dispatch it implements.
     */
    private boolean isDeliverable(ParsleyVectorClock dependencies, Uuid topicId, int partition, long offset) {
        return channels.frontier().dominates(consumedDependencies(dependencies, topicId, partition, offset));
    }

    /**
     * The dependency clock actually checked by the gate — the two-branch dispatch of D1 in one
     * expression: L1's normalisation ({@link ParsleyChannels#normalize} — self-cycle removal, a
     * pure function; I5), then restriction to the coordinates
     * this node consumes ({@link #consumed}). Every other coordinate falls to the ignore branch:
     * unconditionally ignored — sound by the transitivity theorem (I2 + I9: a consumed causal
     * ancestor is always claimed directly in the record's own clock, so an unconsumed entry only
     * ever proxies ancestry the clock already states) — and counted once per received record via
     * the out-of-scope-ignored metric ({@link #receive}), never a failure (the I7 fail-fast is
     * retired, D7). Neither step ever rewrites recorded state or the outbound stamp (the stamp is
     * {@link ParsleyChannels#stamp()}, computed from the unstripped channel folds — I9: the gate
     * may ignore; the merge may not).
     */
    private ParsleyVectorClock consumedDependencies(ParsleyVectorClock deps, Uuid topicId, int partition, long offset) {
        return channels.normalize(deps, topicId, partition, offset).retaining(consumed);
    }

    /**
     * The I8 diagnostic: counts an inbound clock that claims one of this node's own sink coordinates
     * <em>above</em> the {@code ownOutputs} clock. A truthful reflected claim can only ever name a
     * position this node itself produced, so it should sit at or below {@code ownOutputs} once the
     * pending acks are folded; a claim above it means the own-output view is stale (a torn restore
     * beyond what the end-offset seed healed) or a peer's clock is not truthful — worth seeing,
     * never worth failing over (O3 dissolved: the gate treats the claim like any other, and a stale
     * view only ever delays). Checked once per inbound record ({@link #receive}) and once per null
     * message ({@link ParsleyGossip#receive} — the package-private access); the cheap first pass
     * avoids the ack fold unless a candidate is actually above the current view.
     */
    void recordReflectedClaims(ParsleyVectorClock clock) {
        if (!reflectsAboveOwnOutputs(clock)) {
            return;
        }
        // Re-check with pending acks folded: an ack simply not yet drained since the last stamp is
        // not a genuine above-own-outputs reflection.
        channels.foldAcknowledgedOutputs();
        if (reflectsAboveOwnOutputs(clock)) {
            metrics.recordReflectedClaimAboveOwnOutputs();
        }
    }

    private boolean reflectsAboveOwnOutputs(ParsleyVectorClock clock) {
        ParsleyVectorClock ownOutputs = channels.ownOutputs();
        boolean[] above = {false};
        clock.forEach((topicId, partition, offset) -> {
            if (!above[0] && ownSinkTopics.test(topicId, partition)
                    && offset > ownOutputs.offsetFor(topicId, partition)) {
                above[0] = true;
            }
        });
        return above[0];
    }

    /**
     * The stalled-dependency observability scan (T3.0 A9): counts the held records waiting, for
     * longer than {@code thresholdMs}, on a consumed-channel dependency <em>above</em> that
     * channel's highest physically received offset — the exact signature of "nothing received so
     * far can satisfy this claim". Such a hold is fail-safe, never unsafe, but its delay is
     * unbounded when the claimed channel goes permanently silent (an aborted tail whose producer
     * died; keying that never revisits the partition), so it must be visible rather than
     * indistinguishable from ordinary buffering. Reported as a gauge on every call and logged when
     * the count changes; called from the owning processor's periodic metrics tick, never the hot
     * delivery path (the scan is O(buffer × deps)).
     */
    void reportHeldDependencyStalls(long thresholdMs) {
        long now = clock.getAsLong();
        int stalled = 0;
        String exemplar = null;
        for (ParsleyBufferStore.IndexEntry meta : buffer.indexEntries()) {
            if (now - meta.bufferedAt() < thresholdMs) {
                continue;
            }
            ParsleyVectorClock deps =
                    consumedDependencies(meta.dependencies(), meta.topicId(), meta.partition(), meta.offset());
            boolean[] above = {false};
            String[] cause = {""};
            deps.forEach((depTopicId, depPartition, required) -> {
                if (!above[0] && required > channels.highestReceived(depTopicId, depPartition)) {
                    above[0] = true;
                    cause[0] = depTopicId + "-" + depPartition + " @" + required;
                }
            });
            if (above[0]) {
                stalled++;
                if (exemplar == null) {
                    exemplar = meta.topic() + "-" + meta.partition() + " @" + meta.offset()
                            + " waiting on " + cause[0];
                }
            }
        }
        metrics.reportHeldAboveHighestReceived(stalled);
        if (stalled != lastReportedStalls && stalled > 0) {
            log.warn("{} held record(s) have waited longer than {} ms on a dependency above its "
                    + "channel's highest received offset — nothing received so far can satisfy the "
                    + "claim (e.g. an aborted tail whose producer died). Fail-safe: delivery holds, "
                    + "never reorders. Example: {}", stalled, thresholdMs, exemplar);
        }
        lastReportedStalls = stalled;
    }

    /**
     * Records a held record that could not be deserialised on the forward path: logs the
     * (payload-free) diagnostic and counts the error, then leaves the caller to rethrow. Delivery is
     * fail-closed — the record is not dropped and not forwarded; it remains in the buffer changelog and
     * the task fails, so it can be recovered once the schema is fixed or rolled back.
     */
    private void failPoison(CausalBufferDeserializationException e) {
        metrics.recordDeserializationError();
        log.error("Buffered record could not be deserialised; failing fast (fail-closed). "
                + "It remains in the buffer changelog for recovery. {}", e.details(), e);
    }

}
