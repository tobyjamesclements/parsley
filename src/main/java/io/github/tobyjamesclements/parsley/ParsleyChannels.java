package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * L1, the channels layer: the adaptation that turns Kafka topic-partitions into the reliable FIFO
 * channels classical causal broadcast assumes (Hadzilacos–Toueg reliable channels; the links layer
 * of the Cachin–Guerraoui–Rodrigues stack, minus point-to-point, since a partition is
 * multi-producer fan-out). Everything that exists because Kafka violates a classical channel
 * assumption lives here.
 *
 * <p>This class is the single owner of every piece of causal metadata a node persists. Four vector
 * clocks and the highest-received offsets fold into one durable {@code "frontier"} value in the frontier
 * store (loaded once at construction, rewritten on change, otherwise read from memory):
 * <ul>
 *   <li>the <strong>contiguous frontier</strong>, the highest offset delivered without a gap on each
 *       consumed coordinate; the only clock the delivery gate consults (I4);
 *   <li>the <strong>channel clocks</strong>, per input channel, the dependencies advertised on it;
 *       {@link #completeness()} max-merges these with the frontier into the outbound stamp, which
 *       carries transitive ancestry downstream but never substitutes for local delivery;
 *   <li>the <strong>carried ancestry</strong>, causal past re-homed from coordinates that have left
 *       the node's scope, kept so the stamp keeps dominating it (I9); stamp-side only;
 *   <li>the <strong>own outputs</strong>, the node's own acknowledged sink positions, recovering
 *       CBCAST's own-slot semantics since the broker assigns the sender's offsets (I3, I8);
 *       stamp-side only;
 *   <li>the <strong>highest-received offsets</strong>, so {@link #bridge} can distinguish a
 *       consumer-skipped offset (a transaction marker or aborted record) from one still to arrive and
 *       cross the former without stalling the contiguous walk.
 * </ul>
 *
 * <p>The forwarded-offset index is not in the {@code "frontier"} value: it is a growable, order-sensitive set
 * of offsets delivered above the contiguous frontier, so it keeps its own keyed store. The delivery
 * gate, the buffer, and the release cascade run in {@link ParsleyCausalBroadcast} over these
 * operations, which also strips an inbound clock's self-cycle before the gate sees it (I5).
 * Invariants preserved here: I3, I4, I5, I8, I9.
 */
final class ParsleyChannels {

    private ParsleyVectorClock frontier;
    // Per input channel (topicId, partition) -> the dependencies advertised on it (max-merged).
    private final Map<CoordKey, ParsleyVectorClock> channels = new HashMap<>();
    // Causal ancestry this node once delivered or carried on coordinates that have since left its
    // consumption scope, re-homed here by rescope() so the outbound stamp keeps dominating it (I2/I9:
    // carried ancestry may be skipped, never dropped — T3.0 A6). The term is this project's coinage
    // (from the redesign doc); the nearest literature concept is the vector time of the node's causal
    // past, restricted to retired channels. Stamp-side only: merged into completeness(), never
    // consulted by the delivery gate, never advanced by deliveries. Persisted in the frontier value.
    private ParsleyVectorClock carriedAncestry = ParsleyVectorClock.empty();
    // The node's own acked output positions per sink coordinate (D2) — the clock that recovers
    // CBCAST's own-slot semantics (the broker performs the sender's clock increment; acknowledge()
    // learns it). Monotone by construction (acknowledge only ever raises entries — I3 depends on
    // this once T2.3 folds it into the stamp) and stamp-side only: never consulted by the delivery
    // gate. Fed three ways, every one an at-or-below-a-real-appended-offset claim (I8): the
    // interceptor registry drained by foldAcknowledgedOutputs(), the init-time sink end-offset
    // seed (ParsleyProcessor), and the restored frontier value — which may trail the last transaction's
    // acks (store caches flush before the producer flush completes acks); the seed heals that.
    private ParsleyVectorClock ownOutputs = ParsleyVectorClock.empty();
    // The acknowledged-outputs source foldAcknowledgedOutputs() drains (the interceptor registry in
    // production), with the sink-name → UUID resolution fixed at bind time; null until
    // bindOwnOutputSource, and permanently null for test-fixture instances and TopologyTestDriver
    // runs (no registry exists there — ownOutputs then advances only via direct acknowledge calls).
    private @Nullable AckedOutputs ackedOutputs;
    // The pending-send view the crossing wait blocks on (the same registry object as ackedOutputs in
    // production); null until bindOwnOutputSource, making awaitOwnOutputQuiescence a no-op wherever
    // no producer registry exists (test-fixture instances, TopologyTestDriver runs).
    private @Nullable PendingSends pendingSends;
    private long crossingWaitTimeoutMs;
    private Map<String, Uuid> ownOutputTopicIds = Map.of();
    // The input-topic set (name -> UUID) this node's persisted state was written under, updated by
    // rescope() and persisted in the frontier value. Comparing it against the currently declared inputs is
    // what makes a scope change detectable at init (the #21 fix: restart-vs-scope-change keys on
    // "input set unchanged since the blob", not on blob presence alone). Names are recorded alongside
    // UUIDs so a recreated topic (same name, new UUID) is provably destroyed rather than merely
    // out of scope.
    private final Map<String, Uuid> declaredInputs = new HashMap<>();
    // The sink-topic set (name -> UUID) the persisted ownOutputs clock was written under, updated by
    // declareSinks() at init and persisted in the frontier value. This is the heal set for the restored
    // ownOutputs clock (T3.4): the blob always trails the final transaction's own acks (store caches
    // flush before the producer flush completes acks — O1), and the init-time end-offset seed heals
    // only the CURRENTLY declared sinks — so a topic that was a sink in the previous run but is not
    // one now (dropped, or turned into an input) would otherwise restart with stamps under-claiming
    // this node's own final-transaction outputs, an I2 hole in the dangerous direction. The previous
    // run's declaration is exactly the set of topics whose acks can have trailed; ParsleyProcessor
    // heals each of them (end-offset acknowledge when the topic survives with its UUID, purge when
    // the UUID is provably destroyed) before the first post-restart stamp.
    private final Map<String, Uuid> declaredSinks = new HashMap<>();
    // Per input channel (topicId, partition) -> the highest offset ever physically received on it. Persisted
    // in the frontier value (part of the EOS transaction, so exact across restart) and consulted by bridge(): the
    // open interval between the previous highest and a newly-received offset was skipped by the
    // read_committed consumer, so it can only hold transaction markers or aborted-transaction records, never
    // an in-flight or held business record. An absent entry means the channel has never been received on —
    // its baseline is seedIfFirstSeen's concern, not a gap to bridge.
    private final Map<CoordKey, Long> highestReceived = new HashMap<>();
    // Per input channel (topicId, partition) -> the highest offset ever DELIVERED on it, including
    // deliveries above a contiguous-frontier gap (non-head-of-line delivery). The frontier and this
    // clock are the two projections of the BSS delivered vector VT(p) that non-FIFO delivery within
    // a partition splits apart: the gate consults the contiguous projection (frontier — "everything
    // up to n"), the stamp must carry the max projection, because an output emitted from an
    // above-gap delivery is causally after that record even though the frontier has not reached it
    // (the input-side sibling of the own-output gap D2 closed: the stamp otherwise carries the
    // delivered record's causes but not the record itself, and a downstream consumer of both topics
    // can deliver the effect before the cause). Stamping it over-claims the gap offsets below it —
    // real appended offsets, so delay-only (I8), exactly like ownOutputs' seeds. Stamp-side only:
    // never consulted by the delivery gate; monotone by construction (observe only ever raises —
    // I3). Deliberately NOT persisted: above-frontier delivered offsets are exactly the forwarded
    // index's marks, which commit in the same EOS transaction as the frontier blob, so the store
    // constructor reconstructs this clock from them losslessly.
    private ParsleyVectorClock highestDelivered = ParsleyVectorClock.empty();
    // Coordinates observed at least once; guards the one-time baseline seed in seedIfFirstSeen.
    private final Set<CoordKey> seenCoordinates = new HashSet<>();
    private final ParsleyForwardedIndex forwardedIndex;
    // The frontier state store, holding the whole ParsleyFrontierState at key "frontier". Production always
    // passes the task's changelog-backed store; tests pass an in-memory KeyValueStore double
    // (see the test fixture factory) — there is no store-less mode.
    private final KeyValueStore<String, byte[]> store;

    /**
     * Durable instance: loads the frontier clock and channel clocks from key {@code "frontier"} of
     * {@code store} (empty if absent), and rewrites that single value on every subsequent change.
     *
     * <p>On a restored (non-empty) load, also sweeps {@code forwardedIndex} once for every coordinate
     * the restored frontier carries, deleting any marked offset at or below that coordinate's watermark
     * — a stale entry that leaked below the contiguous frontier (e.g. via the benign tear direction
     * {@link #delivered}'s Javadoc describes, now closed off by the {@code exactly_once_v2} requirement,
     * but still possible in a store carried over from before that requirement existed) can never be
     * reached by {@link #delivered}'s absorb walk again, so it would otherwise linger in the
     * changelog-backed store forever. A one-shot pass at load, not on the hot delivery path.
     */
    ParsleyChannels(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex) {
        this.store = store;
        this.forwardedIndex = forwardedIndex;
        byte[] blob = store.get(ParsleyStores.FRONTIER_KEY);
        this.frontier = ParsleyVectorClock.empty();
        if (blob != null) {
            load(blob);
            frontier.forEach((topicId, partition, offset) -> forwardedIndex.pruneAtOrBelow(topicId, partition, offset));
            // Reconstruct highestDelivered from the forwarded index: its marks are exactly the
            // offsets delivered above the contiguous frontier, committed in the same EOS transaction
            // as this blob, so the reconstruction is lossless. Entries at or below the frontier need
            // no reconstruction — the frontier itself dominates them wherever the two are merged
            // (stamp()). Every delivered coordinate was received first, so highestReceived's keys
            // enumerate every channel that can carry a mark.
            for (Map.Entry<CoordKey, Long> received : highestReceived.entrySet()) {
                Uuid topicId = received.getKey().topicId();
                int partition = received.getKey().partition();
                for (long mark : forwardedIndex.forwardedAfter(topicId, partition, frontier.offsetFor(topicId, partition))) {
                    highestDelivered = highestDelivered.observe(topicId, partition, mark);
                }
            }
        }
    }

    /**
     * The current contiguous frontier clock — the delivered vector VT(p), in Mattern's sense (the
     * <em>frontier</em> of a consistent cut; Mattern 1988, "Virtual Time and Global States of
     * Distributed Systems"), indexed by channel rather than by process (see
     * {@link ParsleyVectorClock}).
     */
    ParsleyVectorClock frontier() {
        return frontier;
    }

    /**
     * The <em>receive</em> request: a record arrived on channel {@code (topicId, partition)} at
     * {@code offset}. Establishes the channel's density baseline the first time the coordinate is
     * ever observed ({@link #seedIfFirstSeen}) and bridges the consumer-skipped offsets below the
     * record ({@link #bridge}) — the two Kafka-specific repairs that make the channel look gap-free
     * to L2. Returns {@code true} if the contiguous frontier advanced; the caller must then cascade
     * (a held record may have been waiting on exactly a seeded or bridged offset). The record itself
     * is not delivered here — delivery is L2's decision, reported back via {@link #delivered}.
     *
     * <p>At most one of the two steps can advance the frontier on any single call: a first sighting
     * seeds but records the offset as the channel's highest received without bridging, and a
     * later sighting can bridge but never re-seeds.
     */
    boolean receive(Uuid topicId, int partition, long offset) {
        boolean seeded = seedIfFirstSeen(topicId, partition, offset);
        boolean bridged = bridge(topicId, partition, offset);
        return seeded || bridged;
    }

    /**
     * The <em>normalize</em> request: turns a raw inbound dependency clock into the clock L2's gate
     * evaluates (I5 — after this step, no clock inside L2 carries a self-reference). One step:
     * <strong>self-cycle removal</strong>. A record depending on its own {@code (topicId,
     * partition, offset)} has, by being delivered, met that dependency, so it must not wait on
     * itself (this keeps a self-referential stamp on a fused chain from deadlocking). Only the
     * exact self-coordinate is removed; a backward same-partition dependency ({@code required
     * < offset}) is an intra-topic dependency like any other and flows through unchanged.
     *
     * <p>View-only: never rewrites recorded state or the outbound stamp ({@link #completeness()} is
     * computed separately). A pure function of the raw clock and the source coordinate, so the gate
     * may evaluate it as often as it likes — every evaluation of the same held record yields the
     * same result.
     */
    ParsleyVectorClock normalize(ParsleyVectorClock rawDeps, Uuid sourceTopicId, int sourcePartition, long sourceOffset) {
        return rawDeps.offsetFor(sourceTopicId, sourcePartition) == sourceOffset
                ? rawDeps.without(sourceTopicId, sourcePartition)
                : rawDeps;
    }

    /**
     * The <em>acknowledge</em> request: the producer acked this node's own send to sink coordinate
     * {@code (topicId, partition)} at {@code offset} — or an equally I8-sound stand-in for one (the
     * init-time end-offset seed, which claims the sink's last appended position whether or not this
     * task produced it). Folds into the {@link #ownOutputs()} clock (max, monotone — a lower or
     * equal offset is a no-op, so replaying acks costs nothing) and persists on advance. Keyed by
     * the topic's UUID, like every other request here: L1 owns coordinate identity (E1), and the
     * name → UUID translation happens once, where sinks are resolved at init.
     */
    void acknowledge(Uuid topicId, int partition, long offset) {
        if (offset < 0 || offset <= ownOutputs.offsetFor(topicId, partition)) {
            return;
        }
        ownOutputs = ownOutputs.observe(topicId, partition, offset);
        persist();
    }

    /**
     * The node's own acked output positions per sink coordinate — the clock that recovers CBCAST's
     * own-slot semantics (D2: the broker performs the sender's clock increment, learned via
     * {@link #acknowledge}). Stamp-side only, never consulted by the delivery gate; {@link #stamp()}
     * carries it on every outbound record.
     */
    ParsleyVectorClock ownOutputs() {
        return ownOutputs;
    }

    /**
     * The outbound vector timestamp, {@code completeness ∪ ownOutputs ∪ highestDelivered}: the clock
     * {@link ParsleyCausalBroadcast#broadcast} attaches to every outbound record, and equally the
     * node's total knowledge that the I6 relay rule compares a carried clock against. The own-outputs
     * and highest-delivered clocks are merged here and only here; neither feeds {@link #completeness()}
     * or the delivery gate.
     */
    ParsleyVectorClock stamp() {
        return completeness().merge(ownOutputs).merge(highestDelivered);
    }

    /**
     * Wires the acknowledged-outputs source {@link #foldAcknowledgedOutputs()} drains and the
     * pending-send view the crossing wait blocks on, with this node's sink-name → UUID resolution
     * (acks arrive keyed by name; the clock is keyed by UUID). Called once at init; a test-fixture
     * instance with no registry never calls it, and its {@code ownOutputs} then advances only via
     * direct {@link #acknowledge} calls.
     *
     * @param source                the acked-offsets view {@link #foldAcknowledgedOutputs()} drains
     * @param pendingSends          the pending-send view {@link #awaitOwnOutputQuiescence} waits on
     * @param sinkTopicIds          sink topic name → UUID, resolved at init
     * @param crossingWaitTimeoutMs the crossing wait's bound (the producer's
     *                              {@code delivery.timeout.ms}), past which an unacked send has failed
     */
    void bindOwnOutputSource(AckedOutputs source, PendingSends pendingSends,
                             Map<String, Uuid> sinkTopicIds, long crossingWaitTimeoutMs) {
        this.ackedOutputs = source;
        this.pendingSends = pendingSends;
        this.ownOutputTopicIds = Map.copyOf(sinkTopicIds);
        this.crossingWaitTimeoutMs = crossingWaitTimeoutMs;
    }

    /**
     * The crossing wait: blocks until the task's producer has no unacknowledged send to an own-sink
     * coordinate outside {@code exceptDestinations}, so the ack fold before the next {@link #stamp()}
     * cannot miss a send that process-order-precedes the record being stamped. An empty set waits for
     * full quiescence, the conservative form used before a business forward, whose destination
     * partition is unknowable at stamp time; over-waiting only folds more acked positions, which is
     * sound (I8). A marker stamp passes its exact destination set, which same-partition FIFO and I3
     * already cover.
     *
     * <p>The wait never stamps-and-proceeds: it throws {@link CausalPendingAckException} on timeout or
     * an observed acknowledgement failure, so the caller's EOS transaction dies rather than emit a
     * possibly under-claiming stamp. A no-op until {@link #bindOwnOutputSource} is called.
     */
    void awaitOwnOutputQuiescence(Set<TopicPartition> exceptDestinations) {
        PendingSends pending = pendingSends;
        if (pending != null) {
            pending.awaitQuiescentExcept(exceptDestinations, crossingWaitTimeoutMs);
        }
    }

    /**
     * Drains the bound acknowledged-outputs source into {@link #ownOutputs()} — called by
     * {@link ParsleyCausalBroadcast#broadcast} before every stamp, so no coordinate acked before
     * this stamp can be missing from the clock the stamp (from T2.3) carries. Idempotent and
     * cheap: the source exposes max acked offsets per coordinate and {@link #acknowledge} is a
     * monotone no-op below the current entry, so re-draining persists nothing new. A no-op until
     * {@link #bindOwnOutputSource} is called.
     */
    void foldAcknowledgedOutputs() {
        AckedOutputs source = ackedOutputs;
        if (source == null) {
            return;
        }
        source.forEachAcked((topic, partition, offset) -> {
            Uuid topicId = ownOutputTopicIds.get(topic);
            if (topicId != null) {
                acknowledge(topicId, partition, offset);
            }
        });
    }

    /**
     * The causal completeness clock: this node's contiguous frontier max-merged with every input
     * channel's advertised dependencies and with the {@link #carriedAncestry} re-homed from retired
     * coordinates (I9). It is the delivered/advertised boundary the node carries downstream for each
     * receiver's own gate to verify, not the delivery gate itself: the gate checks {@link #frontier()}
     * alone, so an advertised claim never releases a record ahead of local delivery of its cause.
     */
    ParsleyVectorClock completeness() {
        ParsleyVectorClock result = frontier.merge(carriedAncestry);
        for (ParsleyVectorClock advertised : channels.values()) {
            result = result.merge(advertised);
        }
        return result;
    }

    /**
     * Records that the record at {@code (topicId, partition, offset)} was delivered: marks the offset
     * forwarded, walks the longest contiguous run now achievable, advances the frontier, persists,
     * then prunes the forwarded-index entries the walk absorbed. The frontier write and the prune
     * commit in one Kafka transaction ({@code exactly_once_v2} is required), so a crash cannot tear
     * one from the other. A delivery at or below the current frontier is an at-least-once replay and a
     * no-op, deliberately never marked, since the absorb walk only scans strictly above the frontier.
     */
    void delivered(Uuid topicId, int partition, long offset) {
        long watermark = frontier.offsetFor(topicId, partition);
        if (offset <= watermark) {
            return;
        }
        highestDelivered = highestDelivered.observe(topicId, partition, offset);
        forwardedIndex.mark(topicId, partition, offset);
        absorbContiguous(topicId, partition, watermark);
    }

    /**
     * Bridges the offsets a {@code read_committed} consumer skipped between the previous highest
     * received offset on {@code (topicId, partition)} and {@code receivedOffset} (a transaction marker
     * or aborted record the consumer never returns), so the contiguous walk can cross the hole they
     * would otherwise wedge it at. Called once per received record, before that record's own delivery.
     * Returns {@code true} if the frontier advanced, in which case the caller must cascade
     * ({@link ParsleyCausalBroadcast#propagate}), since a held record may have waited on a bridged offset.
     *
     * <p>Sound because Kafka delivers a partition in strict offset order: once {@code receivedOffset}
     * has arrived, every lower offset that would ever be returned already has, so an offset in the
     * bridged interval was skipped permanently and is never a cause any dependent awaits. A first
     * sighting (no {@code highestReceived} entry) bridges nothing and records the baseline, leaving
     * history below it to {@link #seedIfFirstSeen}; a replay at or below the recorded highest is a
     * no-op. Distinguishing a marker gap from a retention jump that dropped real records is the
     * caller's responsibility (only it sees the log-start offset); this method assumes a genuine skip.
     */
    boolean bridge(Uuid topicId, int partition, long receivedOffset) {
        CoordKey key = new CoordKey(topicId, partition);
        Long prev = highestReceived.get(key);
        if (prev == null) {
            highestReceived.put(key, receivedOffset);
            persist();
            return false;
        }
        if (receivedOffset <= prev) {
            return false;
        }
        long watermark = frontier.offsetFor(topicId, partition);
        // Fast path: the frontier is caught up to the previous highest received (watermark == prev), so no
        // held record sits in the skipped run — it is contiguous from the frontier and, being all
        // consumer-skipped, all markers. Fold it straight into the frontier without O(gap) forwarded-index
        // writes; a large aborted transaction could otherwise mark hundreds of thousands of offsets inside
        // one EOS transaction, risking the transaction timeout and a crash-loop on legal input.
        if (watermark == prev) {
            frontier = frontier.observe(topicId, partition, receivedOffset - 1);
            highestReceived.put(key, receivedOffset);
            persist();
            return frontier.offsetFor(topicId, partition) > watermark;
        }
        // Slow path: a held record may sit between the frontier and the previous highest received, so mark
        // the skipped offsets and let the contiguous walk absorb only the run now reachable (it stops at the
        // held record). At-or-below-watermark offsets are not marked, mirroring deliver().
        for (long skipped = prev + 1; skipped < receivedOffset; skipped++) {
            if (skipped <= watermark) {
                continue;
            }
            forwardedIndex.mark(topicId, partition, skipped);
        }
        highestReceived.put(key, receivedOffset);
        absorbContiguous(topicId, partition, watermark);
        return frontier.offsetFor(topicId, partition) > watermark;
    }

    /**
     * Walks the longest run of consecutive forwarded offsets on {@code (topicId, partition)} now achievable
     * from {@code watermark}, advances the contiguous frontier by it, persists, and only then prunes the
     * absorbed forwarded-index entries. Shared by {@link #delivered} (which marks one offset first) and
     * {@link #bridge} (which marks a whole skipped run first). The persist-before-prune order is
     * load-bearing — see {@link #delivered}'s Javadoc.
     */
    private void absorbContiguous(Uuid topicId, int partition, long watermark) {
        long extended = watermark;
        List<Long> absorbed = new ArrayList<>();
        for (long candidate : forwardedIndex.forwardedAfter(topicId, partition, watermark)) {
            if (candidate != extended + 1) break;
            absorbed.add(candidate);
            extended = candidate;
        }
        frontier = frontier.observe(topicId, partition, extended);
        persist();
        for (long candidate : absorbed) {
            forwardedIndex.unmark(topicId, partition, candidate);
        }
    }

    /**
     * Establishes the contiguous frontier's starting point the first time this coordinate is observed.
     * The name is a coinage: no literature analog — the mechanism exists
     * only because Kafka retention means a channel's history need not begin at its first offset.
     * The first offset seen need not be 0 (finite retention, fresh consumer
     * group); anything below it is outside the causal-broadcast core's purview, not an unfillable gap, so folding
     * {@code offset - 1} into the frontier lets the contiguous walk start there. Returns {@code true} if
     * a seed was applied (the caller should then cascade). The coordinate is marked seen on the first
     * call even if the record is held, so a later record cannot re-trigger the seed and skip the
     * still-held earlier one. The seen-set is in-memory only; {@link ParsleyCausalBroadcast}'s constructor
     * replays this call for every restored held record's source coordinate (at its lowest held
     * offset) so the guard survives a restart. A coordinate the frontier already records (a restored
     * or rescope-seeded channel) is left as-is.
     */
    boolean seedIfFirstSeen(Uuid topicId, int partition, long offset) {
        if (!seenCoordinates.add(new CoordKey(topicId, partition))) return false;
        long current = frontier.offsetFor(topicId, partition);
        // An unrecorded coordinate folds everything below its first offset into the frontier; a recorded
        // one is left as-is.
        long seedTo = current < 0 ? offset - 1 : current;
        if (seedTo < 0 || seedTo <= current) return false;
        frontier = frontier.observe(topicId, partition, seedTo);
        persist();
        return true;
    }

    /**
     * The highest offset ever physically received on channel {@code (topicId, partition)}, or
     * {@code -1} if the channel has never been received on. This is {@link #bridge}'s skip-detection
     * baseline, exposed for the stalled-dependency observability scan (T3.0 A9): a record held on a
     * dependency <em>above</em> this value is waiting on an offset nothing received so far can
     * satisfy — the exact signature of a claim whose producer's send failed or whose channel went
     * permanently silent (fail-safe, never unsafe; the delay is unbounded, so it must be visible).
     */
    long highestReceived(Uuid topicId, int partition) {
        Long highest = highestReceived.get(new CoordKey(topicId, partition));
        return highest == null ? -1L : highest;
    }

    /**
     * Max-merges {@code clock} into channel {@code (topicId, partition)}'s advertised dependencies
     * (monotonic: the stored clock never decreases) and persists. A first call for a channel
     * initialises it from {@code clock}.
     *
     * <p>A channel's entry no longer holds {@link #completeness()} down to an intersection minimum —
     * {@link #completeness()} is a plain max-merge now, so a channel with nothing advertised simply
     * contributes nothing rather than excluding a coordinate every other channel has confirmed. What a
     * seeded-but-silent channel entry still does is give {@link #rescope} something to check against.
     */
    void channelUpdate(Uuid topicId, int partition, ParsleyVectorClock clock) {
        CoordKey key = new CoordKey(topicId, partition);
        ParsleyVectorClock existing = channels.get(key);
        channels.put(key, existing == null ? clock : existing.merge(clock));
        persist();
    }

    /**
     * The input-topic set (name → UUID) the persisted state was written under — the previous run's
     * declaration, empty on a fresh store or a pre-T1.3 blob. Read it <em>before</em> {@link #rescope}
     * (which overwrites it with the current set) to report the scope diff at init.
     */
    Map<String, Uuid> declaredInputs() {
        return Map.copyOf(declaredInputs);
    }

    /**
     * The sink-topic set (name → UUID) the persisted {@link #ownOutputs()} clock was written under —
     * the previous run's declaration, empty on a fresh store or a pre-T3.4 blob. Read it at init to
     * heal the restored clock's trailing acks (see the field note), <em>before</em>
     * {@link #declareSinks} overwrites it with the current declaration.
     */
    Map<String, Uuid> declaredSinks() {
        return Map.copyOf(declaredSinks);
    }

    /**
     * Records the currently declared sink set and persists, so the next init can heal exactly the
     * coordinates whose acks may have trailed this run's final transaction. Called once at init,
     * after the previous declaration has been read and healed. Sink resolution is strict at init
     * (an unresolvable sink fails init), so this always records the full declared set.
     */
    void declareSinks(Map<String, Uuid> currentSinks) {
        declaredSinks.clear();
        declaredSinks.putAll(currentSinks);
        persist();
    }

    /**
     * Removes every {@code topicId} coordinate from the {@link #ownOutputs()} clock and persists —
     * I9's one permitted removal from stamp-feeding state, for a provably destroyed topic (deleted,
     * or recreated under a new UUID): its records can never be delivered by any receiver (E1), so a
     * claim on them can gate nothing and heal nothing. Called by the init-time former-sink heal;
     * {@link #rescope} performs the same purge for recreated <em>input</em> topics.
     */
    void destroyOwnOutput(Uuid topicId) {
        ownOutputs = ownOutputs.retaining((id, partition) -> !id.equals(topicId));
        persist();
    }

    /**
     * Reconciles restored causal state with the currently declared input set, the scope-change step
     * run once at init before the current channels are seeded. The governing principle: the causal
     * past a node has delivered or carried may be skipped, but never dropped and never re-entered.
     * Four cases, diffed against the persisted {@link #declaredInputs}:
     *
     * <ol>
     *   <li><strong>Destroyed coordinates:</strong> a declared topic whose UUID changed was deleted
     *       and recreated, so the old UUID's entries can never be delivered (E1) and are removed
     *       outright, the one removal permitted; the new UUID starts as an added channel.</li>
     *   <li><strong>Shrink:</strong> every other entry leaving scope max-merges into
     *       {@link #carriedAncestry} before it is pruned, so {@link #completeness()} is unchanged
     *       except at destroyed coordinates (I9); dropping it would let a downstream receiver reorder
     *       an effect against its retired-channel cause.</li>
     *   <li><strong>Growth:</strong> an input declared now but not before seeds its frontier at this
     *       node's carried-ancestry value for the coordinate, never at log-start, so what was already
     *       ignored stays skipped; a genuinely new topic with no carried entry seeds nothing.</li>
     *   <li><strong>Fresh store:</strong> nothing to diff; the current set is recorded.</li>
     * </ol>
     *
     * <p>This reconciles clocks only; the disposition of restored held records whose source left scope
     * lives in the {@link ParsleyCausalBroadcast} constructor.
     *
     * @param currentInputs the currently declared input topics, name → UUID
     * @param taskPartition the partition this task owns on every input
     */
    void rescope(Map<String, Uuid> currentInputs, int taskPartition) {
        // 1 — destroyed: recreated inputs' old UUIDs leave every stamp-feeding structure for good.
        Set<Uuid> destroyed = new HashSet<>();
        for (Map.Entry<String, Uuid> declared : declaredInputs.entrySet()) {
            Uuid current = currentInputs.get(declared.getKey());
            if (current != null && !current.equals(declared.getValue())) {
                destroyed.add(declared.getValue());
            }
        }
        if (!destroyed.isEmpty()) {
            ParsleyVectorClock.CoordinatePredicate notDestroyed =
                    (topicId, partition) -> !destroyed.contains(topicId);
            frontier = frontier.retaining(notDestroyed);
            carriedAncestry = carriedAncestry.retaining(notDestroyed);
            channels.keySet().removeIf(key -> destroyed.contains(key.topicId()));
            channels.replaceAll((key, clock) -> clock.retaining(notDestroyed));
            highestReceived.keySet().removeIf(key -> destroyed.contains(key.topicId()));
            // A destroyed topic this node also produced (an input that is its own sink — a cycle)
            // leaves ownOutputs too: its old-UUID coordinates can never be delivered by any
            // receiver (E1), which is I9's one permitted removal from stamp-feeding state.
            // Everything else about the input-set diff leaves ownOutputs alone below — it is
            // keyed by sink coordinates, not input channels, and it only ever grows (I3).
            ownOutputs = ownOutputs.retaining(notDestroyed);
            highestDelivered = highestDelivered.retaining(notDestroyed);
        }

        // 2 — shrink: fold everything else leaving scope into the carried ancestry, then prune it.
        // A retired/recreated coordinate must also leave the highest-received map, or its dead
        // CoordKey would be re-serialised into the frontier value on every persist forever.
        Set<Uuid> currentUuids = new HashSet<>(currentInputs.values());
        ParsleyVectorClock.CoordinatePredicate inScope = (topicId, partition) ->
                partition == taskPartition && currentUuids.contains(topicId);
        ParsleyVectorClock.CoordinatePredicate outOfScope = (topicId, partition) ->
                !inScope.test(topicId, partition);
        carriedAncestry = carriedAncestry.merge(frontier.retaining(outOfScope));
        for (Map.Entry<CoordKey, ParsleyVectorClock> channel : channels.entrySet()) {
            boolean channelRetired = outOfScope.test(channel.getKey().topicId(), channel.getKey().partition());
            carriedAncestry = carriedAncestry.merge(
                    channelRetired ? channel.getValue() : channel.getValue().retaining(outOfScope));
        }
        // An above-gap delivered offset on a retiring channel is delivered causal past like any
        // frontier entry — re-homed, never dropped (A6).
        carriedAncestry = carriedAncestry.merge(highestDelivered.retaining(outOfScope));
        frontier = frontier.retaining(inScope);
        channels.keySet().removeIf(key -> outOfScope.test(key.topicId(), key.partition()));
        channels.replaceAll((key, clock) -> clock.retaining(inScope));
        highestDelivered = highestDelivered.retaining(inScope);
        highestReceived.keySet().removeIf(key -> outOfScope.test(key.topicId(), key.partition()));

        // 3 — growth: an added channel skips the prefix this node already carried. The seed reads
        // stamp(), not completeness(): an added input that is this node's own former sink would
        // otherwise under-skip the prefix its stamps already claimed via ownOutputs — "skip what
        // you already claimed" extends A5's "skip what you already ignored" (T2.2 carry-forward).
        if (!declaredInputs.isEmpty()) {
            for (Map.Entry<String, Uuid> current : currentInputs.entrySet()) {
                if (declaredInputs.containsKey(current.getKey())) {
                    continue;
                }
                Uuid topicId = current.getValue();
                long carried = stamp().offsetFor(topicId, taskPartition);
                if (carried > frontier.offsetFor(topicId, taskPartition)) {
                    frontier = frontier.observe(topicId, taskPartition, carried);
                    forwardedIndex.pruneAtOrBelow(topicId, taskPartition, carried);
                }
            }
        }

        declaredInputs.clear();
        declaredInputs.putAll(currentInputs);
        persist();
    }

    /**
     * Whether {@code (topicId, partition, offset)} has already been delivered by this node: at or
     * below the contiguous frontier, or forwarded out of order (still marked in the forwarded index,
     * above the frontier but not yet absorbed). The receive path consults this to <em>skip</em> a
     * replayed already-delivered record instead of forwarding it to the delegate a second time —
     * routine after {@link #rescope}'s growth seeding (the consumer re-fetches an added input from
     * log-start while the seeded frontier already covers the carried prefix), and otherwise the
     * fail-safe net for an at-or-below-watermark arrival that {@code exactly_once_v2} should make
     * impossible (T3.0 A11).
     */
    boolean alreadyDelivered(Uuid topicId, int partition, long offset) {
        return offset <= frontier.offsetFor(topicId, partition)
                || forwardedIndex.contains(topicId, partition, offset);
    }

    private void persist() {
        store.put(ParsleyStores.FRONTIER_KEY, toBytes());
    }

    /**
     * Serialises this node's persisted causal metadata into the {@code "frontier"} value, delegating
     * the byte layout and wire version to {@link ParsleyFrontierState}. The {@link #highestDelivered}
     * clock is deliberately excluded: it is reconstructed at load from the forwarded index, which
     * commits in the same EOS transaction (see the constructor).
     */
    private byte[] toBytes() {
        return new ParsleyFrontierState(frontier, channels, highestReceived, carriedAncestry,
                declaredInputs, ownOutputs, declaredSinks).toBytes();
    }

    /**
     * Restores every persisted section from {@code blob} into this instance's live state, in the one
     * pass {@link ParsleyFrontierState#fromBytes} performs. A blob whose wire version this build does
     * not recognise faults here rather than loading partially (O6: there is no cross-version upgrade
     * path before 1.0). The restored {@link #ownOutputs} clock may trail the last transaction's acks
     * (store caches flush before the producer flush completes acks); the init-time sink end-offset
     * seed re-covers that, an I8-sound direction. Called once, at construction, over empty maps.
     */
    private void load(byte[] blob) {
        ParsleyFrontierState state = ParsleyFrontierState.fromBytes(blob);
        frontier = state.frontier();
        channels.putAll(state.channels());
        highestReceived.putAll(state.highestReceived());
        carriedAncestry = state.carriedAncestry();
        declaredInputs.putAll(state.declaredInputs());
        ownOutputs = state.ownOutputs();
        declaredSinks.putAll(state.declaredSinks());
    }

    record CoordKey(Uuid topicId, int partition) {}

    /**
     * The narrow seam {@link #foldAcknowledgedOutputs()} drains: a snapshot view of the highest
     * successfully acknowledged offset per sink coordinate, keyed by topic <em>name</em> (that is
     * what the producer ack carries; {@link #bindOwnOutputSource}'s map translates to UUID
     * identity). Implemented by {@code ParsleyOwnOutputRegistry} in production and by hand-rolled
     * doubles in tests. Iteration must be safe against concurrent updates (acks arrive on the
     * producer network thread); values must be monotone per coordinate.
     */
    interface AckedOutputs {

        /** Presents each coordinate's highest successfully acknowledged offset to {@code consumer}. */
        void forEachAcked(Consumer consumer);

        /** A receiver for each {@code (topic, partition, ackedOffset)} entry. */
        @FunctionalInterface
        interface Consumer {
            void accept(String topic, int partition, long offset);
        }
    }

    /**
     * The narrow seam {@link #awaitOwnOutputQuiescence} blocks on: the calling task's unacknowledged
     * own-sink sends, keyed by topic <em>name</em> like {@link AckedOutputs} (that is what the
     * producer path carries). Implemented by {@code ParsleyOwnOutputRegistry} in production —
     * resolving "this task's pending sends" from the calling thread (one producer per StreamThread
     * under EOS v2) — and by hand-rolled doubles in tests.
     */
    @FunctionalInterface
    interface PendingSends {

        /**
         * Blocks until no send to any tracked coordinate outside {@code exceptDestinations} is
         * unacknowledged; empty means full quiescence. Throws {@link CausalPendingAckException}
         * on timeout or on an acknowledgement failure observed while waiting (T3.0 A8) — never
         * returns normally without genuine quiescence.
         */
        void awaitQuiescentExcept(Set<TopicPartition> exceptDestinations, long timeoutMs);
    }
}