package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <strong>L1 — channels: the Kafka-to-reliable-FIFO-channel adaptation.</strong> Turns Kafka
 * topic-partitions into the channels classical causal broadcast assumes (Hadzilacos–Toueg's
 * reliable channels; the links layer of the Cachin–Guerraoui–Rodrigues stack, minus point-to-point —
 * a partition is multi-producer fan-out). Everything that exists because Kafka violates a classical
 * channel assumption lives here, stated once. Presented in the CGR module style ({@code
 * package-info} states the two Parsley-wide deviations from the textbook presentation):
 *
 * <pre>
 * requests:   receive(topicId, partition, offset)     a record arrived on a channel: establish the
 *                                                     density baseline (seed) and bridge
 *                                                     consumer-skipped holes
 *             delivered(topicId, partition, offset)   L2 records a delivery; contiguous frontier
 *                                                     advance (I4)
 *             normalize(rawDeps, source)              normalise an inbound dependency clock before
 *                                                     L2 evaluates it: self-cycle strip (I5); a
 *                                                     pure function of the clock and the source
 *             acknowledge(topicId, partition, offset) producer ack → ownOutputs (D2)
 *             awaitOwnOutputQuiescence(except)        the crossing wait (O1, per-partition per
 *                                                     T3.0 A7): block until no own-sink send
 *                                                     outside the excluded destinations is
 *                                                     unacknowledged; throw, never stamp, on
 *                                                     timeout or ack failure (A8)
 *             rescope(currentInputs, taskPartition)   reconcile restored state with the declared
 *                                                     input set: re-home retired ancestry into the
 *                                                     carried-ancestry clock (A6), seed added
 *                                                     channels from carried ancestry (A5)
 * queries:    frontier() → ParsleyVectorClock         the delivered vector VT(p)
 *             ownOutputs() → ParsleyVectorClock       the node's own acked output positions (D2)
 *             stamp() → ParsleyVectorClock            the outbound vector timestamp
 *                                                     completeness ∪ ownOutputs ∪ highestDelivered
 *                                                     (D2 + the above-gap repair) — equally the
 *                                                     node's total knowledge, the I6 relay bound
 *             alreadyDelivered(topicId, partition,    membership in the delivered set (frontier ∪
 *                 offset) → boolean                   forwarded index); the receive path's replay
 *                                                     skip guard
 * properties: I3 (per-producer stamp monotonicity), I4 (contiguous frontier), I5 (normalised
 *             clocks), I8 (stamp over-claim soundness), I9 (unconditional merge, restore-side)
 * </pre>
 *
 * <p>Concretely, this class is the single owner of all causal metadata a node persists: the
 * contiguous frontier clock, the per-input-channel clocks, and the seeding/forwarding
 * infrastructure that maintains the frontier (the held-record buffer and its candidate index are a
 * separate concern).
 *
 * <p>Three structures fold into one durable value here, stored as a single {@code "f"} key-value pair
 * in the frontier state store (loaded once at construction, rewritten on change, read from memory):
 * <ul>
 *   <li>the <strong>contiguous frontier clock</strong> — the highest offset delivered without a gap
 *       on each coordinate this node consumes; and
 *   <li>the <strong>channel clocks</strong> — for each input channel {@code (topicId, partition)},
 *       the dependencies advertised on it (max-merged). {@link #completeness()} is the frontier clock
 *       max-merged with every channel's advertised view — the <em>outbound stamp</em>, carrying
 *       transitive ancestry downstream. The delivery gate itself checks the contiguous frontier alone
 *       (see {@link ParsleyCausalBroadcast}); an advertised claim never substitutes for local delivery; and
 *   <li>the <strong>highest-received offsets</strong> — for each input channel, the highest offset ever
 *       physically received, so {@link #bridge} can tell a consumer-skipped offset (a transaction marker
 *       or aborted record the {@code read_committed} consumer never returns) from an offset still to
 *       arrive, and cross the former without stalling the contiguous walk.
 * </ul>
 *
 * <p>The <strong>forwarded-offset index</strong> is <em>not</em> in the {@code "f"} blob: it is a
 * growable, order-sensitive set (offsets delivered above the contiguous frontier) with incremental
 * per-offset writes and range reads, so it keeps its own keyed store, injected here as a collaborator.
 *
 * <p>Core operations: {@link #completeness()} (the delivery boundary), {@link #delivered} (advance the
 * contiguous frontier for a delivered record), {@link #seedIfFirstSeen} (establish the baseline the
 * first time a coordinate is observed, since consumption need not start at offset 0), and the channel
 * accessors. {@link ParsleyCausalBroadcast} enforces causal transitivity (the cascade after each delivery) and
 * owns the buffer around these operations.
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
    // consulted by the delivery gate, never advanced by deliveries. Persisted in the "f" blob.
    private ParsleyVectorClock carriedAncestry = ParsleyVectorClock.empty();
    // The node's own acked output positions per sink coordinate (D2) — the clock that recovers
    // CBCAST's own-slot semantics (the broker performs the sender's clock increment; acknowledge()
    // learns it). Monotone by construction (acknowledge only ever raises entries — I3 depends on
    // this once T2.3 folds it into the stamp) and stamp-side only: never consulted by the delivery
    // gate. Fed three ways, every one an at-or-below-a-real-appended-offset claim (I8): the
    // interceptor registry drained by foldAcknowledgedOutputs(), the init-time sink end-offset
    // seed (ParsleyProcessor), and the restored "f" blob — which may trail the last transaction's
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
    // rescope() and persisted in the "f" blob. Comparing it against the currently declared inputs is
    // what makes a scope change detectable at init (the #21 fix: restart-vs-scope-change keys on
    // "input set unchanged since the blob", not on blob presence alone). Names are recorded alongside
    // UUIDs so a recreated topic (same name, new UUID) is provably destroyed rather than merely
    // out of scope.
    private final Map<String, Uuid> declaredInputs = new HashMap<>();
    // The sink-topic set (name -> UUID) the persisted ownOutputs clock was written under, updated by
    // declareSinks() at init and persisted in the "f" blob. This is the heal set for the restored
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
    // in the "f" blob (part of the EOS transaction, so exact across restart) and consulted by bridge(): the
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
    // The frontier state store, holding this frontier+channels blob at key "f". Production always
    // passes the task's changelog-backed store; tests pass an in-memory KeyValueStore double
    // (see the test fixture factory) — there is no store-less mode.
    private final KeyValueStore<String, byte[]> store;

    /**
     * Durable instance: loads the frontier clock and channel clocks from key {@code "f"} of
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
     * The outbound vector timestamp — {@code completeness ∪ ownOutputs ∪ highestDelivered} (D2 +
     * the above-gap delivery repair): everything this node has delivered contiguously, carried, or
     * heard advertised ({@link #completeness()}), max-merged with its own acked output positions
     * ({@link #ownOutputs()}) and with the highest offset delivered on each input channel —
     * including deliveries still above a contiguous-frontier gap, which the frontier cannot yet
     * claim but the stamp must (an output emitted from such a delivery is causally after it; see
     * the {@code highestDelivered} field note). This is the clock
     * {@link ParsleyCausalBroadcast#broadcast} attaches to every outbound record, and equally the
     * node's <em>total knowledge</em> — the {@code known()} the I6 relay rule compares a carried
     * clock against (frontier ∪ channel clocks ∪ carried ancestry ∪ ownOutputs ∪ highestDelivered).
     * Both extra clocks are merged here and only here: they never feed {@link #completeness()}
     * (which stays the delivered/advertised view — what this node has locally delivered, carried,
     * or heard advertised) and never the delivery gate.
     */
    ParsleyVectorClock stamp() {
        return completeness().merge(ownOutputs).merge(highestDelivered);
    }

    /**
     * Wires the acknowledged-outputs source {@link #foldAcknowledgedOutputs()} drains — the
     * interceptor registry in production — together with this node's sink-name → UUID resolution
     * (acks arrive keyed by topic name; the clock is keyed by UUID identity, E1). Called once at
     * init by {@code ParsleyProcessor}; never called for test-fixture instances, whose
     * {@code ownOutputs} then advances only through direct {@link #acknowledge} calls. Sink
     * resolution is strict at init (an unresolvable sink fails init) and a topology is exactly one
     * stage, so {@code sinkTopicIds} always covers every topic the registry tracks — the ack
     * fold's missing-translation skip is a defensive guard, not a load-bearing filter.
     *
     * @param source                the acked-offsets view {@link #foldAcknowledgedOutputs()} drains
     * @param pendingSends          the pending-send view {@link #awaitOwnOutputQuiescence} waits on
     *                              (the same registry object in production)
     * @param sinkTopicIds          sink topic name → UUID, resolved at init
     * @param crossingWaitTimeoutMs the crossing wait's bound — the producer's
     *                              {@code delivery.timeout.ms}, past which an unacked send has
     *                              failed and the wait must throw rather than stamp (T3.0 A8)
     */
    void bindOwnOutputSource(AckedOutputs source, PendingSends pendingSends,
                             Map<String, Uuid> sinkTopicIds, long crossingWaitTimeoutMs) {
        this.ackedOutputs = source;
        this.pendingSends = pendingSends;
        this.ownOutputTopicIds = Map.copyOf(sinkTopicIds);
        this.crossingWaitTimeoutMs = crossingWaitTimeoutMs;
    }

    /**
     * The crossing wait (O1; per-partition granularity per T3.0 A7): blocks until the calling
     * task's producer has no unacknowledged send to any own-sink coordinate outside
     * {@code exceptDestinations}, so the ack fold ({@link #foldAcknowledgedOutputs()}) that
     * precedes the very next {@link #stamp()} read cannot miss the coordinate of a send that
     * process-order-precedes the record being stamped. Passing an <em>empty</em> set waits for
     * full quiescence — the conservative form used before stamping a business forward, whose
     * destination partition is unknowable at stamp time (the sink's partitioner runs downstream
     * of {@code forward()}); over-waiting only ever folds <em>more</em> acked positions, which is
     * monotone-sound (I8), where a mispredicted same-partition exemption would silently
     * under-claim. A marker stamp passes its exact destination set — each sink at the task's own
     * partition — whose same-coordinate pending sends partition FIFO plus I3 already cover (and
     * whose cross-sink exemption O4's recorded null-message exemption covers).
     *
     * <p><strong>Never stamp-and-proceed</strong> (T3.0 A8): the bound wait throws
     * {@link ParsleyPendingAckException} on timeout or on an observed acknowledgement failure —
     * the caller's EOS transaction must die rather than emit a potentially under-claiming stamp.
     * A no-op until {@link #bindOwnOutputSource} is called (test-fixture instances and
     * TopologyTestDriver runs, which have no producer registry and therefore no pending sends).
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
     * The causal completeness clock: this node's own contiguous frontier, max-merged with every input
     * channel's advertised dependencies and with the {@link #carriedAncestry} re-homed from any
     * coordinates that have left this node's scope (a retired channel's history must keep riding in
     * the stamp — I9's "never dropped, only re-homed"). This is the <em>outbound stamp</em> — the
     * boundary this node
     * advertises downstream, carrying transitive ancestry (coordinates a channel has advertised that
     * this node may not itself have delivered yet) for each receiver's own gate to verify locally. It
     * is <em>not</em> the delivery gate: the gate ({@link ParsleyCausalBroadcast}) checks {@link #frontier()}
     * alone, so an advertised claim can never release a record here ahead of local delivery of its
     * cause. With no channel clocks recorded, this is exactly the node's own frontier.
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
     * forwarded, walks the longest contiguous run now achievable, advances the frontier, persists, and
     * only then prunes the forwarded-index entries the walk absorbed.
     *
     * <p>The persist-before-prune order matters: the frontier blob and the forwarded index are separate
     * changelog-backed stores with no cross-store atomicity, so a crash between them must always tear
     * toward "the frontier already reflects the absorbed run, but a since-redundant forwarded-index
     * entry below it still lingers" — harmless, purely cosmetic (the same degraded outcome the watermark
     * guard below prevents on the ordinary replay path) — never the reverse, where an entry is pruned but
     * the frontier advance that accounted for it was lost, which would permanently strand every offset
     * above it. This "always" is literal: {@link CausalTopology#assemble} requires {@code
     * processing.guarantee=exactly_once_v2} unconditionally, so the frontier write and the forwarded-index
     * prune are part of one Kafka transaction — a crash cannot tear one from the other at all, not merely
     * "usually toward" the benign side (see {@link ParsleyCausalBroadcast}'s class Javadoc for the fuller version
     * of this note).
     *
     * <p>A delivery at or below the current watermark ({@code offset <= frontier}) is an at-least-once
     * replay of an already-delivered offset — a no-op here too, and deliberately never marked: the absorb
     * walk below only ever scans strictly above the watermark, so a mark at or below it could never be
     * found and unmarked again, leaking a permanent, purely cosmetic entry in the changelog-backed
     * forwarded index (this used to happen on every such replay).
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
     * Bridges the offsets a {@code read_committed} consumer skipped between the previous highest received
     * offset on {@code (topicId, partition)} and {@code receivedOffset} — a transaction commit/abort marker
     * or an aborted-transaction data record, none of which the consumer ever returns — so the contiguous
     * walk can cross the hole they would otherwise wedge it at (a marker sits at a real offset that no
     * business record ever fills). The name is a coinage: no literature analog — the mechanism exists
     * only because Kafka's EOS commit markers occupy offsets. Called once per received record,
     * <em>before</em> that record's own
     * delivery, on every channel the causal-broadcast core advances a frontier on ({@link ParsleyCausalBroadcast#receive} and
     * {@link ParsleyGossip#receive}). Returns {@code true} if the contiguous frontier advanced — the
     * caller must then cascade ({@link ParsleyCausalBroadcast#propagate}), since a held record may have been waiting
     * on exactly a bridged offset.
     *
     * <p><strong>Soundness.</strong> Kafka delivers a partition strictly in offset order, so once
     * {@code receivedOffset} has arrived every lower offset that would ever be returned to this consumer
     * already has been. An offset in {@code (highestReceived, receivedOffset)} was therefore skipped
     * permanently — a transaction marker, an aborted record, or a protocol record the causal-broadcast core consumed but
     * deliberately did not record as a delivery (a marker on an early-return path) — never a
     * business record still in flight, and never a <em>held</em> business record (a held record was
     * received, so it is in the buffer and its offset is at or below {@code highestReceived}, outside the
     * bridged interval). None of these is a cause any dependent awaits, so folding them is safe. A marker carries no
     * dependencies, so bridging one advances no channel clock and forwards nothing — it only lets the walk
     * proceed. The interval is skipped, not the record at {@code receivedOffset} itself, which the caller
     * delivers or holds by the ordinary gate.
     *
     * <p><strong>First sighting.</strong> An absent {@code highestReceived} entry means the channel has
     * never been received on: the baseline below {@code receivedOffset} is {@link #seedIfFirstSeen}'s
     * concern (finite retention, a fresh consumer group), not a gap. So the first call bridges nothing,
     * records {@code receivedOffset} as the highest received, and returns {@code false}. A
     * {@code receivedOffset} at or below the recorded highest is an at-least-once replay: a strict no-op.
     *
     * <p>The <em>data-loss</em> guard — distinguishing a marker gap
     * from a retention/{@code deleteRecords} jump that would drop real committed records below the log-start
     * offset — is the caller's responsibility, since only it can see the partition's log-start (see
     * {@link ParsleyCausalBroadcast}); this method assumes the interval it is handed is a genuine skip.
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
     * Reconciles restored causal state with the currently declared input set — the scope-change step
     * run once at init, before the current input channels are seeded. Replaces the earlier
     * {@code pruneToScope}, whose outright dropping of out-of-scope entries under-claimed the stamp
     * and broke I2 at third parties (T3.0 A6). The one principle both directions share: <em>the
     * causal past a node has delivered or carried may be skipped, but never dropped and never
     * re-entered.</em> Four cases, diffed against the persisted {@link #declaredInputs}:
     *
     * <ol>
     *   <li><strong>Destroyed coordinates.</strong> A topic name declared both then and now whose
     *       UUID changed was deleted and recreated; the old UUID's entries can never be delivered by
     *       any receiver (E1: offsets rebind to different records), so they are the only entries
     *       removed outright — from the frontier, the channel keys and values, the highest-received
     *       map, and the carried ancestry. The new UUID starts as an added channel (below).</li>
     *   <li><strong>Out-of-scope re-homing (shrink, A6).</strong> Every other entry leaving scope —
     *       a removed input's frontier entry, a retired channel's full advertised clock, an
     *       out-of-scope entry inside a surviving channel's clock — max-merges into
     *       {@link #carriedAncestry} before it is pruned, so {@link #completeness()} (the stamp) is
     *       unchanged by the prune except at destroyed coordinates. Without this, a receiver
     *       downstream of this node could see an effect stamped as if its retired-channel cause never
     *       existed, and reorder them.</li>
     *   <li><strong>Added channels (growth, A5).</strong> An input declared now but not in the
     *       persisted set seeds its frontier at this node's carried-ancestry value for the
     *       coordinate — {@code completeness()} after the re-homing above — never at log-start:
     *       "skip what you already ignored". The prefix at or below what this node previously
     *       carried must never be delivered into its surviving state (an operator who wants that
     *       history performs a full reset); the forwarded index is pruned at or below the seed to
     *       match. A coordinate with no carried entry seeds nothing — a genuinely new topic's
     *       history has no delivered descendants here (I2), so replaying it is ordinary delivery,
     *       not reordering.</li>
     *   <li><strong>No persisted input set</strong> (a fresh store, or a blob from before this
     *       section existed): nothing to diff — no seeding, no destruction; the current set is
     *       simply recorded. Pre-release, no migration path (O6).</li>
     * </ol>
     *
     * <p>The current input set is persisted at the end, so the next init diffs against what this run
     * declared.
     *
     * <p>This method reconciles <em>clocks</em> only. The disposition of restored held
     * <em>records</em> whose source left scope — purge a destroyed incarnation's, fail init on a
     * removed-but-alive input's — lives in the L2 constructor
     * ({@code ParsleyCausalBroadcast}): L1 owns clock state, not the buffer.
     *
     * @param currentInputs the currently declared input topics, name → UUID
     * @param taskPartition the partition this task owns on every input (Streams co-partitions a
     *                      sub-topology's sources)
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
        // CoordKey would be re-serialised into the "f" blob on every persist forever.
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
     * Serialises the frontier clock, channel clocks, and highest-received offsets into the
     * single {@code "f"} value: {@code [frontier-len:4][frontier bytes][channel-count:4]} then per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][clock-len:4][clock bytes]}, then
     * {@code [highest-received-count:4]} and per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][offset:8]}. (T3.2 removed the epoch section
     * that used to sit between the channels and the highest-received offsets — a pre-T3.2 blob is not
     * readable, consistent with the pre-1.0 no-upgrade-path stance.) The
     * highest-received offsets are persisted so {@link #bridge}'s skip detection is exact across a restart
     * (the map is written inside the same EOS transaction as the frontier and forwarded index), rather than
     * reconstructed and possibly having to re-bridge already-forwarded offsets.
     *
     * <p>Two further trailing sections (both written together, both optional on read):
     * {@code [carried-ancestry-len:4][carried-ancestry bytes]} — the {@link #carriedAncestry} clock,
     * persisted because it is stamp-feeding state (I9: dropping it on restart would under-claim every
     * subsequent stamp) — then {@code [input-count:4]} with per input {@code [name UTF][topicId MSB:8]
     * [topicId LSB:8]} — the {@link #declaredInputs} set, which is what makes a scope change
     * detectable at the next init ({@link #rescope}).
     *
     * <p>Then one more optional trailing section (T2.2):
     * {@code [own-outputs-len:4][own-outputs bytes]} — the {@link #ownOutputs} clock. Best-effort
     * durability by design: acks arriving after this transaction's last persist are missing from
     * the committed blob (store caches flush before the producer flush completes acks — O1), so a
     * restored clock can trail by one transaction; the init-time sink end-offset seed re-covers it
     * (I8: both the trail and the seed only ever sit at or below a real appended offset).
     *
     * <p>Then one final optional trailing section (T3.4): {@code [sink-count:4]} with per sink
     * {@code [name UTF][topicId MSB:8][topicId LSB:8]} — the {@link #declaredSinks} set, which is
     * what lets the next init heal the trailing acks of a topic that is no longer a sink then
     * (see the field note).
     */
    private byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            ParsleyByteUtils.writeBytes(dos, frontier.toBytes());
            dos.writeInt(channels.size());
            for (Map.Entry<CoordKey, ParsleyVectorClock> entry : channels.entrySet()) {
                ParsleyByteUtils.writeUuid(dos, entry.getKey().topicId());
                dos.writeInt(entry.getKey().partition());
                ParsleyByteUtils.writeBytes(dos, entry.getValue().toBytes());
            }
            dos.writeInt(highestReceived.size());
            for (Map.Entry<CoordKey, Long> entry : highestReceived.entrySet()) {
                ParsleyByteUtils.writeUuid(dos, entry.getKey().topicId());
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
            }
            ParsleyByteUtils.writeBytes(dos, carriedAncestry.toBytes());
            dos.writeInt(declaredInputs.size());
            for (Map.Entry<String, Uuid> entry : declaredInputs.entrySet()) {
                dos.writeUTF(entry.getKey());
                ParsleyByteUtils.writeUuid(dos, entry.getValue());
            }
            ParsleyByteUtils.writeBytes(dos, ownOutputs.toBytes());
            dos.writeInt(declaredSinks.size());
            for (Map.Entry<String, Uuid> entry : declaredSinks.entrySet()) {
                dos.writeUTF(entry.getKey());
                ParsleyByteUtils.writeUuid(dos, entry.getValue());
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyChannels serialisation failed", e);
        }
    }

    private void load(byte[] blob) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(blob))) {
            frontier = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                Uuid topicId = ParsleyByteUtils.readUuid(dis);
                int partition = dis.readInt();
                ParsleyVectorClock clock = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
                channels.put(new CoordKey(topicId, partition), clock);
            }
            // The highest-received section is optional and trailing: a blob written before it existed
            // simply ends after the channels, leaving the map empty — every channel then reads as
            // a first sighting on its next record (no bridge, records the offset), which self-heals on the
            // following gap. A live blob carries the exact per-channel highest received. available() is
            // exact over the backing ByteArrayInputStream.
            if (dis.available() > 0) {
                int highestReceivedCount = dis.readInt();
                for (int i = 0; i < highestReceivedCount; i++) {
                    Uuid topicId = ParsleyByteUtils.readUuid(dis);
                    int partition = dis.readInt();
                    long offset = dis.readLong();
                    highestReceived.put(new CoordKey(topicId, partition), offset);
                }
            }
            // The carried-ancestry and declared-input sections are trailing and optional as one unit
            // (always written together since they exist): a pre-T1.3 blob simply ends before them,
            // loading an empty carried ancestry (nothing was ever re-homed) and an empty declared
            // input set (rescope then has nothing to diff and just records the current declaration).
            if (dis.available() > 0) {
                carriedAncestry = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
                int inputCount = dis.readInt();
                for (int i = 0; i < inputCount; i++) {
                    String name = dis.readUTF();
                    declaredInputs.put(name, ParsleyByteUtils.readUuid(dis));
                }
            }
            // The own-outputs section is trailing and optional (T2.2): a pre-T2.2 blob ends before
            // it, loading an empty clock — under-stated but never wrong, and the init-time sink
            // end-offset seed immediately re-covers it (the same heal as the blob trailing the
            // last transaction's acks; both are I8-sound directions).
            if (dis.available() > 0) {
                ownOutputs = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            }
            // The declared-sink section is trailing and optional (T3.4): a pre-T3.4 blob ends
            // before it, loading an empty set — the init-time heal then has nothing to heal, which
            // matches the pre-1.0 no-upgrade-path stance (O6).
            if (dis.available() > 0) {
                int sinkCount = dis.readInt();
                for (int i = 0; i < sinkCount; i++) {
                    String name = dis.readUTF();
                    declaredSinks.put(name, ParsleyByteUtils.readUuid(dis));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyChannels deserialisation failed", e);
        }
    }

    private record CoordKey(Uuid topicId, int partition) {}

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
         * unacknowledged; empty means full quiescence. Throws {@link ParsleyPendingAckException}
         * on timeout or on an acknowledgement failure observed while waiting (T3.0 A8) — never
         * returns normally without genuine quiescence.
         */
        void awaitQuiescentExcept(Set<TopicPartition> exceptDestinations, long timeoutMs);
    }
}