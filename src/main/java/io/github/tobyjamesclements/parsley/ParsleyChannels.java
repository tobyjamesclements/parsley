package io.github.tobyjamesclements.parsley;

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
 *                                                     L2 evaluates it: self-cycle strip (I5), plus
 *                                                     the interim below-floor strip (until T3.2)
 *             acknowledge(topic, partition, offset)   producer ack → ownOutputs (D2; stub until
 *                                                     Phase 2)
 * queries:    frontier() → ParsleyVectorClock         the delivered vector VT(p)
 *             ownOutputs() → ParsleyVectorClock       the node's own acked output positions (D2;
 *                                                     empty until Phase 2)
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
 *       (see {@link ParsleyEngine}); an advertised claim never substitutes for local delivery; and
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
 * accessors. {@link ParsleyEngine} enforces causal transitivity (the cascade after each delivery) and
 * owns the buffer around these operations.
 */
final class ParsleyChannels {

    private ParsleyVectorClock frontier;
    // Per input channel (topicId, partition) -> the dependencies advertised on it (max-merged).
    private final Map<CoordKey, ParsleyVectorClock> channels = new HashMap<>();
    // Per input channel (topicId, partition) -> the highest offset ever physically received on it. Persisted
    // in the "f" blob (part of the EOS transaction, so exact across restart) and consulted by bridge(): the
    // open interval between the previous highest and a newly-received offset was skipped by the
    // read_committed consumer, so it can only hold transaction markers or aborted-transaction records, never
    // an in-flight or held business record. An absent entry means the channel has never been received on —
    // its baseline is seedIfFirstSeen's concern, not a gap to bridge.
    private final Map<CoordKey, Long> highestReceived = new HashMap<>();
    // Coordinates observed at least once; guards the one-time baseline seed in seedIfFirstSeen.
    private final Set<CoordKey> seenCoordinates = new HashSet<>();
    private final ParsleyForwardedIndex forwardedIndex;
    // The topology epoch's lower bounds — the per-coordinate floor consulted on every clock this
    // frontier builds or merges (deliver, seedIfFirstSeen, completeness, channelUpdate), so no causal
    // clock ever carries an entry below the floor. NONE (every coordinate unbounded) disables it, so
    // the floor is a no-op and behaviour matches epoch 0.
    private final ParsleyEpoch epoch;
    // The frontier state store, holding this frontier+channels blob at key "f"; null for in-memory
    // (test) instances, which skip persistence.
    private final @Nullable KeyValueStore<String, byte[]> store;
    // When false, channel clocks are not tracked: channelUpdate is a no-op and completeness() is the
    // node's own frontier (single-layer, frontier-only gating). Used to exercise the frontier/buffer
    // mechanics in isolation, without the cross-channel completeness layer.
    private final boolean trackChannels;

    /**
     * In-memory instance that tracks channel clocks: starts from {@code initial} with no channels and
     * no persistence. Used by tests exercising {@link #completeness()} and any caller that does not
     * need a durable frontier.
     */
    ParsleyChannels(ParsleyVectorClock initial, ParsleyForwardedIndex forwardedIndex) {
        this(initial, forwardedIndex, true, ParsleyEpoch.NONE);
    }

    /**
     * In-memory instance with channel tracking optionally disabled and an explicit epoch floor. With
     * {@code trackChannels = false}, {@link #completeness()} is the node's own frontier and
     * {@link #channelUpdate} is a no-op.
     */
    ParsleyChannels(ParsleyVectorClock initial, ParsleyForwardedIndex forwardedIndex,
                    boolean trackChannels, ParsleyEpoch epoch) {
        this.frontier = initial;
        this.forwardedIndex = forwardedIndex;
        this.epoch = epoch;
        this.store = null;
        this.trackChannels = trackChannels;
    }

    /**
     * Durable instance with no epoch floor ({@link ParsleyEpoch#NONE}): loads the frontier clock and
     * channel clocks from key {@code "f"} of {@code store} (empty if absent), and rewrites that single
     * value on every subsequent change.
     */
    ParsleyChannels(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex) {
        this(store, forwardedIndex, ParsleyEpoch.NONE);
    }

    /**
     * Durable instance with an explicit epoch floor: loads the frontier clock and channel clocks from
     * key {@code "f"} of {@code store} (empty if absent), and rewrites that single value on every
     * subsequent change.
     *
     * <p>On a restored (non-empty) load, also sweeps {@code forwardedIndex} once for every coordinate
     * the restored frontier carries, deleting any marked offset at or below that coordinate's watermark
     * — a stale entry that leaked below the contiguous frontier (e.g. via the benign tear direction
     * {@link #delivered}'s Javadoc describes, now closed off by the {@code exactly_once_v2} requirement,
     * but still possible in a store carried over from before that requirement existed) can never be
     * reached by {@link #delivered}'s absorb walk again, so it would otherwise linger in the
     * changelog-backed store forever. A one-shot pass at load, not on the hot delivery path.
     */
    ParsleyChannels(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex, ParsleyEpoch epoch) {
        this.store = store;
        this.forwardedIndex = forwardedIndex;
        this.epoch = epoch;
        this.trackChannels = true;
        byte[] blob = store.get(ParsleyStores.FRONTIER_KEY);
        this.frontier = ParsleyVectorClock.empty();
        if (blob != null) {
            load(blob);
            frontier.forEach((topicId, partition, offset) -> forwardedIndex.pruneAtOrBelow(topicId, partition, offset));
        }
    }

    /** The topology epoch's lower bounds this frontier floors against; {@link ParsleyEpoch#NONE} if unbounded. */
    ParsleyEpoch epoch() {
        return epoch;
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
     * evaluates (I5 — after this step, no clock inside L2 carries a self-reference). Two steps:
     * <ol>
     *   <li><strong>Self-cycle removal.</strong> A record depending on its own {@code (topicId,
     *       partition, offset)} has, by being delivered, met that dependency, so it must not wait on
     *       itself (this keeps a self-referential stamp on a fused chain from deadlocking). Only the
     *       exact self-coordinate is removed; a backward same-partition dependency ({@code required
     *       < offset}) is an intra-topic dependency like any other and flows through unchanged.</li>
     *   <li><strong>Below-floor strip (interim — deleted with the floors in T3.2, per D4).</strong>
     *       Any dependency below its coordinate's topology-epoch {@code startsAt} bound is dropped
     *       ({@link ParsleyVectorClock#strippedBelow}) — an out-of-domain reference to a prior,
     *       closed epoch that no channel in this epoch will ever confirm. A no-op under
     *       {@link ParsleyEpoch#NONE}.</li>
     * </ol>
     *
     * <p>View-only: never rewrites recorded state or the outbound stamp ({@link #completeness()} is
     * computed separately). The gate re-normalises on every evaluation rather than caching the result
     * at receive time, because the epoch floor can rise while a record is held (a closing epoch
     * transition strips a held record's below-floor dependencies — the drain {@link ParsleyEngine}
     * runs after {@link #tryAdvanceEpoch} depends on that re-evaluation); once T3.2 removes the
     * floor clause, the result is a pure function of the raw clock and the source coordinate.
     */
    ParsleyVectorClock normalize(ParsleyVectorClock rawDeps, Uuid sourceTopicId, int sourcePartition, long sourceOffset) {
        ParsleyVectorClock deps = rawDeps.offsetFor(sourceTopicId, sourcePartition) == sourceOffset
                ? rawDeps.without(sourceTopicId, sourcePartition)
                : rawDeps;
        return deps.strippedBelow(epoch);
    }

    /**
     * The <em>acknowledge</em> request: the producer acked this node's own send to sink coordinate
     * {@code (topic, partition)} at {@code offset}. Feeds the {@link #ownOutputs()} clock — D2's
     * own-slot tracking. A stub until Phase 2 (T2.2) wires the producer-interceptor registry in;
     * declared now so the module's API is complete from the start.
     */
    void acknowledge(String topic, int partition, long offset) {
        // Stub until Phase 2 (T2.2): acked output positions will fold into the ownOutputs clock.
    }

    /**
     * The node's own acked output positions per sink coordinate — the clock that recovers CBCAST's
     * own-slot semantics (D2: the broker performs the sender's clock increment, learned via
     * {@link #acknowledge}). Empty until Phase 2 (T2.2) lands the tracking; stamp-side only, never
     * consulted by the delivery gate.
     */
    ParsleyVectorClock ownOutputs() {
        return ParsleyVectorClock.empty();
    }

    /**
     * The causal completeness clock: this node's own contiguous frontier, max-merged with every input
     * channel's advertised dependencies. This is the <em>outbound stamp</em> — the boundary this node
     * advertises downstream, carrying transitive ancestry (coordinates a channel has advertised that
     * this node may not itself have delivered yet) for each receiver's own gate to verify locally. It
     * is <em>not</em> the delivery gate: the gate ({@link ParsleyEngine}) checks {@link #frontier()}
     * alone, so an advertised claim can never release a record here ahead of local delivery of its
     * cause. With no channel clocks recorded, this is exactly the node's own frontier.
     *
     * <p>This is the delivered frontier and the outbound stamp, carried as-is — <em>not</em> floored to
     * the epoch. The epoch transition is invisible in the data plane: each node floors <em>incoming</em>
     * dependencies against its own epoch locally (the gate and the below-floor {@link #delivered}/
     * {@link #seedIfFirstSeen}), so the stamp only ever carries positions this node actually delivered,
     * and a below-floor origin a downstream might see is floored out by that downstream's own gate.
     */
    ParsleyVectorClock completeness() {
        ParsleyVectorClock result = frontier;
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
     * "usually toward" the benign side (see {@link ParsleyEngine}'s class Javadoc for the fuller version
     * of this note).
     *
     * <p>A <em>below-floor</em> delivery ({@code offset < startsAt}) is a no-op on the causal frontier:
     * the record still feeds state and is forwarded by the engine, but an out-of-domain offset must not
     * advance the causal frontier (it stays at the epoch origin until an in-domain offset is delivered)
     * nor enter the forwarded index. Under {@link ParsleyEpoch#NONE} every offset is in-domain.
     *
     * <p>A delivery at or below the current watermark ({@code offset <= frontier}) is an at-least-once
     * replay of an already-delivered offset — a no-op here too, and deliberately never marked: the absorb
     * walk below only ever scans strictly above the watermark, so a mark at or below it could never be
     * found and unmarked again, leaking a permanent, purely cosmetic entry in the changelog-backed
     * forwarded index (this used to happen on every such replay).
     */
    void delivered(Uuid topicId, int partition, long offset) {
        if (offset < epoch.startsAt(topicId, partition)) {
            return;
        }
        long watermark = frontier.offsetFor(topicId, partition);
        if (offset <= watermark) {
            return;
        }
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
     * delivery, on every channel the engine advances a frontier on ({@link ParsleyEngine#receive} and
     * {@link ParsleyEngine#onWatermark}). Returns {@code true} if the contiguous frontier advanced — the
     * caller must then cascade ({@link ParsleyEngine#propagate}), since a held record may have been waiting
     * on exactly a bridged offset.
     *
     * <p><strong>Soundness.</strong> Kafka delivers a partition strictly in offset order, so once
     * {@code receivedOffset} has arrived every lower offset that would ever be returned to this consumer
     * already has been. An offset in {@code (highestReceived, receivedOffset)} was therefore skipped
     * permanently — a transaction marker, an aborted record, or a protocol record the engine consumed but
     * deliberately did not record as a delivery (a boundary marker on an early-return path) — never a
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
     * <p>The epoch floor is honoured exactly as {@link #delivered} honours it: a skipped offset below the
     * coordinate's {@code startsAt} is not marked (an out-of-domain position must not enter the forwarded
     * index or advance the causal frontier). The <em>data-loss</em> guard — distinguishing a marker gap
     * from a retention/{@code deleteRecords} jump that would drop real committed records below the log-start
     * offset — is the caller's responsibility, since only it can see the partition's log-start (see
     * {@link ParsleyEngine}); this method assumes the interval it is handed is a genuine skip.
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
        long startsAt = epoch.startsAt(topicId, partition);
        // Fast path: the frontier is caught up to the previous highest received (watermark == prev), so no
        // held record sits in the skipped run — it is contiguous from the frontier and, being all
        // consumer-skipped, all markers. Fold it straight into the frontier without O(gap) forwarded-index
        // writes; a large aborted transaction could otherwise mark hundreds of thousands of offsets inside
        // one EOS transaction, risking the transaction timeout and a crash-loop on legal input. The
        // in-domain guard (prev + 1 >= startsAt) keeps a below-floor offset out of the frontier — and holds
        // whenever watermark == prev, since a seeded coordinate's watermark never sits below startsAt - 1.
        if (watermark == prev && prev + 1 >= startsAt) {
            frontier = frontier.observe(topicId, partition, receivedOffset - 1);
            highestReceived.put(key, receivedOffset);
            persist();
            return frontier.offsetFor(topicId, partition) > watermark;
        }
        // Slow path: a held record may sit between the frontier and the previous highest received, so mark
        // the skipped offsets and let the contiguous walk absorb only the run now reachable (it stops at the
        // held record). Below-floor and at-or-below-watermark offsets are not marked, mirroring deliver().
        for (long skipped = prev + 1; skipped < receivedOffset; skipped++) {
            if (skipped < startsAt || skipped <= watermark) {
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
     * Establishes the contiguous frontier's starting point the first time this coordinate is observed,
     * floored to the epoch origin. The name is a coinage: no literature analog — the mechanism exists
     * only because Kafka retention means a channel's history need not begin at its first offset.
     * The first offset seen need not be 0 (finite retention, fresh consumer
     * group); anything below it is outside the engine's purview, not an unfillable gap, so folding
     * {@code offset - 1} into the frontier lets the contiguous walk start there. Returns {@code true} if
     * a seed was applied (the caller should then cascade). The coordinate is marked seen on the first
     * call even if the record is held, so a later record cannot re-trigger the seed and skip the
     * still-held earlier one. The seen-set is in-memory only; {@link ParsleyEngine}'s constructor
     * replays this call for every restored held record's source coordinate (at its lowest held
     * offset) so the guard survives a restart.
     *
     * <p>The epoch generalises the per-channel origin into a domain-wide one: the causal frontier for a
     * coordinate begins at the <em>epoch origin</em> {@code startsAt - 1} (nothing below the floor is
     * in-domain), so the seed target is {@code max(firstOffset - 1, startsAt - 1)}. A below-floor
     * first sighting therefore still anchors the frontier at the origin (not below it), and a restored
     * value sitting below the origin is lifted to it. Under {@link ParsleyEpoch#NONE} the origin is
     * {@code -1}, so this reduces exactly to the original "seed to {@code offset - 1} only when the
     * coordinate is unrecorded" behaviour.
     */
    boolean seedIfFirstSeen(Uuid topicId, int partition, long offset) {
        if (!seenCoordinates.add(new CoordKey(topicId, partition))) return false;
        long startsAt = epoch.startsAt(topicId, partition);
        // The epoch origin: startsAt - 1, or -1 (absent) with no epoch floor. Guarded against the
        // NO_BOUND (Long.MIN_VALUE) underflow.
        long floorOrigin = startsAt > 0 ? startsAt - 1 : -1L;
        long current = frontier.offsetFor(topicId, partition);
        // An unrecorded coordinate folds everything below its first offset into the frontier; a recorded
        // one is left as-is. Either way, never below the epoch origin.
        long seedTo = Math.max(current < 0 ? offset - 1 : current, floorOrigin);
        if (seedTo < 0 || seedTo <= current) return false;
        frontier = frontier.observe(topicId, partition, seedTo);
        persist();
        return true;
    }

    /** The clock advertised on channel {@code (topicId, partition)}, or empty if never updated. */
    ParsleyVectorClock channelGet(Uuid topicId, int partition) {
        ParsleyVectorClock clock = channels.get(new CoordKey(topicId, partition));
        return clock == null ? ParsleyVectorClock.empty() : clock;
    }

    /**
     * Max-merges {@code clock} into channel {@code (topicId, partition)}'s advertised dependencies
     * (monotonic: the stored clock never decreases) and persists. A first call for a channel
     * initialises it from {@code clock}. Channel clocks are <em>not</em> floored here: a below-floor
     * position a channel advertises is harmless — a dependency on it is stripped at the gate (against
     * the effective floor), so it never gates anything, and completeness carries it only as the
     * unfloored delivered frontier the stamp advertises.
     *
     * <p>A channel's entry no longer holds {@link #completeness()} down to an intersection minimum —
     * {@link #completeness()} is a plain max-merge now, so a channel with nothing advertised simply
     * contributes nothing rather than excluding a coordinate every other channel has confirmed. What a
     * seeded-but-silent channel entry still does is give {@link #tryAdvanceEpoch}'s per-channel
     * marker-seen bookkeeping and {@link #pruneToScope} something to check against.
     */
    void channelUpdate(Uuid topicId, int partition, ParsleyVectorClock clock) {
        if (!trackChannels) {
            return;
        }
        CoordKey key = new CoordKey(topicId, partition);
        ParsleyVectorClock existing = channels.get(key);
        channels.put(key, existing == null ? clock : existing.merge(clock));
        persist();
    }

    /**
     * Records an epoch-boundary marker received on channel {@code (channelTopicId, channelPartition)},
     * persisting when it changed the state. Returns {@code true} if the marker was newly recorded — a
     * fresh pending transition or a channel not yet seen for the current one (see
     * {@link ParsleyEpochState#onBoundary}); the relaying caller forwards the marker downstream only on
     * {@code true}. A no-op returning {@code false} unless this frontier's epoch is a live
     * {@link ParsleyEpochState} (tests and the epoch-0 default hold a static {@link ParsleyEpoch}). See
     * {@link #tryAdvanceEpoch}.
     */
    boolean recordEpochMarker(long epochId, ParsleyVectorClock lowerBounds, Uuid channelTopicId, int channelPartition) {
        if (epoch instanceof ParsleyEpochState state) {
            boolean newlyRecorded = state.onBoundary(epochId, lowerBounds, channelTopicId, channelPartition);
            if (newlyRecorded) {
                persist();
            }
            return newlyRecorded;
        }
        return false;
    }

    /**
     * Closes an in-progress epoch transition if it is ready — the boundary marker has been received on
     * <em>every</em> input channel and this node's own contiguous frontier ({@link #frontier()})
     * dominates the pending floor {@code F_e}, restricted to the coordinates this node can ever observe
     * — promoting {@code F_e} to the settled floor and persisting. Returns {@code true} if it advanced
     * (the caller should then re-drain, since a raised floor can strip a held replay record's
     * below-floor dependencies). A no-op with no live {@link ParsleyEpochState} or no ready transition.
     * Called after every engine operation that can advance the frontier.
     *
     * <p>The dominance check deliberately uses the frontier, not {@link #completeness()}: the window
     * closing is the proof that everything below {@code F_e} has been delivered <em>here</em>, and a
     * channel's advertised claim that a peer delivered it is no such proof — closing on completeness
     * would let the raised floor strip a held e-1 record's dependencies before this node had actually
     * delivered them, releasing it out of causal order (the same hearsay hole the delivery gate
     * closes; see {@link ParsleyEngine#isDeliverable}).
     *
     * <p>{@code F_e} is the DAG-wide committed floor — the {@code mergeMin} of every member's published
     * completeness — so it can carry coordinates for topics downstream of (or parallel to) this node
     * that this node never channels at all (its own {@code completeness()} can never contain them: a
     * dependency clock only ever spans a node's own input channels). Comparing the unfiltered floor
     * against this node's completeness would therefore never dominate at any non-terminal stage. When
     * channel tracking is on (the production case; see {@link #trackChannels}), the floor is filtered
     * here to {@link #channels}' own coordinates — the same scoping {@link #pruneToScope} already
     * applies to the frontier — before the dominance check. A coordinate that <em>is</em> in scope but
     * not yet delivered here is deliberately left in the filtered floor rather than dropped:
     * {@link ParsleyVectorClock#dominates} then reads it as unsatisfied (an absent coordinate is never
     * dominated), so the window correctly keeps holding until this node catches up — conservative,
     * not permissive, exactly mirroring "hold over guess" for causal safety. With channel tracking off
     * (the single-layer, frontier-only test mode, where {@link #channels} is permanently empty) there
     * is no channel concept to scope by, so the floor is left unfiltered — scoping to an always-empty
     * key set would strip every coordinate and vacuously close every window.
     */
    boolean tryAdvanceEpoch() {
        if (!(epoch instanceof ParsleyEpochState state)) {
            return false;
        }
        ParsleyVectorClock pendingFloor = state.pendingFloor();
        if (pendingFloor == null) {
            return false;
        }
        for (CoordKey key : channels.keySet()) {
            if (!state.hasMarker(key.topicId(), key.partition())) {
                return false;
            }
        }
        ParsleyVectorClock scopedFloor = trackChannels
                ? pendingFloor.retaining((topicId, partition) -> channels.containsKey(new CoordKey(topicId, partition)))
                : pendingFloor;
        if (!frontier().dominates(scopedFloor)) {
            return false;
        }
        state.promote();
        persist();
        return true;
    }

    /**
     * Prunes causal state to the coordinates {@code inScope} accepts: retains the frontier clock,
     * drops any channel whose coordinate is out of scope (e.g. a topic dropped and recreated with a
     * new UUID), and prunes each surviving channel's <em>advertised clock</em> to the same scope.
     * Called once at init before seeding the current input channels.
     *
     * <p>Pruning the channel <em>values</em> is what lets a coordinate ever be retired from the DAG.
     * A channel's advertised clock is monotonic ({@link #channelUpdate} only ever max-merges) and
     * feeds {@link #completeness()} — the outbound stamp — so an entry for a topic that has left the
     * topology would otherwise be re-advertised downstream forever, where every receiver's gate fails
     * it fast as an unreachable dependency (a permanent crash loop regenerated from this store on
     * every restart). The prune is safe because a live advertised coordinate is always in scope here:
     * every stage channels its whole causal ancestry (the full-mesh/ancestry contract the engine's
     * fail-closed unreachable check enforces), upstream stamps are co-partitioned onto this task's
     * own partition, and this node's own sink coordinates were already stripped before folding
     * ({@code ParsleyEngine#advertised}) — so an out-of-scope entry inside a channel clock can only
     * be a retired or recreated coordinate, never live transitive ancestry.
     */
    void pruneToScope(ParsleyVectorClock.CoordinatePredicate inScope) {
        frontier = frontier.retaining(inScope);
        channels.keySet().removeIf(key -> !inScope.test(key.topicId(), key.partition()));
        channels.replaceAll((key, clock) -> clock.retaining(inScope));
        // A retired/recreated coordinate must also leave the highest-received map, or its dead CoordKey
        // would be re-serialised into the "f" blob on every persist forever. No misapplication is possible
        // (a recreated topic has a new UUID, hence a new key), but the leak contradicts the scope prune.
        highestReceived.keySet().removeIf(key -> !inScope.test(key.topicId(), key.partition()));
        persist();
    }

    private void persist() {
        if (store != null) {
            store.put(ParsleyStores.FRONTIER_KEY, toBytes());
        }
    }

    /**
     * Serialises the frontier clock, channel clocks, epoch state, and highest-received offsets into the
     * single {@code "f"} value: {@code [frontier-len:4][frontier bytes][channel-count:4]} then per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][clock-len:4][clock bytes]}, then
     * {@code [epoch-present:1]} and, when the epoch is a live {@link ParsleyEpochState},
     * {@code [epoch-len:4][epoch bytes]}, then {@code [highest-received-count:4]} and per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][offset:8]}. The epoch state is persisted so a
     * mid-transition restart — which resumes past the already-consumed boundary marker — resumes the
     * pending window rather than losing it (which would leave the transition unable to close). The
     * highest-received offsets are persisted so {@link #bridge}'s skip detection is exact across a restart
     * (the map is written inside the same EOS transaction as the frontier and forwarded index), rather than
     * reconstructed and possibly having to re-bridge already-forwarded offsets.
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
            if (epoch instanceof ParsleyEpochState state) {
                dos.writeBoolean(true);
                ParsleyByteUtils.writeBytes(dos, state.toBytes());
            } else {
                dos.writeBoolean(false);
            }
            dos.writeInt(highestReceived.size());
            for (Map.Entry<CoordKey, Long> entry : highestReceived.entrySet()) {
                ParsleyByteUtils.writeUuid(dos, entry.getKey().topicId());
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
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
            // The epoch section is optional and trailing: a blob written before epoch state existed (or
            // by a static-epoch frontier) simply ends after the channels. available() is exact over the
            // backing ByteArrayInputStream.
            if (dis.available() > 0 && dis.readBoolean()) {
                byte[] e = ParsleyByteUtils.readBytes(dis);
                // Only a live ParsleyEpochState can restore epoch bytes; a static epoch (tests/epoch 0)
                // never wrote them, so this branch is not reached for those.
                if (epoch instanceof ParsleyEpochState state) {
                    state.restore(e);
                }
            }
            // The highest-received section is likewise optional and trailing: a blob written before it
            // existed simply ends after the epoch flag, leaving the map empty — every channel then reads as
            // a first sighting on its next record (no bridge, records the offset), which self-heals on the
            // following gap. A live blob carries the exact per-channel highest received.
            if (dis.available() > 0) {
                int highestReceivedCount = dis.readInt();
                for (int i = 0; i < highestReceivedCount; i++) {
                    Uuid topicId = ParsleyByteUtils.readUuid(dis);
                    int partition = dis.readInt();
                    long offset = dis.readLong();
                    highestReceived.put(new CoordKey(topicId, partition), offset);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyChannels deserialisation failed", e);
        }
    }

    private record CoordKey(Uuid topicId, int partition) {}
}