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
 * The causal state of a {@link ParsleyEngine}: the contiguous frontier clock, the per-input-channel
 * clocks, and the seeding/forwarding infrastructure that maintains the frontier — the single owner
 * of all causal metadata a node persists (the held-record buffer and its candidate index are a
 * separate concern).
 *
 * <p>Two structures fold into one durable value here, stored as a single {@code "f"} key-value pair
 * in the frontier state store (loaded once at construction, rewritten on change, read from memory):
 * <ul>
 *   <li>the <strong>contiguous frontier clock</strong> — the highest offset delivered without a gap
 *       on each coordinate this node consumes; and
 *   <li>the <strong>channel clocks</strong> — for each input channel {@code (topicId, partition)},
 *       the dependencies advertised on it (max-merged). {@link #completeness()} is the frontier clock
 *       max-merged with every channel's advertised view — the <em>outbound stamp</em>, carrying
 *       transitive ancestry downstream. The delivery gate itself checks the contiguous frontier alone
 *       (see {@link ParsleyEngine}); an advertised claim never substitutes for local delivery.
 * </ul>
 *
 * <p>The <strong>forwarded-offset index</strong> is <em>not</em> in the {@code "f"} blob: it is a
 * growable, order-sensitive set (offsets delivered above the contiguous frontier) with incremental
 * per-offset writes and range reads, so it keeps its own keyed store, injected here as a collaborator.
 *
 * <p>Core operations: {@link #completeness()} (the delivery boundary), {@link #deliver} (advance the
 * contiguous frontier for a delivered record), {@link #seedIfFirstSeen} (establish the baseline the
 * first time a coordinate is observed, since consumption need not start at offset 0), and the channel
 * accessors. {@link ParsleyEngine} enforces causal transitivity (the cascade after each delivery) and
 * owns the buffer around these operations.
 */
final class ParsleyFrontier {

    private ParsleyClock frontier;
    // Per input channel (topicId, partition) -> the dependencies advertised on it (max-merged).
    private final Map<CoordKey, ParsleyClock> channels = new HashMap<>();
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
    ParsleyFrontier(ParsleyClock initial, ParsleyForwardedIndex forwardedIndex) {
        this(initial, forwardedIndex, true, ParsleyEpoch.NONE);
    }

    /**
     * In-memory instance with channel tracking optionally disabled and an explicit epoch floor. With
     * {@code trackChannels = false}, {@link #completeness()} is the node's own frontier and
     * {@link #channelUpdate} is a no-op.
     */
    ParsleyFrontier(ParsleyClock initial, ParsleyForwardedIndex forwardedIndex,
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
    ParsleyFrontier(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex) {
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
     * {@link #deliver}'s Javadoc describes, now closed off by the {@code exactly_once_v2} requirement,
     * but still possible in a store carried over from before that requirement existed) can never be
     * reached by {@link #deliver}'s absorb walk again, so it would otherwise linger in the
     * changelog-backed store forever. A one-shot pass at load, not on the hot delivery path.
     */
    ParsleyFrontier(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex, ParsleyEpoch epoch) {
        this.store = store;
        this.forwardedIndex = forwardedIndex;
        this.epoch = epoch;
        this.trackChannels = true;
        byte[] blob = store.get(ParsleyStores.FRONTIER_KEY);
        this.frontier = ParsleyClock.empty();
        if (blob != null) {
            load(blob);
            frontier.forEach((topicId, partition, offset) -> forwardedIndex.pruneAtOrBelow(topicId, partition, offset));
        }
    }

    /** The topology epoch's lower bounds this frontier floors against; {@link ParsleyEpoch#NONE} if unbounded. */
    ParsleyEpoch epoch() {
        return epoch;
    }

    /** The current contiguous frontier clock. */
    ParsleyClock snapshot() {
        return frontier;
    }

    /**
     * The causal completeness clock: this node's own contiguous frontier, max-merged with every input
     * channel's advertised dependencies. This is the <em>outbound stamp</em> — the boundary this node
     * advertises downstream, carrying transitive ancestry (coordinates a channel has advertised that
     * this node may not itself have delivered yet) for each receiver's own gate to verify locally. It
     * is <em>not</em> the delivery gate: the gate ({@link ParsleyEngine}) checks {@link #snapshot()}
     * alone, so an advertised claim can never release a record here ahead of local delivery of its
     * cause. With no channel clocks recorded, this is exactly the node's own frontier.
     *
     * <p>This is the delivered frontier and the outbound stamp, carried as-is — <em>not</em> floored to
     * the epoch. The epoch transition is invisible in the data plane: each node floors <em>incoming</em>
     * dependencies against its own epoch locally (the gate and the below-floor {@link #deliver}/
     * {@link #seedIfFirstSeen}), so the stamp only ever carries positions this node actually delivered,
     * and a below-floor origin a downstream might see is floored out by that downstream's own gate.
     */
    ParsleyClock completeness() {
        ParsleyClock result = frontier;
        for (ParsleyClock advertised : channels.values()) {
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
    void deliver(Uuid topicId, int partition, long offset) {
        if (offset < epoch.startsAt(topicId, partition)) {
            return;
        }
        long watermark = frontier.offsetFor(topicId, partition);
        if (offset <= watermark) {
            return;
        }
        forwardedIndex.mark(topicId, partition, offset);
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
     * floored to the epoch origin. The first offset seen need not be 0 (finite retention, fresh consumer
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
    ParsleyClock channelGet(Uuid topicId, int partition) {
        ParsleyClock clock = channels.get(new CoordKey(topicId, partition));
        return clock == null ? ParsleyClock.empty() : clock;
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
    void channelUpdate(Uuid topicId, int partition, ParsleyClock clock) {
        if (!trackChannels) {
            return;
        }
        CoordKey key = new CoordKey(topicId, partition);
        ParsleyClock existing = channels.get(key);
        channels.put(key, existing == null ? clock : existing.merge(clock));
        persist();
    }

    /**
     * Records an epoch-boundary marker received on channel {@code (channelTopicId, channelPartition)}
     * and persists. A no-op unless this frontier's epoch is a live {@link ParsleyEpochState} (tests and
     * the epoch-0 default hold a static {@link ParsleyEpoch}). See {@link #tryAdvanceEpoch}.
     */
    void recordEpochMarker(long epochId, ParsleyClock lowerBounds, Uuid channelTopicId, int channelPartition) {
        if (epoch instanceof ParsleyEpochState state) {
            state.onBoundary(epochId, lowerBounds, channelTopicId, channelPartition);
            persist();
        }
    }

    /**
     * Closes an in-progress epoch transition if it is ready — the boundary marker has been received on
     * <em>every</em> input channel and this node's own contiguous frontier ({@link #snapshot()})
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
     * {@link ParsleyClock#dominates} then reads it as unsatisfied (an absent coordinate is never
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
        ParsleyClock pendingFloor = state.pendingFloor();
        if (pendingFloor == null) {
            return false;
        }
        for (CoordKey key : channels.keySet()) {
            if (!state.hasMarker(key.topicId(), key.partition())) {
                return false;
            }
        }
        ParsleyClock scopedFloor = trackChannels
                ? pendingFloor.retaining((topicId, partition) -> channels.containsKey(new CoordKey(topicId, partition)))
                : pendingFloor;
        if (!snapshot().dominates(scopedFloor)) {
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
    void pruneToScope(ParsleyClock.CoordinatePredicate inScope) {
        frontier = frontier.retaining(inScope);
        channels.keySet().removeIf(key -> !inScope.test(key.topicId(), key.partition()));
        channels.replaceAll((key, clock) -> clock.retaining(inScope));
        persist();
    }

    private void persist() {
        if (store != null) {
            store.put(ParsleyStores.FRONTIER_KEY, toBytes());
        }
    }

    /**
     * Serialises the frontier clock, channel clocks, and epoch state into the single {@code "f"} value:
     * {@code [frontier-len:4][frontier bytes][channel-count:4]} then per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][clock-len:4][clock bytes]}, then
     * {@code [epoch-present:1]} and, when the epoch is a live {@link ParsleyEpochState},
     * {@code [epoch-len:4][epoch bytes]}. The epoch state is persisted so a mid-transition restart —
     * which resumes past the already-consumed boundary marker — resumes the pending window rather than
     * losing it (which would leave the transition unable to close).
     */
    private byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            ParsleyByteUtils.writeBytes(dos, frontier.toBytes());
            dos.writeInt(channels.size());
            for (Map.Entry<CoordKey, ParsleyClock> entry : channels.entrySet()) {
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
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyFrontier serialisation failed", e);
        }
    }

    private void load(byte[] blob) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(blob))) {
            frontier = ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                Uuid topicId = ParsleyByteUtils.readUuid(dis);
                int partition = dis.readInt();
                ParsleyClock clock = ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis));
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
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyFrontier deserialisation failed", e);
        }
    }

    private record CoordKey(Uuid topicId, int partition) {}
}