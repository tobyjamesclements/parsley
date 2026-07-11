package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The parent epoch state a task holds over its {@link ParsleyFrontier} — a State (GoF): the settled
 * per-coordinate floor, plus an optional in-progress epoch transition. Implements {@link ParsleyEpoch} so
 * the frontier and the delivery gate floor against the settled floor directly ({@link #startsAt}).
 *
 * <p><strong>Overlapping-epoch transition (the reason this is stateful).</strong> A message must be
 * gated against the floor of the epoch it was <em>written</em> in, not whatever the latest floor is.
 * At an e-1→e boundary there are in-flight records written in e-1 whose dependencies point at e-1
 * coordinates in {@code [F_{e-1}, F_e)}; flooring them to {@code F_e} would strip those dependencies
 * and release them out of causal order within e-1. So the transition is an <em>interval</em>, not an
 * instant, and both floors are held across it:
 * <ul>
 *   <li>On receiving an {@code ParsleyEpochBoundary(e, F_e)} marker ({@link #onBoundary}) epoch e becomes
 *       <em>pending</em>; the settled (effective) floor stays {@code F_{e-1}} — the lower held floor.
 *   <li>The window is fully engaged once the marker has arrived on <em>every</em> input channel
 *       (Chandy-Lamport-exact; Kafka per-partition order means a marker on a channel implies all e-1
 *       records on it have been received) and closes once this node's own contiguous frontier
 *       <em>dominates</em> {@code F_e} — everything up to the new floor has been delivered
 *       <em>here</em>, so e-1 is provably drained locally (a peer's advertised claim is never proof;
 *       see {@link ParsleyFrontier#tryAdvanceEpoch}). {@link #promote} then makes {@code F_e} the
 *       settled floor. Because it only closes once the local frontier dominates {@code F_e},
 *       advancing the floor needs no re-flooring of already-recorded state.
 * </ul>
 * While the window is open the effective floor is {@code F_{e-1}}, so in-flight e-1 records drain in
 * causal order (their deps are ≥ {@code F_{e-1}} and gated normally) while e records (deps ≥ {@code F_e}
 * ≥ {@code F_{e-1}}) are unaffected — one conservative floor is correct for both epochs, so no message
 * has to be classified by offset.
 *
 * <p>Not thread-safe: a single Kafka Streams task thread owns it. Persisted inside the frontier's
 * {@code "f"} blob (see {@link #toBytes}/{@link #restore}), because a mid-window restart resumes past
 * the already-consumed marker, so the pending window must survive or the transition never closes.
 */
final class ParsleyEpochState implements ParsleyEpoch {

    private static final Logger log = LoggerFactory.getLogger(ParsleyEpochState.class);

    static final byte WIRE_VERSION = 1;

    // The settled (effective) floor: the highest committed epoch's lowerBounds. startsAt reads this.
    private ParsleyClock settledFloor;
    private long settledEpochId;

    // The in-progress transition, or null. While present, the effective floor stays settledFloor (the
    // lower held floor F_{e-1}); the pending floor F_e becomes settled only once the window closes.
    private @Nullable Pending pending;

    private static final class Pending {
        final long epochId;
        final ParsleyClock floor;                          // F_e
        final Set<CoordKey> markersSeen = new HashSet<>(); // input channels the marker has arrived on

        Pending(long epochId, ParsleyClock floor) {
            this.epochId = epochId;
            this.floor = floor;
        }
    }

    private record CoordKey(Uuid topicId, int partition) {}

    /** A fresh state at epoch 0 (floor 0 everywhere, i.e. every coordinate unbounded). */
    ParsleyEpochState() {
        this(ParsleyClock.empty(), 0L);
    }

    ParsleyEpochState(ParsleyClock settledFloor, long settledEpochId) {
        this.settledFloor = settledFloor;
        this.settledEpochId = settledEpochId;
    }

    @Override
    public long startsAt(Uuid topicId, int partition) {
        long offset = settledFloor.offsetFor(topicId, partition);
        return offset < 0 ? NO_BOUND : offset;
    }

    /** The highest committed (settled) epoch id. */
    long settledEpochId() {
        return settledEpochId;
    }

    /** Whether an epoch transition is currently in progress (a boundary received, window not yet closed). */
    boolean isTransitioning() {
        return pending != null;
    }

    /** The pending epoch's floor {@code F_e}, or {@code null} when no transition is in progress. */
    @Nullable ParsleyClock pendingFloor() {
        return pending == null ? null : pending.floor;
    }

    /** Whether the pending transition's marker has been received on channel {@code (topicId, partition)}. */
    boolean hasMarker(Uuid topicId, int partition) {
        return pending != null && pending.markersSeen.contains(new CoordKey(topicId, partition));
    }

    /**
     * Records an epoch-boundary marker received on one input channel. Monotonic: a marker for an epoch
     * at or below the settled epoch is a stale/duplicate re-delivery and ignored; a marker for a higher
     * epoch than the current pending one supersedes it (nested boundaries collapse to the latest, which
     * is safe because the floor advances monotonically). Returns {@code true} if this began a new
     * pending transition (the caller should persist).
     */
    boolean onBoundary(long epochId, ParsleyClock lowerBounds, Uuid channelTopicId, int channelPartition) {
        if (epochId <= settledEpochId) {
            return false;
        }
        boolean began = false;
        if (pending == null || epochId > pending.epochId) {
            pending = new Pending(epochId, lowerBounds);
            began = true;
            log.info("Epoch transition started: epoch {} pending (floor {})", epochId, lowerBounds);
        }
        if (epochId == pending.epochId) {
            pending.markersSeen.add(new CoordKey(channelTopicId, channelPartition));
            log.info("Epoch {} boundary marker received on {}-{} ({} channel(s) seen)",
                    epochId, channelTopicId, channelPartition, pending.markersSeen.size());
        }
        return began;
    }

    /**
     * Promotes the pending floor to settled — the window has closed. The caller must first have
     * verified the marker is present on every input channel and that the delivered frontier dominates
     * {@link #pendingFloor()}. No-op if no transition is in progress.
     */
    void promote() {
        if (pending == null) {
            return;
        }
        settledFloor = pending.floor;
        settledEpochId = pending.epochId;
        log.info("Epoch transition complete: epoch {} effective (floor {})", pending.epochId, pending.floor);
        pending = null;
    }

    /**
     * Serialises this state: {@code [version:1][settledEpochId:8][settledFloor clock][pending:1]} then,
     * if pending, {@code [pendingEpochId:8][pendingFloor clock][markerCount:4]} and per marker
     * {@code [topicId MSB:8][topicId LSB:8][partition:4]}.
     */
    byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
            dos.writeLong(settledEpochId);
            ParsleyByteUtils.writeBytes(dos, settledFloor.toBytes());
            if (pending == null) {
                dos.writeBoolean(false);
            } else {
                dos.writeBoolean(true);
                dos.writeLong(pending.epochId);
                ParsleyByteUtils.writeBytes(dos, pending.floor.toBytes());
                dos.writeInt(pending.markersSeen.size());
                for (CoordKey key : pending.markersSeen) {
                    ParsleyByteUtils.writeUuid(dos, key.topicId());
                    dos.writeInt(key.partition());
                }
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyEpochState serialisation failed", e);
        }
    }

    /** Replaces this state's contents with the {@link #toBytes serialised} form. */
    void restore(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported ParsleyEpochState wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            this.settledEpochId = dis.readLong();
            this.settledFloor = ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            if (dis.readBoolean()) {
                long pendingEpochId = dis.readLong();
                ParsleyClock pendingFloor = ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis));
                Pending restored = new Pending(pendingEpochId, pendingFloor);
                int markerCount = dis.readInt();
                for (int i = 0; i < markerCount; i++) {
                    Uuid topicId = ParsleyByteUtils.readUuid(dis);
                    int partition = dis.readInt();
                    restored.markersSeen.add(new CoordKey(topicId, partition));
                }
                this.pending = restored;
            } else {
                this.pending = null;
            }
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyEpochState deserialisation failed", e);
        }
    }
}
