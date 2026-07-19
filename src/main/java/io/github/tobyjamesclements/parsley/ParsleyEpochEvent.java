package io.github.tobyjamesclements.parsley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

/**
 * A record on the topology's single-partition {@code epoch-events} log — the shared, totally-ordered
 * coordination handshake for the leaderless epoch protocol. All coordination is event-sourced here; the
 * log's total order plus the deterministic fold in {@link ParsleyEpochLog} let every node agree on round
 * ownership, membership, and the committed lower bounds without a leader. This log carries only the
 * handshake; the runtime floor itself still travels in-band via {@link ParsleyEpochBoundary} markers.
 *
 * <p>The five events:
 * <ul>
 *   <li>{@link JoinRequested} — a node announces itself (declaring its app id, topics, its view of the
 *       founding-member roster, and its app's total task count); it becomes a running member at a commit.
 *   <li>{@link SnapshotRequested} — a node proposes a snapshot round. The first one after the last
 *       {@link EpochCommitted} (by log order) opens the round; the rest coalesce.
 *   <li>{@link FrontierPublished} — a running member publishes its completeness frontier for the open
 *       round (safe without a consistent cut: completeness is monotonic).
 *   <li>{@link EpochCommitted} — the decided {@code epochId}, {@code lowerBounds}, and the {@code
 *       committedRoster} (the app-id membership this epoch establishes). Appended by <em>any</em> node
 *       with a local member once the round is complete — the fold is deterministic, so every node
 *       computes the identical commit and dedup-by-{@code epochId} makes concurrent appends idempotent.
 *   <li>{@link Leave} — a member is removed from the domain (graceful, explicit decommission only).
 * </ul>
 *
 * <p><strong>Wire compatibility.</strong> {@link JoinRequested} and {@link EpochCommitted} carry the
 * genesis-cohort fields on <em>new</em> tags ({@link #TAG_JOIN2}/{@link #TAG_COMMIT2}); the pre-cohort
 * tags ({@link #TAG_JOIN}/{@link #TAG_COMMIT}) are rejected on read with a fatal
 * {@link ParsleyIncompatibleEpochLogException}. This is deliberate: extending the old bodies in place
 * would let an older binary silently ignore the trailing bytes and fold under the old rules (committing a
 * vacuous genesis), so an incompatibility fails loud instead. A domain coordinated by an older Parsley
 * must reset its epoch-events topic, which is safe before genesis has committed.
 */
sealed interface ParsleyEpochEvent
        permits ParsleyEpochEvent.JoinRequested, ParsleyEpochEvent.SnapshotRequested,
                ParsleyEpochEvent.FrontierPublished, ParsleyEpochEvent.EpochCommitted, ParsleyEpochEvent.Leave {

    byte TAG_JOIN = 1;          // pre-cohort JoinRequested — rejected on read (see class Javadoc)
    byte TAG_SNAPSHOT = 2;
    byte TAG_FRONTIER = 3;
    byte TAG_COMMIT = 4;        // pre-cohort EpochCommitted — rejected on read
    byte TAG_LEAVE = 5;
    byte TAG_JOIN2 = 6;         // JoinRequested with the genesis-cohort fields
    byte TAG_COMMIT2 = 7;       // EpochCommitted with the committed roster

    /**
     * A node announces itself; it becomes a running member (counted in the cut) at a commit that admits
     * its app. The declared {@code inputTopics} (channels it consumes) and {@code sinkTopics} (topics it
     * produces) feed the DAG-wide source-topic registry ({@code ∪inputTopics − ∪sinkTopics} over every
     * declared member). {@code appId} is the causal node's application id, carried explicitly rather than
     * string-parsed from {@code memberId} ({@code appId/taskId}). {@code rosterView} is this node's
     * configured view of the complete member-app roster, and {@code taskTotal} is the total number of
     * tasks its app will run — together they drive the genesis cohort barrier and the committed-member
     * roster agreement (see {@link ParsleyEpochLog}).
     */
    record JoinRequested(String memberId, String appId, Set<String> inputTopics, Set<String> sinkTopics,
                         Set<String> rosterView, int taskTotal) implements ParsleyEpochEvent {}

    /** A node proposes a snapshot round; the first after the last commit opens it and owns it. */
    record SnapshotRequested(String memberId) implements ParsleyEpochEvent {}

    /** A running member's completeness frontier for the currently open round. */
    record FrontierPublished(String memberId, ParsleyVectorClock completeness) implements ParsleyEpochEvent {}

    /** A complete round's decision — the new epoch id, its lower bounds, and the app-id membership
     * (committed roster) it establishes — appended by any node with a local member (identical everywhere;
     * dedup by {@code epochId}). */
    record EpochCommitted(long epochId, ParsleyVectorClock lowerBounds, Set<String> committedRoster)
            implements ParsleyEpochEvent {}

    /**
     * A member is removed from the domain — appended only by the member itself, via a graceful
     * {@code ParsleyCoordination.leave()}. There is no automatic eviction: a round with a silent member
     * simply waits, unbounded, until that member publishes or explicitly leaves — evicting it and
     * committing a floor without it could strand records it still holds below that floor and release
     * them before their causes.
     */
    record Leave(String memberId) implements ParsleyEpochEvent {}

    /** Serialises this event: {@code [tag:1]} then the tag-specific body. */
    default byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            switch (this) {
                case JoinRequested e -> {
                    dos.writeByte(TAG_JOIN2);
                    ParsleyByteUtils.writeString(dos, e.memberId());
                    ParsleyByteUtils.writeString(dos, e.appId());
                    ParsleyByteUtils.writeStringSet(dos, e.inputTopics());
                    ParsleyByteUtils.writeStringSet(dos, e.sinkTopics());
                    ParsleyByteUtils.writeStringSet(dos, e.rosterView());
                    dos.writeInt(e.taskTotal());
                }
                case SnapshotRequested e -> {
                    dos.writeByte(TAG_SNAPSHOT);
                    ParsleyByteUtils.writeString(dos, e.memberId());
                }
                case FrontierPublished e -> {
                    dos.writeByte(TAG_FRONTIER);
                    ParsleyByteUtils.writeString(dos, e.memberId());
                    ParsleyByteUtils.writeBytes(dos, e.completeness().toBytes());
                }
                case EpochCommitted e -> {
                    dos.writeByte(TAG_COMMIT2);
                    dos.writeLong(e.epochId());
                    ParsleyByteUtils.writeBytes(dos, e.lowerBounds().toBytes());
                    ParsleyByteUtils.writeStringSet(dos, e.committedRoster());
                }
                case Leave e -> {
                    dos.writeByte(TAG_LEAVE);
                    ParsleyByteUtils.writeString(dos, e.memberId());
                }
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("ParsleyEpochEvent serialisation failed", ex);
        }
    }

    /**
     * Reconstructs an event from its {@link #toBytes serialised} form.
     *
     * @throws ParsleyIncompatibleEpochLogException if {@code bytes} is corrupt/truncated, or carries a
     *         pre-cohort ({@link #TAG_JOIN}/{@link #TAG_COMMIT}) or unrecognised tag — a permanent
     *         incompatibility the runtime fails closed on rather than retrying (see the class Javadoc)
     */
    static ParsleyEpochEvent fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte tag = dis.readByte();
            ParsleyEpochEvent event = switch (tag) {
                case TAG_JOIN2 -> new JoinRequested(
                        ParsleyByteUtils.readString(dis), ParsleyByteUtils.readString(dis),
                        ParsleyByteUtils.readStringSet(dis), ParsleyByteUtils.readStringSet(dis),
                        ParsleyByteUtils.readStringSet(dis), dis.readInt());
                case TAG_SNAPSHOT -> new SnapshotRequested(ParsleyByteUtils.readString(dis));
                case TAG_FRONTIER -> new FrontierPublished(ParsleyByteUtils.readString(dis),
                        ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis)));
                case TAG_COMMIT2 -> new EpochCommitted(dis.readLong(),
                        ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis)),
                        ParsleyByteUtils.readStringSet(dis));
                case TAG_LEAVE -> new Leave(ParsleyByteUtils.readString(dis));
                case TAG_JOIN, TAG_COMMIT -> throw new ParsleyIncompatibleEpochLogException(
                        "epoch-events record uses the pre-genesis-cohort wire format (tag " + tag + "); this "
                        + "binary is incompatible with it. Reset the epoch-events topic (safe before genesis "
                        + "has committed) and restart the domain.");
                default -> throw new ParsleyIncompatibleEpochLogException(
                        "unrecognised ParsleyEpochEvent tag: " + tag + " — the epoch-events log was written by "
                        + "an incompatible binary. Reset the epoch-events topic (safe before genesis).");
            };
            // No trailing bytes may remain after a recognised body — trailing content means a newer,
            // longer wire format this binary cannot fully read (the one-level-down form of the pre-cohort
            // hazard). Fail closed rather than silently ignore it.
            if (dis.read() != -1) {
                throw new ParsleyIncompatibleEpochLogException("epoch-events record (tag " + tag + ") has "
                        + "trailing bytes after its body — a newer wire format; reset the epoch-events topic "
                        + "(safe before genesis has committed).");
            }
            return event;
        } catch (IOException ex) {
            throw new ParsleyIncompatibleEpochLogException(
                    "ParsleyEpochEvent deserialisation failed — corrupt or truncated epoch-events record", ex);
        }
    }
}
