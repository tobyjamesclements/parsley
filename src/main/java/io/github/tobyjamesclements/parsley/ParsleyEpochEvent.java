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
 * <p>The four events:
 * <ul>
 *   <li>{@link JoinRequested} — a node announces itself; it becomes a running member at the next commit.
 *   <li>{@link SnapshotRequested} — a node proposes a snapshot round. The first one after the last
 *       {@link EpochCommitted} (by log order) opens the round and elects its author as owner; the rest
 *       coalesce.
 *   <li>{@link FrontierPublished} — a running member publishes its completeness frontier for the open
 *       round (safe without a consistent cut: completeness is monotonic).
 *   <li>{@link EpochCommitted} — the round owner records the decided {@code epochId} and {@code
 *       lowerBounds} (the {@link ParsleyClock#mergeMin} fold of the published frontiers). Idempotent,
 *       dedup by {@code epochId}.
 * </ul>
 */
sealed interface ParsleyEpochEvent
        permits ParsleyEpochEvent.JoinRequested, ParsleyEpochEvent.SnapshotRequested,
                ParsleyEpochEvent.FrontierPublished, ParsleyEpochEvent.EpochCommitted, ParsleyEpochEvent.Leave {

    byte TAG_JOIN = 1;
    byte TAG_SNAPSHOT = 2;
    byte TAG_FRONTIER = 3;
    byte TAG_COMMIT = 4;
    byte TAG_LEAVE = 5;

    /**
     * A node announces itself; it becomes a running member (counted in the cut) at the next commit. The
     * declared {@code inputTopics} (the channels it consumes) and {@code sinkTopics} (the topics it
     * produces) feed the DAG-wide source-topic registry: the fold derives the topology's external source
     * topics as {@code ∪inputTopics − ∪sinkTopics} over every declared member, so no node has to be told
     * by hand which of its inputs are external.
     */
    record JoinRequested(String memberId, Set<String> inputTopics, Set<String> sinkTopics)
            implements ParsleyEpochEvent {}

    /** A node proposes a snapshot round; the first after the last commit opens it and owns it. */
    record SnapshotRequested(String memberId) implements ParsleyEpochEvent {}

    /** A running member's completeness frontier for the currently open round. */
    record FrontierPublished(String memberId, ParsleyClock completeness) implements ParsleyEpochEvent {}

    /** The round owner's decision: the new epoch id and its lower bounds. */
    record EpochCommitted(long epochId, ParsleyClock lowerBounds) implements ParsleyEpochEvent {}

    /**
     * A member is removed from the domain — appended by the member itself (a graceful
     * {@code ParsleyCoordination.leave()}) or by any node evicting a silent member after a round waits too
     * long. Either way the fold drops it from membership, so a gone member cannot freeze rounds forever.
     */
    record Leave(String memberId) implements ParsleyEpochEvent {}

    /** Serialises this event: {@code [tag:1]} then the tag-specific body. */
    default byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            switch (this) {
                case JoinRequested e -> {
                    dos.writeByte(TAG_JOIN);
                    ParsleyByteUtils.writeString(dos, e.memberId());
                    ParsleyByteUtils.writeStringSet(dos, e.inputTopics());
                    ParsleyByteUtils.writeStringSet(dos, e.sinkTopics());
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
                    dos.writeByte(TAG_COMMIT);
                    dos.writeLong(e.epochId());
                    ParsleyByteUtils.writeBytes(dos, e.lowerBounds().toBytes());
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
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised tag
     */
    static ParsleyEpochEvent fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte tag = dis.readByte();
            return switch (tag) {
                case TAG_JOIN -> new JoinRequested(ParsleyByteUtils.readString(dis),
                        ParsleyByteUtils.readStringSet(dis), ParsleyByteUtils.readStringSet(dis));
                case TAG_SNAPSHOT -> new SnapshotRequested(ParsleyByteUtils.readString(dis));
                case TAG_FRONTIER -> new FrontierPublished(ParsleyByteUtils.readString(dis),
                        ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis)));
                case TAG_COMMIT -> new EpochCommitted(dis.readLong(),
                        ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis)));
                case TAG_LEAVE -> new Leave(ParsleyByteUtils.readString(dis));
                default -> throw new IllegalStateException("unrecognised ParsleyEpochEvent tag: " + tag);
            };
        } catch (IOException ex) {
            throw new IllegalStateException("ParsleyEpochEvent deserialisation failed", ex);
        }
    }
}
