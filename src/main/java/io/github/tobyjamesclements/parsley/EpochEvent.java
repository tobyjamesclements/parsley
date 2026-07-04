package io.github.tobyjamesclements.parsley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A record on the topology's single-partition {@code epoch-events} log — the shared, totally-ordered
 * coordination handshake for the leaderless epoch protocol. All coordination is event-sourced here; the
 * log's total order plus the deterministic fold in {@link ParsleyEpochLog} let every node agree on round
 * ownership, membership, and the committed lower bounds without a leader. This log carries only the
 * handshake; the runtime floor itself still travels in-band via {@link EpochBoundary} markers.
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
sealed interface EpochEvent
        permits EpochEvent.JoinRequested, EpochEvent.SnapshotRequested,
                EpochEvent.FrontierPublished, EpochEvent.EpochCommitted, EpochEvent.Leave {

    byte TAG_JOIN = 1;
    byte TAG_SNAPSHOT = 2;
    byte TAG_FRONTIER = 3;
    byte TAG_COMMIT = 4;
    byte TAG_LEAVE = 5;

    /** A node announces itself; it becomes a running member (counted in the cut) at the next commit. */
    record JoinRequested(String memberId) implements EpochEvent {}

    /** A node proposes a snapshot round; the first after the last commit opens it and owns it. */
    record SnapshotRequested(String memberId) implements EpochEvent {}

    /** A running member's completeness frontier for the currently open round. */
    record FrontierPublished(String memberId, ParsleyClock completeness) implements EpochEvent {}

    /** The round owner's decision: the new epoch id and its lower bounds. */
    record EpochCommitted(long epochId, ParsleyClock lowerBounds) implements EpochEvent {}

    /**
     * A member is removed from the domain — appended by the member itself (a graceful
     * {@code CausalCoordination.leave()}) or by any node evicting a silent member after a round waits too
     * long. Either way the fold drops it from membership, so a gone member cannot freeze rounds forever.
     */
    record Leave(String memberId) implements EpochEvent {}

    /** Serialises this event: {@code [tag:1]} then the tag-specific body. */
    default byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            switch (this) {
                case JoinRequested e -> {
                    dos.writeByte(TAG_JOIN);
                    writeString(dos, e.memberId());
                }
                case SnapshotRequested e -> {
                    dos.writeByte(TAG_SNAPSHOT);
                    writeString(dos, e.memberId());
                }
                case FrontierPublished e -> {
                    dos.writeByte(TAG_FRONTIER);
                    writeString(dos, e.memberId());
                    writeClock(dos, e.completeness());
                }
                case EpochCommitted e -> {
                    dos.writeByte(TAG_COMMIT);
                    dos.writeLong(e.epochId());
                    writeClock(dos, e.lowerBounds());
                }
                case Leave e -> {
                    dos.writeByte(TAG_LEAVE);
                    writeString(dos, e.memberId());
                }
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("EpochEvent serialisation failed", ex);
        }
    }

    /**
     * Reconstructs an event from its {@link #toBytes serialised} form.
     *
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised tag
     */
    static EpochEvent fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte tag = dis.readByte();
            return switch (tag) {
                case TAG_JOIN -> new JoinRequested(readString(dis));
                case TAG_SNAPSHOT -> new SnapshotRequested(readString(dis));
                case TAG_FRONTIER -> new FrontierPublished(readString(dis), readClock(dis));
                case TAG_COMMIT -> new EpochCommitted(dis.readLong(), readClock(dis));
                case TAG_LEAVE -> new Leave(readString(dis));
                default -> throw new IllegalStateException("unrecognised EpochEvent tag: " + tag);
            };
        } catch (IOException ex) {
            throw new IllegalStateException("EpochEvent deserialisation failed", ex);
        }
    }

    private static void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(b.length);
        dos.write(b);
    }

    private static String readString(DataInputStream dis) throws IOException {
        return new String(dis.readNBytes(dis.readInt()), StandardCharsets.UTF_8);
    }

    private static void writeClock(DataOutputStream dos, ParsleyClock clock) throws IOException {
        byte[] b = clock.toBytes();
        dos.writeInt(b.length);
        dos.write(b);
    }

    private static ParsleyClock readClock(DataInputStream dis) throws IOException {
        return ParsleyClock.fromBytes(dis.readNBytes(dis.readInt()));
    }
}
