package io.github.tobyjamesclements.parsley;

import io.github.tobyjamesclements.parsley.ParsleyChannels.CoordKey;
import org.apache.kafka.common.Uuid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The durable value of the {@code {ns}-frontier} store: every piece of causal metadata
 * {@link ParsleyChannels} persists, gathered into one named record that owns its own serialisation.
 * Stored under {@link ParsleyStores#FRONTIER_KEY}, loaded once at {@link ParsleyChannels}
 * construction and rewritten on every change, in the node's EOS transaction.
 *
 * <p>The seven components, in wire order:
 * <ul>
 *   <li>{@link #frontier} — the node's contiguous delivered frontier clock, the only clock the
 *       delivery gate consults;
 *   <li>{@link #channels} — per input channel {@code (topicId, partition)}, the dependencies
 *       advertised on it, max-merged into the outbound stamp by {@code completeness()};
 *   <li>{@link #highestReceived} — per input channel, the highest offset ever physically received,
 *       making {@code bridge()}'s consumer-skip detection exact across a restart;
 *   <li>{@link #carriedAncestry} — causal past re-homed from coordinates that have left the node's
 *       scope, kept so the stamp keeps dominating it; stamp-side only;
 *   <li>{@link #declaredInputs} — the input topics (name to UUID) this state was written under,
 *       which makes a scope change detectable at the next init;
 *   <li>{@link #ownOutputs} — the node's own acknowledged sink positions; stamp-side only;
 *   <li>{@link #declaredSinks} — the sink topics (name to UUID) the own-outputs clock was written
 *       under, the heal set for a topic that is no longer a sink.
 * </ul>
 *
 * <p>The layout carries a leading {@link #WIRE_VERSION} byte and hard-fails on a mismatch, exactly
 * as {@link ParsleyVectorClock} and {@link ParsleySerializer} do. There is no cross-version upgrade
 * path before 1.0: an unreadable version faults at init rather than being reinterpreted, and a
 * blob written by an older layout (which had no version byte) is rejected rather than mis-parsed.
 * Every section is always written and always read under a given version; a new field bumps the
 * version rather than appending a trailing-optional section. The forwarded-offset index is
 * deliberately not folded in here (it is growable and order-sensitive, so it keeps its own keyed
 * store). {@code docs/reference/wire-format.md} is the authoritative byte layout.
 */
record ParsleyFrontierState(
        ParsleyVectorClock frontier,
        Map<CoordKey, ParsleyVectorClock> channels,
        Map<CoordKey, Long> highestReceived,
        ParsleyVectorClock carriedAncestry,
        Map<String, Uuid> declaredInputs,
        ParsleyVectorClock ownOutputs,
        Map<String, Uuid> declaredSinks) {

    /** The frontier-store value's wire version. Bumped whenever the layout below changes. */
    static final byte WIRE_VERSION = 1;

    byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
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
            throw new IllegalStateException("ParsleyFrontierState serialisation failed", e);
        }
    }

    static ParsleyFrontierState fromBytes(byte[] blob) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(blob))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException("unsupported ParsleyFrontierState wire version: "
                        + version + " (expected " + WIRE_VERSION + ")");
            }
            ParsleyVectorClock frontier = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            Map<CoordKey, ParsleyVectorClock> channels = new HashMap<>();
            int channelCount = dis.readInt();
            for (int i = 0; i < channelCount; i++) {
                Uuid topicId = ParsleyByteUtils.readUuid(dis);
                int partition = dis.readInt();
                channels.put(new CoordKey(topicId, partition),
                        ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis)));
            }
            Map<CoordKey, Long> highestReceived = new HashMap<>();
            int highestReceivedCount = dis.readInt();
            for (int i = 0; i < highestReceivedCount; i++) {
                Uuid topicId = ParsleyByteUtils.readUuid(dis);
                int partition = dis.readInt();
                highestReceived.put(new CoordKey(topicId, partition), dis.readLong());
            }
            ParsleyVectorClock carriedAncestry = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            Map<String, Uuid> declaredInputs = new HashMap<>();
            int inputCount = dis.readInt();
            for (int i = 0; i < inputCount; i++) {
                String name = dis.readUTF();
                declaredInputs.put(name, ParsleyByteUtils.readUuid(dis));
            }
            ParsleyVectorClock ownOutputs = ParsleyVectorClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            Map<String, Uuid> declaredSinks = new HashMap<>();
            int sinkCount = dis.readInt();
            for (int i = 0; i < sinkCount; i++) {
                String name = dis.readUTF();
                declaredSinks.put(name, ParsleyByteUtils.readUuid(dis));
            }
            return new ParsleyFrontierState(frontier, channels, highestReceived, carriedAncestry,
                    declaredInputs, ownOutputs, declaredSinks);
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyFrontierState deserialisation failed", e);
        }
    }
}
