package io.github.tobyjamesclements.parsley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A topology epoch-boundary marker: the {@code epochId} (strictly increasing) and the epoch's
 * {@code lowerBounds} — the new per-coordinate floor, carried as a {@link ParsleyClock}. Written by the
 * Topology Co-ordinator to every input channel and carried in the {@link ParsleyHeader#EPOCH_BOUNDARY}
 * control header; a processor adopts it into its {@link ParsleyEpochState} on receipt.
 *
 * @param epochId     the new epoch's id, strictly increasing across boundaries
 * @param lowerBounds the new floor {@code F_e}: the lowest in-domain offset per {@code (topicId, partition)}
 */
record ParsleyEpochBoundary(long epochId, ParsleyClock lowerBounds) {

    /** Leading byte of the wire format. */
    static final byte WIRE_VERSION = 1;

    /** Serialises to {@code [version:1][epochId:8][lowerBounds clock]}. */
    byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
            dos.writeLong(epochId);
            ParsleyByteUtils.writeBytes(dos, lowerBounds.toBytes());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyEpochBoundary serialisation failed", e);
        }
    }

    /**
     * Reconstructs a boundary from its {@link #toBytes serialised} form.
     *
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    static ParsleyEpochBoundary fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported ParsleyEpochBoundary wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            long epochId = dis.readLong();
            ParsleyClock lowerBounds = ParsleyClock.fromBytes(ParsleyByteUtils.readBytes(dis));
            return new ParsleyEpochBoundary(epochId, lowerBounds);
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyEpochBoundary deserialisation failed", e);
        }
    }
}
