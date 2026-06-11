package io.parsley.serialisation;

import io.parsley.Partition;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.internal.ImmutableVectorClock;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Compact binary implementation of {@link VectorClockSerialiser}.
 *
 * <p>This is the default serialiser shipped with Parsley and is registered as a
 * {@link java.util.ServiceLoader} provider via
 * {@code META-INF/services/io.parsley.VectorClockSerialiser}.
 *
 * <h2>Wire format</h2>
 * <p>The serialised form is a sequence of big-endian fields:
 * <pre>
 * [int   count]
 * for each entry:
 *   [short topicLen] [byte[] topic (UTF-8)] [int partition] [long offset]
 * </pre>
 *
 * <p>The format is intentionally simple — no versioning or length prefixes beyond
 * {@code count}. Deserialisation will throw {@link IllegalStateException} if the byte array
 * is truncated or otherwise malformed.
 */
public final class DefaultVectorClockSerialiser implements VectorClockSerialiser {

    /**
     * Serialises {@code clock} to a compact binary byte array.
     *
     * @param clock the clock to serialise; must not be {@code null}
     * @return a byte array encoding all partition-offset pairs in the clock
     * @throws IllegalStateException if an I/O error occurs during serialisation (should not
     *                               happen with in-memory streams)
     */
    @Override
    public byte[] serialise(VectorClock clock) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            Map<Partition, Long> positions = clock.positions();
            dos.writeInt(positions.size());
            for (Map.Entry<Partition, Long> entry : positions.entrySet()) {
                byte[] topicBytes = entry.getKey().topic().getBytes(StandardCharsets.UTF_8);
                dos.writeShort(topicBytes.length);
                dos.write(topicBytes);
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Serialisation failed", e);
        }
    }

    /**
     * Deserialises a byte array produced by {@link #serialise} into a {@link VectorClock}.
     *
     * @param bytes the bytes to deserialise; must not be {@code null}
     * @return the reconstructed {@code VectorClock}
     * @throws IllegalStateException if the byte array is malformed or truncated
     */
    @Override
    public VectorClock deserialise(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int count = dis.readInt();
            Map<Partition, Long> positions = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                int topicLen = dis.readUnsignedShort();
                byte[] topicBytes = new byte[topicLen];
                dis.readFully(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                int partition = dis.readInt();
                long offset = dis.readLong();
                positions.put(new Partition(topic, partition), offset);
            }
            return new ImmutableVectorClock(positions);
        } catch (IOException e) {
            throw new IllegalStateException("Deserialisation failed", e);
        }
    }
}
