package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Wire-primitive read/write helpers shared by every Parsley type that hand-rolls its own binary
 * {@code toBytes}/{@code fromBytes} — {@link ParsleyVectorClock}, {@link ParsleyChannels}. Modelled on
 * Kafka's own
 * {@code org.apache.kafka.common.utils.ByteUtils}: a stateless utility of primitive encoders, not a
 * shared serializer — each type still owns its own version byte, framing, and {@code toBytes}/{@code
 * fromBytes} pair; only the repeated byte-fiddling boilerplate lives here.
 */
final class ParsleyByteUtils {

    private ParsleyByteUtils() {}

    /** Writes {@code bytes} length-prefixed: {@code [length:4][bytes]}. */
    static void writeBytes(DataOutputStream dos, byte[] bytes) throws IOException {
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    /** Reads a length-prefixed byte array written by {@link #writeBytes}. */
    static byte[] readBytes(DataInputStream dis) throws IOException {
        return dis.readNBytes(dis.readInt());
    }

    /** Writes {@code id} as {@code [mostSignificantBits:8][leastSignificantBits:8]}. */
    static void writeUuid(DataOutputStream dos, Uuid id) throws IOException {
        dos.writeLong(id.getMostSignificantBits());
        dos.writeLong(id.getLeastSignificantBits());
    }

    /** Reads a {@link Uuid} written by {@link #writeUuid}. */
    static Uuid readUuid(DataInputStream dis) throws IOException {
        return new Uuid(dis.readLong(), dis.readLong());
    }

    /** Writes {@code s} length-prefixed as UTF-8: {@code [length:4][utf8 bytes]}. */
    static void writeString(DataOutputStream dos, String s) throws IOException {
        writeBytes(dos, s.getBytes(StandardCharsets.UTF_8));
    }

    /** Reads a length-prefixed UTF-8 string written by {@link #writeString}. */
    static String readString(DataInputStream dis) throws IOException {
        return new String(readBytes(dis), StandardCharsets.UTF_8);
    }

}
