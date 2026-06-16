package io.parsley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises a held {@link ParsleyRecord} to and from the byte value stored in the durable buffer
 * store (keyed by insertion sequence), so held records survive a restart.
 *
 * <p>Key and value bytes are produced/consumed with the serdes a {@link ParsleyResolver} resolves
 * from the record's own source topic (read from the {@link ParsleyAttributes#SRC_TOPIC} header).
 * Headers are written in full — they carry the source coordinate and dependency clock — so the
 * serialised form is self-describing and no separate coordinate fields are needed.
 *
 * <p>Format (v2): {@code [version:1][timestamp:8][header-count:4][headers...][key-len:4][key-bytes][value-len:4][value-bytes]}.
 * Each header: {@code [key-len:2][key-bytes][value-len:4][value-bytes|-1 for null]}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleySerializer<K, V> {

    /** Leading byte of the buffer-store value format; lets the format evolve compatibly. */
    private static final byte FORMAT_VERSION = 2;

    private final ParsleyResolver<K, V> resolver;

    ParsleySerializer(ParsleyResolver<K, V> resolver) {
        this.resolver = resolver;
    }

    /**
     * Serialises a held record to bytes.
     */
    byte[] serialize(ParsleyRecord<K, V> record) {
        String topic = record.sourceTopic();
        byte[] keyBytes = resolver.keySerde(topic).serializer().serialize(topic, record.key());
        byte[] valueBytes = resolver.valueSerde(topic).serializer().serialize(topic, record.value());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(FORMAT_VERSION);
            out.writeLong(record.timestamp());
            out.writeInt(record.headers().size());
            for (ParsleyHeader header : record.headers()) {
                writeString(out, header.key());
                writeNullable(out, header.value());
            }
            writeNullable(out, keyBytes);
            writeNullable(out, valueBytes);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Buffered record serialisation failed", e);
        }
    }

    /**
     * Reconstructs a held record from {@link #serialize bytes}.
     */
    ParsleyRecord<K, V> deserialize(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = in.readByte();
            if (version != FORMAT_VERSION) {
                throw new IllegalStateException(
                        "unsupported buffered-record format version: " + version + " (expected " + FORMAT_VERSION + ")");
            }
            long timestamp = in.readLong();
            int headerCount = in.readInt();
            List<ParsleyHeader> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                String key = readString(in);
                byte[] value = readNullable(in);
                headers.add(new ParsleyHeader(key, value));
            }
            String topic = findHeaderString(headers, ParsleyAttributes.SRC_TOPIC);
            byte[] keyBytes = readNullable(in);
            byte[] valueBytes = readNullable(in);

            K key = keyBytes == null ? null
                    : resolver.keySerde(topic).deserializer().deserialize(topic, keyBytes);
            V value = valueBytes == null ? null
                    : resolver.valueSerde(topic).deserializer().deserialize(topic, valueBytes);

            return new ParsleyRecord<>(key, value, timestamp, headers);
        } catch (IOException e) {
            throw new IllegalStateException("Buffered record deserialisation failed", e);
        }
    }

    private static String findHeaderString(List<ParsleyHeader> headers, String key) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            if (key.equals(headers.get(i).key())) {
                byte[] v = headers.get(i).value();
                return v == null ? "" : new String(v, StandardCharsets.UTF_8);
            }
        }
        return "";
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeNullable(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readNullable(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }
}
