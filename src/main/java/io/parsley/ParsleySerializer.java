package io.parsley;

import org.apache.kafka.common.Uuid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises a held {@link ParsleyMessage} to and from the byte value stored in the durable buffer
 * store (keyed by insertion sequence), so held records survive a restart.
 *
 * <p>Key and value bytes are produced/consumed with the serdes a {@link ParsleyResolver} resolves
 * from the message's own source topic. The source coordinate and dependency clock are written as
 * typed fields, not headers — only the user's headers are carried.
 *
 * <p>Format (v3):
 * {@code [version:1][timestamp:8][topic:str][topicId:16][partition:4][offset:8][deps:nullable]
 * [header-count:4][user-headers...][key:nullable][value:nullable]}. Each header:
 * {@code [key-len:2][key-bytes][value-len:4][value-bytes|-1 for null]}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleySerializer<K, V> {

    /** Leading byte of the buffer-store value format; lets the format evolve compatibly. */
    private static final byte FORMAT_VERSION = 3;

    private final ParsleyResolver<K, V> resolver;

    ParsleySerializer(ParsleyResolver<K, V> resolver) {
        this.resolver = resolver;
    }

    /**
     * Serialises a held message to bytes.
     */
    byte[] serialize(ParsleyMessage<K, V> message) {
        String topic = message.topic();
        byte[] keyBytes = resolver.keySerde(topic).serializer().serialize(topic, message.key());
        byte[] valueBytes = resolver.valueSerde(topic).serializer().serialize(topic, message.value());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(FORMAT_VERSION);
            out.writeLong(message.timestamp());
            writeString(out, topic);
            out.write(ParsleyHeader.uuidToBytes(message.topicId()));
            out.writeInt(message.partition());
            out.writeLong(message.offset());
            writeNullable(out, message.dependencies().toBytes());
            out.writeInt(message.headers().size());
            for (ParsleyHeader header : message.headers()) {
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
     * Reconstructs a held message from {@link #serialize bytes}.
     */
    ParsleyMessage<K, V> deserialize(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = in.readByte();
            if (version != FORMAT_VERSION) {
                throw new IllegalStateException(
                        "unsupported buffered-record format version: " + version + " (expected " + FORMAT_VERSION + ")");
            }
            long timestamp = in.readLong();
            String topic = readString(in);
            byte[] topicIdBytes = new byte[16];
            in.readFully(topicIdBytes);
            Uuid topicId = ParsleyHeader.uuidFromBytes(topicIdBytes);
            int partition = in.readInt();
            long offset = in.readLong();
            ParsleyClock dependencies = ParsleyClock.fromBytes(readNullable(in));
            int headerCount = in.readInt();
            List<ParsleyHeader> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                String key = readString(in);
                byte[] value = readNullable(in);
                headers.add(new ParsleyHeader(key, value));
            }
            byte[] keyBytes = readNullable(in);
            byte[] valueBytes = readNullable(in);

            K key = keyBytes == null ? null
                    : resolver.keySerde(topic).deserializer().deserialize(topic, keyBytes);
            V value = valueBytes == null ? null
                    : resolver.valueSerde(topic).deserializer().deserialize(topic, valueBytes);

            return new ParsleyMessage<>(topic, topicId, partition, offset, timestamp,
                    key, value, headers, dependencies);
        } catch (IOException e) {
            throw new IllegalStateException("Buffered record deserialisation failed", e);
        }
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
