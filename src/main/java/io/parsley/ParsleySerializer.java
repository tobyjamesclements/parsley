package io.parsley;

import org.apache.kafka.common.TopicPartition;

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
 * from the record's own source topic; the rest of the envelope (source coordinate, timestamp,
 * dependency-clock bytes, headers) is written in a compact hand-rolled form. The dependency clock
 * travels as the record's {@code encodedDependencies} and is re-decoded on restore (a buffered record
 * always carries a valid clock), so it is stored once.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleySerializer<K, V> {

    private final ParsleyResolver<K, V> resolver;

    ParsleySerializer(ParsleyResolver<K, V> resolver) {
        this.resolver = resolver;
    }

    /**
     * Serialises a held record to bytes.
     */
    byte[] serialize(ParsleyRecord<K, V> record) {
        String topic = record.sourcePartition().topic();
        byte[] keyBytes = resolver.keySerde(topic).serializer().serialize(topic, record.key());
        byte[] valueBytes = resolver.valueSerde(topic).serializer().serialize(topic, record.value());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            writeString(out, topic);
            out.writeInt(record.sourcePartition().partition());
            out.writeLong(record.sourceOffset());
            out.writeLong(record.timestamp());
            writeNullable(out, keyBytes);
            writeNullable(out, valueBytes);
            writeNullable(out, record.encodedDependencies());
            out.writeInt(record.headers().size());
            for (ParsleyHeader header : record.headers()) {
                writeString(out, header.key());
                writeNullable(out, header.value());
            }
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
            String topic = readString(in);
            int partition = in.readInt();
            long offset = in.readLong();
            long timestamp = in.readLong();
            byte[] keyBytes = readNullable(in);
            byte[] valueBytes = readNullable(in);
            byte[] encodedDependencies = readNullable(in);
            int headerCount = in.readInt();
            List<ParsleyHeader> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                String key = readString(in);
                byte[] value = readNullable(in);
                headers.add(new ParsleyHeader(key, value));
            }

            K key = keyBytes == null ? null
                    : resolver.keySerde(topic).deserializer().deserialize(topic, keyBytes);
            V value = valueBytes == null ? null
                    : resolver.valueSerde(topic).deserializer().deserialize(topic, valueBytes);

            return new ParsleyRecord<>(
                    key, value, timestamp, headers, encodedDependencies,
                    new TopicPartition(topic, partition), offset);
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
