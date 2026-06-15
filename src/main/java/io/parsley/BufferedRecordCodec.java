package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Serialises a buffered {@link ParsleyRecord} to and from the byte value stored in the durable buffer
 * store (keyed by insertion sequence), so held records survive a restart.
 *
 * <p>The key and value are (de)serialised with the user's serdes, resolved <strong>by the record's
 * own source topic</strong> and invoked with that source topic as the {@code (topic, …)} argument —
 * never the buffer store's changelog name — so topic-scoped serdes (e.g. Avro + Schema Registry
 * {@code TopicNameStrategy}) resolve the correct subject. Everything else (source coordinate,
 * timestamp, dependency-clock bytes, and headers) is written in a compact hand-rolled form.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class BufferedRecordCodec<K, V> {

    private final Function<String, Serde<K>> keySerdeByTopic;
    private final Function<String, Serde<V>> valueSerdeByTopic;

    BufferedRecordCodec(Function<String, Serde<K>> keySerdeByTopic,
                        Function<String, Serde<V>> valueSerdeByTopic) {
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
    }

    /**
     * Serialises a held record to bytes. The dependency clock travels as the record's
     * {@code encodedDependencies}; the engine re-decodes it on restore (a buffered record always
     * carries a valid clock), so it is stored once.
     */
    byte[] serialize(ParsleyRecord<K, V> record) {
        String topic = record.sourcePartition().topic();
        byte[] keyBytes = keySerdeByTopic.apply(topic).serializer().serialize(topic, record.key());
        byte[] valueBytes = valueSerdeByTopic.apply(topic).serializer().serialize(topic, record.value());

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
                    : keySerdeByTopic.apply(topic).deserializer().deserialize(topic, keyBytes);
            V value = valueBytes == null ? null
                    : valueSerdeByTopic.apply(topic).deserializer().deserialize(topic, valueBytes);

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
