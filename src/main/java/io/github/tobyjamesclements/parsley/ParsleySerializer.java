package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
     * Serialises {@code key} with the key serde resolved for {@code topic} — used to re-serialise a
     * dead-lettered record's already-decoded key for the dead-letter topic (which carries raw bytes,
     * never typed objects).
     */
    byte @Nullable [] keyBytes(String topic, @Nullable K key) {
        return resolver.keySerde(topic).serializer().serialize(topic, key);
    }

    /**
     * As {@link #keyBytes(String, Object)}, for the value.
     */
    byte @Nullable [] valueBytes(String topic, @Nullable V value) {
        return resolver.valueSerde(topic).serializer().serialize(topic, value);
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
            // Dependencies are always framed non-null (serialize() writes dependencies().toBytes()).
            ParsleyClock dependencies = ParsleyClock.fromBytes(Objects.requireNonNull(readNullable(in)));
            int headerCount = in.readInt();
            List<ParsleyHeader> headers = new ArrayList<>(headerCount);
            for (int i = 0; i < headerCount; i++) {
                String key = readString(in);
                byte[] value = readNullable(in);
                headers.add(new ParsleyHeader(key, value));
            }
            byte[] keyBytes = readNullable(in);
            byte[] valueBytes = readNullable(in);

            K key;
            V value;
            try {
                key = keyBytes == null ? null
                        : resolver.keySerde(topic).deserializer().deserialize(topic, keyBytes);
                value = valueBytes == null ? null
                        : resolver.valueSerde(topic).deserializer().deserialize(topic, valueBytes);
            } catch (RuntimeException e) {
                // The user serde (e.g. Avro + Schema Registry) could not decode the held bytes —
                // typically a registry state change after buffering. Surface a typed error carrying
                // everything decodable without the serde; how it's handled (fail vs skip) is the
                // caller's decision.
                int schemaId = schemaId(valueBytes, keyBytes);
                // (schemaId reads either array's Confluent magic byte; both may be null tombstones)
                String details = details(topic, topicId, partition, offset, timestamp,
                        dependencies, headers, keyBytes, valueBytes, schemaId);
                throw new ParsleyBufferDeserializationException(topic, topicId, partition, offset, timestamp,
                        headers, keyBytes, valueBytes, schemaId, details, e);
            }

            return new ParsleyMessage<>(topic, topicId, partition, offset, timestamp,
                    key, value, headers, dependencies);
        } catch (IOException e) {
            throw new IllegalStateException("Buffered record deserialisation failed", e);
        }
    }

    /**
     * The source coordinate and dependency clock decodable for an {@link ParsleyBufferStore.IndexEntry
     * IndexEntry} — see {@link #deserializeIndexMetadata}.
     */
    record IndexMetadata(String topic, Uuid topicId, int partition, long offset, ParsleyClock dependencies) {}

    /**
     * Reconstructs only the metadata a restored buffer needs to rebuild its candidate index, and to
     * identify a record for eviction reporting — the source coordinate and the insertion-time
     * dependency clock — <strong>without invoking the user serde</strong>. Both are part of Parsley's
     * own framing (written before the key/value bytes), so a value Parsley cannot decode (e.g. an
     * incompatible Schema Registry change) never blocks startup or eviction; that failure surfaces
     * only when the record is actually forwarded via {@link #deserialize}.
     *
     * @param bytes the buffered-record bytes
     * @return the record's source coordinate and decoded dependency clock
     */
    IndexMetadata deserializeIndexMetadata(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = in.readByte();
            if (version != FORMAT_VERSION) {
                throw new IllegalStateException(
                        "unsupported buffered-record format version: " + version + " (expected " + FORMAT_VERSION + ")");
            }
            in.readLong();               // timestamp
            String topic = readString(in);
            byte[] topicIdBytes = new byte[16];
            in.readFully(topicIdBytes);
            Uuid topicId = ParsleyHeader.uuidFromBytes(topicIdBytes);
            int partition = in.readInt();
            long offset = in.readLong();
            // Dependencies are always framed non-null (serialize() writes dependencies().toBytes()).
            ParsleyClock dependencies = ParsleyClock.fromBytes(Objects.requireNonNull(readNullable(in)));
            return new IndexMetadata(topic, topicId, partition, offset, dependencies);
        } catch (IOException e) {
            throw new IllegalStateException("Buffered record metadata deserialisation failed", e);
        }
    }

    /**
     * Renders an operator diagnostic for a held record that could not be deserialised — everything
     * decodable <strong>without</strong> the user serde, and <strong>never the payload bytes</strong>
     * (those stay in the buffer changelog topic). Lengths and the schema id are enough to identify the
     * record and its subject.
     */
    private static String details(String topic, Uuid topicId, int partition, long offset, long timestamp,
                                  ParsleyClock dependencies, List<ParsleyHeader> headers,
                                  byte @Nullable [] keyBytes, byte @Nullable [] valueBytes, int schemaId) {
        List<String> headerKeys = new ArrayList<>(headers.size());
        for (ParsleyHeader header : headers) {
            headerKeys.add(header.key());
        }
        return "held record " + topic + "-" + partition + "@" + offset
                + " (topicId " + topicId + ", ts " + timestamp + ")"
                + "; schema id: " + (schemaId >= 0 ? schemaId : "n/a")
                + "; dependencies: " + dependencies
                + "; header keys: " + headerKeys
                + "; key bytes: " + (keyBytes == null ? "null" : keyBytes.length)
                + "; value bytes: " + (valueBytes == null ? "null" : valueBytes.length);
    }

    /**
     * Best-effort extraction of the Confluent wire-format writer schema id ({@code [0x00][id:4]}) for
     * diagnostics, from the first candidate that carries the magic byte; {@code -1} if none does.
     */
    private static int schemaId(byte @Nullable [] first, byte @Nullable [] second) {
        for (byte[] bytes : new byte[][] {first, second}) {
            if (bytes != null && bytes.length >= 5 && bytes[0] == 0x0) {
                return ((bytes[1] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16)
                        | ((bytes[3] & 0xFF) << 8) | (bytes[4] & 0xFF);
            }
        }
        return -1;
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

    private static void writeNullable(DataOutputStream out, byte @Nullable [] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte @Nullable [] readNullable(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }
}
