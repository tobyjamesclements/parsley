package io.parsley;

import org.apache.kafka.common.Uuid;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A single record header — a {@code (key, value)} pair — plus Parsley's header-key vocabulary.
 *
 * <p>This type owns the names of every header Parsley reads or writes and the rule that distinguishes
 * Parsley's internal routing headers (the {@code _parsley_*} prefix) from user headers, so that
 * knowledge lives with the header type rather than scattered as loose constants. Typed factories
 * ({@link #srcOffset}, {@link #srcTopicId}, …) bundle each reserved key with its byte encoding.
 *
 * @param key   the header name; must not be {@code null} or empty
 * @param value the raw header bytes; may be {@code null}
 */
record ParsleyHeader(String key, byte[] value) {

    /** Prefix marking a header as Parsley-internal routing metadata, stripped before user delivery. */
    static final String INTERNAL_PREFIX = "_parsley_";

    /** Header carrying a record's serialised causal dependency clock. */
    static final String CAUSAL_DEPENDENCIES = "parsley-causal-dependencies";

    /** Internal header: the source topic of a record routed through the outbox path. */
    static final String SRC_TOPIC     = "_parsley_src_topic";
    /** Internal header: the Kafka topic UUID (16 bytes: MSB then LSB) of the source topic. */
    static final String SRC_TOPIC_ID  = "_parsley_src_topic_id";
    /** Internal header: the source partition (4-byte big-endian int) of a routed record. */
    static final String SRC_PARTITION = "_parsley_src_partition";
    /** Internal header: the source offset (8-byte big-endian long) of a routed record. */
    static final String SRC_OFFSET    = "_parsley_src_offset";

    ParsleyHeader {
        Objects.requireNonNull(key, "header key must not be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("header key must not be empty");
        }
    }

    /** Returns {@code true} if this is a Parsley-internal routing header (the {@code _parsley_} prefix). */
    boolean isInternal() {
        return key.startsWith(INTERNAL_PREFIX);
    }

    static ParsleyHeader srcTopic(String topic)        { return new ParsleyHeader(SRC_TOPIC, topic.getBytes(UTF_8)); }
    static ParsleyHeader srcTopicId(Uuid topicId)      { return new ParsleyHeader(SRC_TOPIC_ID, uuidToBytes(topicId)); }
    static ParsleyHeader srcPartition(int partition)   { return new ParsleyHeader(SRC_PARTITION, intToBytes(partition)); }
    static ParsleyHeader srcOffset(long offset)        { return new ParsleyHeader(SRC_OFFSET, longToBytes(offset)); }
    static ParsleyHeader causalDependencies(byte[] bytes) { return new ParsleyHeader(CAUSAL_DEPENDENCIES, bytes); }

    static byte[] intToBytes(int v)    { return ByteBuffer.allocate(4).putInt(v).array(); }
    static byte[] longToBytes(long v)  { return ByteBuffer.allocate(8).putLong(v).array(); }
    static byte[] uuidToBytes(Uuid id) {
        return ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }

    static int intFromBytes(byte[] b)   { return ByteBuffer.wrap(b).getInt(); }
    static long longFromBytes(byte[] b) { return ByteBuffer.wrap(b).getLong(); }
    static Uuid uuidFromBytes(byte[] b) {
        return new Uuid(ByteBuffer.wrap(b, 0, 8).getLong(), ByteBuffer.wrap(b, 8, 8).getLong());
    }
}
