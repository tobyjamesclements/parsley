package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * Thrown when a held record's key or value cannot be deserialised from the buffer store on the
 * forward path (drain or propagate) — typically because the registry state for its subject changed
 * after the record was buffered (e.g. the writer schema id was deleted, or a redeployed reader
 * schema is incompatible).
 *
 * <p>When a dead-letter sink is configured, {@link ParsleyEngine} dead-letters the record — carrying
 * {@link #rawKey()}/{@link #rawValue()} (the raw bytes the user's own serializer produced, recovered
 * before the failed decode attempt) onto the dead-letter topic — and orphans its coordinate, cascading
 * to any buffered dependent that can now never be satisfied either. Without one, the task fails fast:
 * the offending entry stays in the buffer store (the forward path deserialises before it removes), so
 * the record is preserved for recovery once the registry is fixed or rolled back. Either way this is a
 * {@link RuntimeException}, fatal only to the owning Streams task, never crashing the JVM; and the
 * index-restore path does not deserialise key/value, so a poison record never blocks startup.
 *
 * <p>{@link #details()} carries everything decodable <em>without</em> the user serde (coordinate,
 * dependencies, header keys, payload lengths, schema id) for an operator log — never the payload
 * bytes themselves; those live only in {@link #rawKey()}/{@link #rawValue()} (for the dead-letter
 * record) and the buffer changelog topic, never in a log line.
 */
final class ParsleyBufferDeserializationException extends RuntimeException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final List<ParsleyHeader> headers;
    private final byte @Nullable [] rawKey;
    private final byte @Nullable [] rawValue;
    private final String details;

    ParsleyBufferDeserializationException(String topic, Uuid topicId, int partition, long offset,
                                          long timestamp, List<ParsleyHeader> headers,
                                          byte @Nullable [] rawKey, byte @Nullable [] rawValue,
                                          int schemaId, String details, Throwable cause) {
        super("failed to deserialise buffered record from " + topic + "-" + partition + "@" + offset
                + (schemaId >= 0 ? " (writer schema id " + schemaId + ")" : "")
                + "; the record remains in the buffer changelog for recovery", cause);
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = headers;
        this.rawKey = rawKey;
        this.rawValue = rawValue;
        this.details = details;
    }

    String topic() {
        return topic;
    }

    /** The source topic's stable UUID — used to drive the forwarded-index coordinate on poison-drop. */
    Uuid topicId() {
        return topicId;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }

    /** The held record's original timestamp — carried onto the dead-letter record. */
    long timestamp() {
        return timestamp;
    }

    /** The held record's user headers — carried onto the dead-letter record. */
    List<ParsleyHeader> headers() {
        return headers;
    }

    /** The raw key bytes the user's serializer produced, or {@code null} for a null key. */
    byte @Nullable [] rawKey() {
        return rawKey;
    }

    /** The raw value bytes the user's serializer produced (the bytes the deserializer failed on). */
    byte @Nullable [] rawValue() {
        return rawValue;
    }

    /** Operator diagnostic: the held record's metadata (no payload bytes). */
    String details() {
        return details;
    }
}
