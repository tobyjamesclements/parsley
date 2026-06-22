package io.parsley;

/**
 * Thrown when a held record's key or value cannot be deserialised from the buffer store on the
 * forward path (drain or eviction) — typically because the registry state for its subject changed
 * after the record was buffered (e.g. the writer schema id was deleted, or a redeployed reader
 * schema is incompatible).
 *
 * <p>How a decode failure is handled follows {@code parsley.buffer.deserialization.failure.policy}
 * (read once at processor {@code init()}): the default {@code fail} fails fast — the offending entry
 * stays in the buffer store (the forward path deserialises before it removes), so the record is
 * preserved for recovery once the registry is fixed or rolled back — while {@code continue} skips it
 * (dropped, logged, counted as a violation). Either way this is a {@link RuntimeException}, fatal
 * only to the owning Streams task, never crashing the JVM; and the index-restore path does not
 * deserialise key/value, so a poison record never blocks startup.
 *
 * <p>{@link #details()} carries everything decodable <em>without</em> the user serde (coordinate,
 * dependencies, header keys, payload lengths, schema id) for an operator log — never the payload
 * bytes themselves; the authoritative bytes live in the buffer changelog topic.
 */
final class ParsleyBufferDeserializationException extends RuntimeException {

    private final String topic;
    private final int partition;
    private final long offset;
    private final String details;

    ParsleyBufferDeserializationException(String topic, int partition, long offset, int schemaId,
                                          String details, Throwable cause) {
        super("failed to deserialise buffered record from " + topic + "-" + partition + "@" + offset
                + (schemaId >= 0 ? " (writer schema id " + schemaId + ")" : "")
                + "; the record remains in the buffer changelog for recovery", cause);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.details = details;
    }

    String topic() {
        return topic;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }

    /** Operator diagnostic: the held record's metadata (no payload bytes). */
    String details() {
        return details;
    }
}
