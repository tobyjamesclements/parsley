package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * Thrown when a held record's key or value cannot be deserialised from the buffer store on the
 * forward path (drain or propagate) — typically because the registry state for its subject changed
 * after the record was buffered (e.g. the writer schema id was deleted, or a redeployed reader
 * schema is incompatible).
 *
 * <p>Parsley fails the task fast: the offending entry stays in the buffer store (the
 * forward path deserialises before it removes), so the record is preserved for recovery once the
 * registry is fixed or rolled back. The failure is fatal only to the owning
 * Streams task, never crashing the JVM; the index-restore path does not deserialise key/value, so a
 * poison record never blocks startup. A restart alone does not heal it — the same record poisons the
 * drain again until the external serde state is repaired.
 *
 * <p>{@link #details()} carries everything decodable <em>without</em> the user serde (coordinate,
 * dependencies, header keys, payload lengths, schema id) for an operator log — never the payload
 * bytes themselves.
 */
public final class CausalBufferDeserializationException extends CausalCoordinateException {

    private final String details;

    CausalBufferDeserializationException(String topic, Uuid topicId, int partition, long offset,
                                         int schemaId, String details, Throwable cause) {
        super("failed to deserialise buffered record from " + topic + "-" + partition + "@" + offset
                + (schemaId >= 0 ? " (writer schema id " + schemaId + ")" : "")
                + "; the record remains in the buffer changelog for recovery", cause,
                topic, topicId, partition, offset);
        this.details = details;
    }

    /** Operator diagnostic: the held record's metadata (no payload bytes). */
    public String details() {
        return details;
    }
}
