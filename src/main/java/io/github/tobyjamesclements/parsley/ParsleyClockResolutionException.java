package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * Thrown at ingest when an inbound record's {@code parsley-causal-dependencies} header is present but
 * cannot be decoded into a {@link ParsleyClock} — a corrupt or truncated header, or one written in an
 * unsupported wire version.
 *
 * <p>The record's key and value were already deserialised fine by Kafka Streams before ingest ever runs
 * — only this header failed to decode. {@link ParsleyProcessor} fails the task fast rather than
 * forwarding the record on an unknown causal premise — it was never buffered and its source offset is
 * not committed past it, so it is reprocessed on restart. This is a {@link RuntimeException}, fatal
 * only to the owning Streams task, never crashing the JVM.
 *
 * <p>{@link #details()} carries the source coordinate and the encoded header length for an operator
 * log — never the payload bytes themselves.
 */
final class ParsleyClockResolutionException extends ParsleyCoordinateException {

    private final byte[] encodedDependencies;
    private final String details;

    ParsleyClockResolutionException(String topic, Uuid topicId, int partition, long offset,
                                    byte[] encodedDependencies, String details, Throwable cause) {
        super("failed to resolve the causal-dependencies header on " + topic + "-" + partition + "@" + offset
                + "; the record was not forwarded", cause, topic, topicId, partition, offset);
        this.encodedDependencies = encodedDependencies;
        this.details = details;
    }

    /** The raw, undecodable header bytes, for operator forensics. */
    byte[] encodedDependencies() {
        return encodedDependencies;
    }

    /** Operator diagnostic: the record's coordinate and encoded header length (no payload bytes). */
    String details() {
        return details;
    }
}
