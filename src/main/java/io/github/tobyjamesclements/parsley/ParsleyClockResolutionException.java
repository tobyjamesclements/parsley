package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * Thrown at ingest when an inbound record's {@code parsley-causal-dependencies} header is present but
 * cannot be decoded into a {@link ParsleyClock} — a corrupt or truncated header, or one written in an
 * unsupported wire version.
 *
 * <p>How an unresolvable header is handled follows {@code parsley.clock.resolution.failure.policy}
 * (read once at processor {@code init()}): the default {@code fail} fails fast — the record was never
 * buffered and its source offset is not committed past it, so it is reprocessed on restart, rather
 * than forwarded on an unknown causal premise — while {@code continue} treats the header as empty
 * (vacuously satisfied) and forwards the record anyway (logged, counted as a violation). Either way
 * this is a {@link RuntimeException}, fatal only to the owning Streams task, never crashing the JVM.
 *
 * <p>{@link #details()} carries the source coordinate and the encoded header length for an operator
 * log — never the payload bytes themselves.
 */
final class ParsleyClockResolutionException extends RuntimeException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;
    private final String details;

    ParsleyClockResolutionException(String topic, Uuid topicId, int partition, long offset,
                                    String details, Throwable cause) {
        super("failed to resolve the causal-dependencies header on " + topic + "-" + partition + "@" + offset
                + "; the record was not forwarded", cause);
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.offset = offset;
        this.details = details;
    }

    String topic() {
        return topic;
    }

    /** The source topic's stable UUID. */
    Uuid topicId() {
        return topicId;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }

    /** Operator diagnostic: the record's coordinate and encoded header length (no payload bytes). */
    String details() {
        return details;
    }
}
