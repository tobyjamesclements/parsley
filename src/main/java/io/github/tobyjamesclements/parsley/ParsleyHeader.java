package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A single record header — a {@code (key, value)} pair — plus Parsley's header-key vocabulary.
 *
 * <p>This type owns the names of every header Parsley reads or writes and the rule that distinguishes
 * Parsley's internal routing headers (the {@code _parsley_*} prefix) from user headers, so that
 * knowledge lives with the header type rather than scattered as loose constants.
 *
 * @param key   the header name; must not be {@code null} or empty
 * @param value the raw header bytes; may be {@code null}
 */
record ParsleyHeader(String key, byte @Nullable [] value) {

    /** Prefix marking a header as Parsley-internal routing metadata, stripped before user delivery. */
    static final String INTERNAL_PREFIX = "_parsley_";

    /** Header carrying a record's serialised causal dependency clock. */
    static final String CAUSAL_DEPENDENCIES = "parsley-causal-dependencies";

    /**
     * Header marking a record as a Parsley protocol watermark. A watermark carries no business
     * payload (null key, null value) and exists solely to propagate the emitting node's completeness
     * frontier to downstream processors when the user delegate did not forward any business record
     * for the delivered input. The {@code _parsley_} prefix means it is stripped from user view by
     * {@code ParsleyMessage.userHeaders} and from any public header API.
     */
    static final String WATERMARK = "_parsley_watermark";

    /**
     * Header marking a record as a Parsley topology epoch-boundary marker. Written by the Topology
     * Co-ordinator to every input channel; on consuming it a processor adopts the new epoch's lower
     * bounds into its {@link ParsleyEpochState} (an overlapping-epoch transition). Like a watermark it
     * carries no business payload and is never delivered to the user delegate or buffered; the value
     * holds the serialised {@link EpochBoundary}. The {@code _parsley_} prefix strips it from user view.
     */
    static final String EPOCH_BOUNDARY = "_parsley_epoch_boundary";

    /**
     * Header marking a record as a Parsley topology epoch-snapshot marker — the first marker of the
     * Mattern two-marker cut. Written by the Topology Co-ordinator to every input channel; on consuming
     * it a processor publishes its current completeness frontier to the coordinator (see
     * {@link ParsleyEpochSnapshotPublisher}), which merge-mins the published clocks into the next
     * epoch's lower bounds. Like a watermark it carries no business payload and is never delivered to
     * the user delegate or buffered; the {@code _parsley_} prefix strips it from user view.
     */
    static final String EPOCH_SNAPSHOT = "_parsley_epoch_snapshot";

    // Explicit canonical constructor: NullAway does not propagate the type-use @Nullable from an
    // array record component to the implicit constructor parameter, so annotate it here directly.
    ParsleyHeader(String key, byte @Nullable [] value) {
        Objects.requireNonNull(key, "header key must not be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("header key must not be empty");
        }
        this.key = key;
        this.value = value;
    }

    /** Returns {@code true} if this is a Parsley-internal routing header (the {@code _parsley_} prefix). */
    boolean isInternal() {
        return key.startsWith(INTERNAL_PREFIX);
    }

    /**
     * Returns a fresh, empty, mutable {@link Headers} to populate via {@code add(String, byte[])}.
     * Kafka exposes no public {@code Headers} factory and its only implementation lives in an
     * {@code internals} package; a throwaway {@link ProducerRecord} hands back an empty mutable
     * instance through the public API, which is what we want without depending on that internals type.
     */
    static Headers mutableHeaders() {
        return new ProducerRecord<byte[], byte[]>("", null, null).headers();
    }

    static byte[] uuidToBytes(Uuid id) {
        return ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }

    static Uuid uuidFromBytes(byte[] b) {
        return new Uuid(ByteBuffer.wrap(b, 0, 8).getLong(), ByteBuffer.wrap(b, 8, 8).getLong());
    }
}
