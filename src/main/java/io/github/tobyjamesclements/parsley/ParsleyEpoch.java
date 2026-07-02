package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * The wire format and lookup abstraction for the topology epoch's {@code startsAt} bounds — the
 * proven lower edge of the causal domain, per {@code (topicId, partition)} coordinate.
 *
 * <p>Epoch bounds live in a compacted internal topic, one record per coordinate: the key encodes
 * {@code (topicId, partition)} and the value encodes {@code startsAt}. A {@link CausalStreams} stage
 * mirrors that topic into a Kafka Streams global store (always current, one replica per instance),
 * and the delivery gate reads it live through a {@link View}: a dependency on
 * {@code (topicId, partition, offset)} with {@code offset < startsAt(topicId, partition)} references a
 * coordinate outside this epoch's domain (a prior, closed epoch) and is stripped before the
 * completeness check. A coordinate with no recorded bound reads as {@link #NO_BOUND} (the minimum
 * {@code startsAt}, so nothing is below it): it is gated normally over its full history, exactly as it
 * is today. Bounding a coordinate is therefore always an explicit, authored act — the conservative,
 * correct default is to strip nothing until a finite {@code startsAt} is written, never to strip a
 * coordinate merely because it is absent (which would deliver its dependents early).
 *
 * <p>This class is a stateless codec plus the {@link View} it produces; the durable state lives
 * entirely in the global store, so nothing here is persisted per task.
 */
final class ParsleyEpoch {

    /** Leading byte of the value wire format. */
    static final byte VALUE_VERSION = 1;

    /**
     * The {@code startsAt} of a coordinate with no recorded bound: the minimum possible value, so
     * {@code offset < NO_BOUND} is false for every offset and nothing is ever stripped. Gating such a
     * coordinate over its full history is the correct, conservative default.
     */
    static final long NO_BOUND = Long.MIN_VALUE;

    private static final int KEY_LENGTH = 20;   // 8 + 8 (UUID) + 4 (partition)
    private static final int VALUE_LENGTH = 9;  // 1 (version) + 8 (startsAt)

    private ParsleyEpoch() {}

    /** Encodes the {@code (topicId, partition)} coordinate as a compacted-topic key: {@code [MSB:8][LSB:8][partition:4]}. */
    static byte[] key(Uuid topicId, int partition) {
        return ByteBuffer.allocate(KEY_LENGTH)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .putInt(partition)
                .array();
    }

    /** Encodes a {@code startsAt} bound as a compacted-topic value: {@code [version:1][startsAt:8]}. */
    static byte[] value(long startsAt) {
        return ByteBuffer.allocate(VALUE_LENGTH)
                .put(VALUE_VERSION)
                .putLong(startsAt)
                .array();
    }

    /**
     * Decodes a {@code startsAt} value written by {@link #value(long)}.
     *
     * @throws IllegalStateException if {@code bytes} is the wrong length or an unrecognised version
     */
    static long startsAt(byte[] bytes) {
        if (bytes.length != VALUE_LENGTH) {
            throw new IllegalStateException(
                    "epoch value must be " + VALUE_LENGTH + " bytes, got " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version != VALUE_VERSION) {
            throw new IllegalStateException(
                    "unsupported epoch value version: " + version + " (expected " + VALUE_VERSION + ")");
        }
        return buffer.getLong();
    }

    /**
     * A live view of the {@code startsAt} bounds the delivery gate consults. Returns {@link #NO_BOUND}
     * for any coordinate with no recorded bound, so a dependency on it is never stripped (gated
     * normally over its full history).
     */
    @FunctionalInterface
    interface View {
        long startsAt(Uuid topicId, int partition);

        /** The no-op view used when epoch bounding is disabled: every coordinate is unbounded. */
        View NONE = (topicId, partition) -> NO_BOUND;
    }

    /**
     * A {@link View} backed by the epoch global store, read live at gate time. A missing entry reads
     * as {@link #NO_BOUND} (gated normally); a coordinate is stripped only once a finite bound has
     * been authored for it.
     */
    static View over(ReadOnlyKeyValueStore<byte[], byte[]> store) {
        return (topicId, partition) -> {
            byte @Nullable [] value = store.get(key(topicId, partition));
            return value == null ? NO_BOUND : startsAt(value);
        };
    }
}
