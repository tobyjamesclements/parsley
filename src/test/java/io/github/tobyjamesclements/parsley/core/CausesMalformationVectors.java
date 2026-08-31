package io.github.tobyjamesclements.parsley.core;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The one spelling of the malformation battery: every class of damaged causes bytes the
 * frozen grammar refuses, as (family, label, bytes, diagnosis fragment) vectors.
 *
 * <p>Two decoders must refuse these identically — the engine's own
 * ({@code CausesCodecTest}) and the session companion's ({@code CausalPastMalformationTest},
 * D99) — and a class pinned for one but not the other is silent drift in exactly the
 * property the companion promises ("exactly as strict as the engine's"). Sharing the
 * vectors makes the mirror structural: a new malformation class is added <em>here</em>, and
 * both decoders' catalogue sweeps pick it up with no further test change. Public across
 * test packages for the same reason {@code EngineTestFactory} is.
 *
 * <p>{@link #all()} builds fresh arrays on every call, so callers may mutate what they
 * receive without poisoning another test.
 */
public final class CausesMalformationVectors {

    /**
     * One malformed encoding and the refusal it must draw.
     *
     * @param family            the malformation class, grouping vectors a per-class test
     *                          exercises together
     * @param label             names the vector in failure messages
     * @param bytes             the malformed header value; null models an absent value
     * @param diagnosisFragment a fragment the refusal's message must contain
     */
    public record Vector(String family, String label, byte[] bytes, String diagnosisFragment) {
    }

    private static final ChannelId CH_A = new ChannelId(new UUID(1, 1), 0);
    private static final ChannelId CH_B = new ChannelId(new UUID(1, 2), 3);

    private CausesMalformationVectors() {
    }

    private static ByteBuffer header(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(CausesCodec.FORMAT_VERSION);
        return buffer;
    }

    /**
     * Builds every vector, with fresh byte arrays.
     *
     * @return the whole battery, one entry per (family, spelling) pair
     */
    public static List<Vector> all() {
        byte[] twoChannels = CausesCodec.encode(Causes.of(Map.of(CH_A, 41L, CH_B, 7L)));

        ByteBuffer negativePosition = header(1 + 1 + 16 + 1 + 9);
        negativePosition.put((byte) 1);
        negativePosition.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(-5);

        ByteBuffer zeroTopicId = header(1 + 1 + 16 + 1 + 9);
        zeroTopicId.put((byte) 1);
        zeroTopicId.putLong(0).putLong(0).put((byte) 1).put((byte) 0).putLong(7);

        ByteBuffer topicsDescending = header(1 + 1 + 2 * 26);
        topicsDescending.put((byte) 2);
        topicsDescending.putLong(1).putLong(2).put((byte) 1).put((byte) 3).putLong(1);
        topicsDescending.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(2);

        ByteBuffer topicsDuplicate = header(1 + 1 + 2 * 26);
        topicsDuplicate.put((byte) 2);
        topicsDuplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        topicsDuplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 1).putLong(2);

        ByteBuffer partitionsDescending = header(1 + 1 + 16 + 1 + 2 * 9);
        partitionsDescending.put((byte) 1);
        partitionsDescending.putLong(1).putLong(1).put((byte) 2);
        partitionsDescending.put((byte) 5).putLong(1);
        partitionsDescending.put((byte) 2).putLong(2);

        ByteBuffer partitionsDuplicate = header(1 + 1 + 16 + 1 + 2 * 9);
        partitionsDuplicate.put((byte) 1);
        partitionsDuplicate.putLong(1).putLong(1).put((byte) 2);
        partitionsDuplicate.put((byte) 2).putLong(1);
        partitionsDuplicate.put((byte) 2).putLong(2);

        ByteBuffer zeroPartitions = header(1 + 1 + 16 + 1);
        zeroPartitions.put((byte) 1);
        zeroPartitions.putLong(1).putLong(1).put((byte) 0);

        // Count miscounts mutate real encoder output, so the vectors cannot drift from the
        // grammar. At these values every count is a one-byte varint: the topic count at
        // offset 1, the first group's partition count at offset 18. An overstated count
        // runs the decoder off the end (truncation); an understated one leaves bytes over.
        byte[] oneTopic = CausesCodec.encode(Causes.of(Map.of(CH_A, 1L)));
        byte[] topicsOverstated = oneTopic.clone();
        topicsOverstated[1] = 2;
        byte[] topicsUnderstated = twoChannels.clone();
        topicsUnderstated[1] = 1;
        byte[] partitionsOverstated = oneTopic.clone();
        partitionsOverstated[18] = 2;
        byte[] partitionsUnderstated = CausesCodec.encode(
                Causes.of(Map.of(CH_A, 1L, new ChannelId(CH_A.topicId(), 6), 2L)));
        partitionsUnderstated[18] = 1;

        ByteBuffer paddedTopicCount = header(1 + 2 + 26);
        paddedTopicCount.put((byte) 0x81).put((byte) 0x00);
        paddedTopicCount.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);

        ByteBuffer paddedPartition = header(1 + 1 + 16 + 1 + 2 + 8);
        paddedPartition.put((byte) 1);
        paddedPartition.putLong(1).putLong(1).put((byte) 1);
        paddedPartition.put((byte) 0x85).put((byte) 0x00).putLong(1);

        byte[][] overflowProbes = {
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01},
        };
        Vector[] overflow = new Vector[overflowProbes.length];
        for (int i = 0; i < overflowProbes.length; i++) {
            ByteBuffer buffer = header(1 + overflowProbes[i].length + 26);
            buffer.put(overflowProbes[i]);
            buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
            overflow[i] = new Vector("varint-overflow", "overflow probe " + i,
                    buffer.array(), "exceeds the non-negative int range");
        }

        return List.of(
                new Vector("null-value", "null header value", null, "null value"),
                new Vector("unknown-version", "version zero", new byte[] {0, 0}, "unknown causes format version"),
                new Vector("unknown-version", "snapshot-era version 2", new byte[] {2, 0}, "unknown causes format version"),
                new Vector("unknown-version", "unassigned version 3", new byte[] {3, 0}, "unknown causes format version"),
                new Vector("unknown-version", "far version 9", new byte[] {9, 0}, "unknown causes format version"),
                new Vector("truncation", "inside a position", java.util.Arrays.copyOf(twoChannels, twoChannels.length - 3), "truncated causes header"),
                new Vector("truncation", "inside a topic id", java.util.Arrays.copyOf(twoChannels, twoChannels.length - 20), "truncated causes header"),
                new Vector("truncation", "inside a varint", new byte[] {CausesCodec.FORMAT_VERSION, (byte) 0xAC}, "truncated causes header"),
                new Vector("truncation", "below the version byte", new byte[0], "truncated causes header"),
                new Vector("trailing", "one surplus byte", java.util.Arrays.copyOf(twoChannels, twoChannels.length + 1), "trailing bytes"),
                new Vector("negative-position", "position -5", negativePosition.array(), "negative position"),
                new Vector("zero-topic-id", "reserved zero id", zeroTopicId.array(), "zero topic id"),
                new Vector("topic-order", "topics descending", topicsDescending.array(), "topics not strictly ascending"),
                new Vector("topic-order", "topic duplicated", topicsDuplicate.array(), "topics not strictly ascending"),
                new Vector("partition-order", "partitions descending", partitionsDescending.array(), "partitions not strictly ascending"),
                new Vector("partition-order", "partition duplicated", partitionsDuplicate.array(), "partitions not strictly ascending"),
                new Vector("zero-partitions", "empty topic group", zeroPartitions.array(), "zero partitions"),
                new Vector("count-miscount", "topics overstated", topicsOverstated, "truncated causes header"),
                new Vector("count-miscount", "topics understated", topicsUnderstated, "trailing bytes"),
                new Vector("count-miscount", "partitions overstated", partitionsOverstated, "truncated causes header"),
                new Vector("count-miscount", "partitions understated", partitionsUnderstated, "trailing bytes"),
                new Vector("non-minimal-varint", "padded topic count", paddedTopicCount.array(), "non-minimal varint"),
                new Vector("non-minimal-varint", "padded partition", paddedPartition.array(), "non-minimal varint"),
                overflow[0], overflow[1], overflow[2]);
    }

    /**
     * The vectors of one family.
     *
     * @param family the malformation class to select
     * @return that family's vectors, never empty for a family the battery holds
     */
    public static List<Vector> family(String family) {
        return all().stream().filter(vector -> vector.family().equals(family)).toList();
    }
}
