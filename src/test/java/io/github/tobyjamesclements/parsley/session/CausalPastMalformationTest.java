package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the token parser refuses what the engine's codec refuses: every
 * malformation class {@code CausesCodecTest} pins is re-pinned here through
 * {@link CausalPast#decode}, because a salvaging token parser is the same hazard as a
 * salvaging codec (issue #96). A token decoded leniently from damaged bytes is a weaker
 * frontier than the one minted, and a weaker frontier silently weakens the session
 * guarantee — so these stay red if decode is ever rewritten around the codec rather than
 * through it.
 *
 * <p>Vectors are the codec test's own: encode-then-mutate where real output exists, and
 * hand-built buffers where the malformation cannot be produced by the encoder.
 */
class CausalPastMalformationTest {
    private static final ChannelId CH_A = new ChannelId(new UUID(1, 1), 0);
    private static final ChannelId CH_B = new ChannelId(new UUID(1, 2), 3);

    private static ByteBuffer header(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(CausesCodec.FORMAT_VERSION);
        return buffer;
    }

    private static CausesCodec.UndecodableMetadataException refuses(byte[] encoded) {
        return assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausalPast.decode(encoded));
    }

    /** Rejects a null value: an absent token is no token, not an empty one. */
    @Test
    void rejectsNullValue() {
        refuses(null);
    }

    /** Rejects unknown versions, the retired flat grammar's byte included. */
    @Test
    void rejectsUnknownVersion() {
        byte[] encoded = CausalPast.none().encode();
        for (byte version : new byte[] {1, 3, 9}) {
            encoded[0] = version;
            assertThrows(CausesCodec.UndecodableMetadataException.class, () -> CausalPast.decode(encoded.clone()),
                    "version byte " + version + " must be undecodable");
        }
    }

    /** Rejects truncation at every depth, and a surplus byte, as classified refusals. */
    @Test
    void rejectsTruncationAndTrailingBytes() {
        byte[] encoded = CausalPast.of(Causes.of(Map.of(CH_A, 41L, CH_B, 7L))).encode();
        for (int keep : new int[] {encoded.length - 3, encoded.length - 20, 0}) {
            CausesCodec.UndecodableMetadataException thrown =
                    refuses(java.util.Arrays.copyOf(encoded, keep));
            assertTrue(thrown.getMessage().contains("truncated"),
                    () -> "truncation to " + keep + " bytes must classify, not throw raw: " + thrown.getMessage());
        }
        CausesCodec.UndecodableMetadataException midVarint =
                refuses(new byte[] {CausesCodec.FORMAT_VERSION, (byte) 0xAC});
        assertTrue(midVarint.getMessage().contains("truncated"),
                () -> "truncation inside a varint must classify: " + midVarint.getMessage());
        CausesCodec.UndecodableMetadataException trailing =
                refuses(java.util.Arrays.copyOf(encoded, encoded.length + 1));
        assertTrue(trailing.getMessage().contains("trailing bytes"),
                () -> "the diagnosis must name the surplus: " + trailing.getMessage());
    }

    /** Rejects a negative position: a token cannot demand a position no log can hold. */
    @Test
    void rejectsNegativePosition() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(-5);
        assertTrue(refuses(buffer.array()).getMessage().contains("negative position"));
    }

    /** Rejects the reserved zero topic id: a forged token must not plant an unanswerable id. */
    @Test
    void rejectsZeroTopicId() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1 + 9);
        buffer.put((byte) 1);
        buffer.putLong(0).putLong(0).put((byte) 1).put((byte) 0).putLong(7);
        assertTrue(refuses(buffer.array()).getMessage().contains("zero topic id"));
    }

    /** Rejects topics out of order or duplicated: one spelling per past survives transport. */
    @Test
    void rejectsTopicsOutOfOrderOrDuplicate() {
        ByteBuffer descending = header(1 + 1 + 2 * 26);
        descending.put((byte) 2);
        descending.putLong(1).putLong(2).put((byte) 1).put((byte) 3).putLong(1);
        descending.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(2);
        assertTrue(refuses(descending.array()).getMessage().contains("topics not strictly ascending"));

        ByteBuffer duplicate = header(1 + 1 + 2 * 26);
        duplicate.put((byte) 2);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        duplicate.putLong(1).putLong(1).put((byte) 1).put((byte) 1).putLong(2);
        assertTrue(refuses(duplicate.array()).getMessage().contains("topics not strictly ascending"));
    }

    /** Rejects partitions out of order or duplicated within a group. */
    @Test
    void rejectsPartitionsOutOfOrderOrDuplicate() {
        ByteBuffer descending = header(1 + 1 + 16 + 1 + 2 * 9);
        descending.put((byte) 1);
        descending.putLong(1).putLong(1).put((byte) 2);
        descending.put((byte) 5).putLong(1);
        descending.put((byte) 2).putLong(2);
        assertTrue(refuses(descending.array()).getMessage().contains("partitions not strictly ascending"));

        ByteBuffer duplicate = header(1 + 1 + 16 + 1 + 2 * 9);
        duplicate.put((byte) 1);
        duplicate.putLong(1).putLong(1).put((byte) 2);
        duplicate.put((byte) 2).putLong(1);
        duplicate.put((byte) 2).putLong(2);
        assertTrue(refuses(duplicate.array()).getMessage().contains("partitions not strictly ascending"));
    }

    /** Rejects a topic group naming zero partitions. */
    @Test
    void rejectsZeroPartitionTopic() {
        ByteBuffer buffer = header(1 + 1 + 16 + 1);
        buffer.put((byte) 1);
        buffer.putLong(1).putLong(1).put((byte) 0);
        assertTrue(refuses(buffer.array()).getMessage().contains("zero partitions"));
    }

    /** Rejects miscounted topic and partition counts against the bytes present. */
    @Test
    void rejectsCountMiscounts() {
        byte[] oneTopic = CausalPast.of(Causes.of(Map.of(CH_A, 1L))).encode();
        byte[] topicsOverstated = oneTopic.clone();
        topicsOverstated[1] = 2;
        refuses(topicsOverstated);

        byte[] twoTopics = CausalPast.of(Causes.of(Map.of(CH_A, 1L, CH_B, 2L))).encode();
        byte[] topicsUnderstated = twoTopics.clone();
        topicsUnderstated[1] = 1;
        refuses(topicsUnderstated);

        byte[] partitionsOverstated = oneTopic.clone();
        partitionsOverstated[18] = 2;
        refuses(partitionsOverstated);

        byte[] twoPartitions = CausalPast.of(
                Causes.of(Map.of(CH_A, 1L, new ChannelId(CH_A.topicId(), 6), 2L))).encode();
        byte[] partitionsUnderstated = twoPartitions.clone();
        partitionsUnderstated[18] = 1;
        refuses(partitionsUnderstated);
    }

    /** Rejects non-minimal varints: two spellings of one token would defeat comparison by bytes. */
    @Test
    void rejectsNonMinimalVarint() {
        ByteBuffer paddedTopicCount = header(1 + 2 + 26);
        paddedTopicCount.put((byte) 0x81).put((byte) 0x00);
        paddedTopicCount.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
        assertTrue(refuses(paddedTopicCount.array()).getMessage().contains("non-minimal varint"));

        ByteBuffer paddedPartition = header(1 + 1 + 16 + 1 + 2 + 8);
        paddedPartition.put((byte) 1);
        paddedPartition.putLong(1).putLong(1).put((byte) 1);
        paddedPartition.put((byte) 0x85).put((byte) 0x00).putLong(1);
        assertTrue(refuses(paddedPartition.array()).getMessage().contains("non-minimal varint"));
    }

    /** Rejects varints past the non-negative int range, the silent-alias spellings included. */
    @Test
    void rejectsVarintBeyondTheNonNegativeIntRange() {
        byte[][] probes = {
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08},
                {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01},
        };
        for (byte[] probe : probes) {
            ByteBuffer buffer = header(1 + probe.length + 26);
            buffer.put(probe);
            buffer.putLong(1).putLong(1).put((byte) 1).put((byte) 0).putLong(1);
            assertTrue(refuses(buffer.array()).getMessage().contains("exceeds the non-negative int range"));
        }
    }
}
