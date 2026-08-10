package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that corrupt ordering state raises the classified refusal, never a raw
 * runtime exception and never an allocation sized by the corruption.
 *
 * <p>The blob and the store entries arrive verbatim from the changelog; a corrupted record
 * or a misdirected producer reaches these decode paths directly, and
 * {@code docs/failing-closed.md} promises a diagnosed stop for them. Length and count
 * fields are hostile input: each is validated against the bytes actually present before
 * anything is allocated from it.
 */
class StoreCodecCorruptionTest {
    private static final ChannelId CH = new ChannelId(new UUID(1, 2), 0);
    private static final int CAUSE_ENTRY = ChannelId.ENCODED_LENGTH + Long.BYTES;

    /** A valid blob: key "k", value "v", one header ("h", [1]), one cause. */
    private static byte[] validBlob() {
        return StoreCodec.encodeHeld(7L, new byte[] {'k'}, new byte[] {'v'},
                List.of(new HeaderKV("h", new byte[] {1})), Causes.of(Map.of(CH, 3L)));
    }

    // Layout of validBlob(): version@0, timestamp@1, flags@9, keyLen@10, key@14,
    // valueLen@15, value@19, headerCount@20, headerKeyLen@24, headerKey@28,
    // headerValueLen@29, headerValue@33, causeCount@34, causeEntry@38.
    private static byte[] patched(int offset, int value) {
        byte[] blob = validBlob();
        ByteBuffer.wrap(blob).putInt(offset, value);
        return blob;
    }

    private static ParsleyFailClosedException refusal(byte[] blob) {
        ParsleyFailClosedException thrown =
                assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.decodeHeld(blob));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
        assertFalse(thrown.getMessage().isBlank(), "the refusal must carry a diagnosis");
        return thrown;
    }

    @Test
    void roundTripRestoresEveryFieldByteForByte() {
        StoreCodec.HeldBlob decoded = StoreCodec.decodeHeld(validBlob());
        assertEquals(7L, decoded.timestamp());
        assertArrayEquals(new byte[] {'k'}, decoded.key());
        assertArrayEquals(new byte[] {'v'}, decoded.value());
        assertEquals(1, decoded.headers().size());
        assertEquals("h", decoded.headers().get(0).key());
        assertArrayEquals(new byte[] {1}, decoded.headers().get(0).value());
        assertEquals(Causes.of(Map.of(CH, 3L)), decoded.causes());
    }

    @Test
    void roundTripDistinguishesNullsFromEmpty() {
        byte[] blob = StoreCodec.encodeHeld(0L, null, new byte[0],
                List.of(new HeaderKV("n", null)), Causes.of(Map.of()));
        StoreCodec.HeldBlob decoded = StoreCodec.decodeHeld(blob);
        assertNull(decoded.key());
        assertArrayEquals(new byte[0], decoded.value());
        assertNull(decoded.headers().get(0).value());
    }

    @Test
    void negativeKeyLengthRaisesTheRefusal() {
        refusal(patched(10, -2));
    }

    @Test
    void oversizedKeyLengthRaisesTheRefusalWithoutAllocating() {
        refusal(patched(10, Integer.MAX_VALUE - 2));
    }

    @Test
    void oversizedValueLengthRaisesTheRefusal() {
        refusal(patched(15, Integer.MAX_VALUE - 2));
    }

    @Test
    void oversizedHeaderCountRaisesTheRefusalWithoutAllocating() {
        refusal(patched(20, Integer.MAX_VALUE));
    }

    @Test
    void negativeHeaderCountRaisesTheRefusal() {
        refusal(patched(20, -1));
    }

    @Test
    void oversizedHeaderKeyLengthRaisesTheRefusal() {
        refusal(patched(24, Integer.MAX_VALUE - 2));
    }

    @Test
    void oversizedHeaderValueLengthRaisesTheRefusal() {
        refusal(patched(29, Integer.MAX_VALUE - 2));
    }

    @Test
    void headerValueLengthBelowNullSentinelRaisesTheRefusal() {
        refusal(patched(29, Integer.MIN_VALUE));
    }

    @Test
    void miscountedCausesRaiseTheRefusal() {
        byte[] blob = validBlob();
        refusal(patched(blob.length - 4 - CAUSE_ENTRY, 2));
    }

    @Test
    void truncatedBlobRaisesTheRefusal() {
        byte[] blob = validBlob();
        refusal(Arrays.copyOf(blob, blob.length - 4));
    }

    @Test
    void trailingBytesRaiseTheRefusal() {
        byte[] blob = validBlob();
        refusal(Arrays.copyOf(blob, blob.length + 3));
    }

    @Test
    void unknownBlobVersionRaisesTheRefusal() {
        byte[] blob = validBlob();
        blob[0] = 9;
        refusal(blob);
    }

    @Test
    void decodeLongRequiresExactlyEightBytes() {
        assertEquals(3L, StoreCodec.decodeLong(StoreCodec.encodeLong(3L)));
        for (int length : new int[] {0, 4, 12}) {
            ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                    () -> StoreCodec.decodeLong(new byte[length]));
            assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
        }
    }

    @Test
    void malformedChannelKeysRaiseTheRefusal() {
        byte[] shortKey = {StoreCodec.TAG_FED_UP_TO, 1, 2, 3};
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.channelOfChannelKey(shortKey));
        byte[] longKey = new byte[1 + ChannelId.ENCODED_LENGTH + 1];
        longKey[0] = StoreCodec.TAG_FRONTIER;
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.channelOfChannelKey(longKey));
        assertEquals(CH, StoreCodec.channelOfChannelKey(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, CH)));
    }

    @Test
    void malformedHeldKeysRaiseTheRefusal() {
        byte[] shortKey = {StoreCodec.TAG_HELD, 1, 2, 3};
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.channelOfHeldKey(shortKey));
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.positionOfHeldKey(shortKey));
        byte[] good = StoreCodec.heldKey(CH, 9L);
        assertEquals(CH, StoreCodec.channelOfHeldKey(good));
        assertEquals(9L, StoreCodec.positionOfHeldKey(good));
    }

    /** A corrupt store entry stops engine construction with the diagnosis, not a raw crash. */
    @Test
    void engineRestoreRefusesACorruptFedUpToValue() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        store.put(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, CH), new byte[4]);

        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in"), store, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
    }

    /** A malformed held key in the store stops engine construction with the diagnosis. */
    @Test
    void engineRestoreRefusesAMalformedHeldKey() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        store.put(new byte[] {StoreCodec.TAG_HELD, 1, 2}, new byte[] {0});

        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in"), store, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
        assertTrue(thrown.getMessage().contains("held key"),
                () -> "diagnosis should name the malformed key: " + thrown.getMessage());
    }
}
