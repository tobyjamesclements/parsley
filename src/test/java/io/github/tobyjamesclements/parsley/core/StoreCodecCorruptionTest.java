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

    /**
     * A tag constant added to {@link StoreCodec} without an entry in {@code STATE_TAGS}
     * marks state the changelog-head-loss refusal never scans, so that state silently
     * escapes the unversioned-state check. This pin reflects over the {@code TAG_} fields
     * so a new tag fails here, by name, rather than drifting — the same shape as
     * {@code SabotageModeAlignmentTest}.
     */
    @Test
    void stateTagsCoverEveryTagConstantExceptVersion() throws Exception {
        java.util.Set<Byte> declaredTags = new java.util.TreeSet<>();
        for (java.lang.reflect.Field field : StoreCodec.class.getDeclaredFields()) {
            if (field.getName().startsWith("TAG_") && !field.getName().equals("TAG_VERSION")) {
                field.setAccessible(true);
                declaredTags.add(field.getByte(null));
            }
        }
        java.util.Set<Byte> stateTags = new java.util.TreeSet<>();
        for (byte tag : StoreCodec.stateTags()) {
            stateTags.add(tag);
        }
        assertEquals(declaredTags, stateTags,
                "STATE_TAGS must equal the TAG_ constants minus TAG_VERSION; the"
                        + " unversioned-state refusal scans exactly this set");
        assertFalse(declaredTags.isEmpty(), "the reflection must have found the TAG_ constants");
    }

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

    /**
     * Only -1 spells null; a non-canonical negative is corruption, not an alternate null.
     * Patched over an actual null sentinel so the blob is otherwise perfectly aligned — a
     * decoder accepting any negative as null would decode this without complaint.
     */
    @Test
    void nonSentinelNegativeHeaderValueLengthRaisesTheRefusal() {
        byte[] blob = StoreCodec.encodeHeld(0L, null, new byte[0],
                List.of(new HeaderKV("n", null)), Causes.of(Map.of()));
        // version@0, timestamp@1, flags@9, valueLen@10, headerCount@14, headerKeyLen@18,
        // key 'n'@22, headerValueLen@23 — the encoder's one null spelling.
        assertEquals(-1, ByteBuffer.wrap(blob).getInt(23), "the layout premise must hold");
        ByteBuffer.wrap(blob).putInt(23, -2);
        refusal(blob);
    }

    /**
     * The header-value length refusal must name the bad length and the bytes actually
     * present — `docs/failing-closed.md` promises a diagnosed stop, and D81 makes naming the
     * condition the rule. The refusal's signature is shared with decodeHeld's catch-all wrap
     * at the bottom, so this pin is message-level: deleting the header-value length guard
     * still refuses — the oversized read underflows and the negative one throws
     * NegativeArraySizeException, both caught and wrapped as a bare "corrupt held blob" —
     * and only these diagnosis assertions go red. Remaining counts follow the validBlob()
     * layout above: the length int at 29 is read at position 33 of the 66-byte blob.
     */
    @Test
    void headerValueLengthDiagnosisNamesTheLengthAndTheBytesRemaining() {
        ParsleyFailClosedException oversized = refusal(patched(29, 1000));
        assertTrue(oversized.getMessage().contains("header value length 1000 with 33 bytes remaining"),
                () -> "the refusal must weigh the declared length against the bytes present: "
                        + oversized.getMessage());
        ParsleyFailClosedException negative = refusal(patched(29, -2));
        assertTrue(negative.getMessage().contains("header value length -2 with 33 bytes remaining"),
                () -> "the refusal must name the non-sentinel negative length: " + negative.getMessage());
    }

    /**
     * The sized-bytes refusal must say which field's length was bad — key, value or header
     * key — and against how many bytes were present (D81). Signature shared with the
     * catch-all wrap, so this pin is message-level: deleting the guard inside readSizedBytes
     * still refuses every probe — negative lengths as NegativeArraySizeException and
     * oversized ones as underflow, wrapped without the diagnosis — and only these assertions
     * go red. Remaining counts follow the validBlob() layout: each length int is read at the
     * offset the layout comment gives plus four, of the 66-byte blob.
     */
    @Test
    void sizedBytesLengthDiagnosisNamesTheFieldAndTheBytesRemaining() {
        ParsleyFailClosedException key = refusal(patched(10, -2));
        assertTrue(key.getMessage().contains("key length -2 with 52 bytes remaining"),
                () -> "the refusal must name the key field's bad length: " + key.getMessage());
        ParsleyFailClosedException value = refusal(patched(15, 1000));
        assertTrue(value.getMessage().contains("value length 1000 with 47 bytes remaining"),
                () -> "the refusal must name the value field's bad length: " + value.getMessage());
        ParsleyFailClosedException headerKey = refusal(patched(24, -7));
        assertTrue(headerKey.getMessage().contains("header key length -7 with 38 bytes remaining"),
                () -> "the refusal must name the header-key field's bad length: " + headerKey.getMessage());
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
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.channelOfEntryKey(shortKey));
        byte[] longKey = new byte[1 + ChannelId.ENCODED_LENGTH + 1];
        longKey[0] = StoreCodec.TAG_FRONTIER;
        assertThrows(ParsleyFailClosedException.class, () -> StoreCodec.channelOfEntryKey(longKey));
        assertEquals(CH, StoreCodec.channelOfEntryKey(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, CH)));
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

    /** A store with no state at all is fresh: stamped with the current version and trusted. */
    @Test
    void engineStampsAndTrustsAFreshStore() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        new ProcessEngine("p", Map.of(CH, "in"), store, 64 * 1024);
        assertArrayEquals(new byte[] {StoreCodec.STORE_FORMAT_VERSION}, store.get(StoreCodec.versionKey()));
    }

    /**
     * A store carrying ordering state but no version entry is evidence the changelog head —
     * where the version is written, in the earliest transaction — has been lost, and an
     * unknowable amount of state with it. It must be refused, not re-stamped and trusted.
     */
    @Test
    void engineRefusesAnUnversionedStoreThatContainsState() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, CH), StoreCodec.encodeLong(5L));

        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in"), store, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
        assertTrue(thrown.getMessage().contains("version"),
                () -> "diagnosis should name the missing version entry: " + thrown.getMessage());
    }

    /** The refusal fires whichever class of record survives — a lone held message included. */
    @Test
    void engineRefusesAnUnversionedStoreWithOnlyHeldState() {
        MemoryOrderingStore store = new MemoryOrderingStore();
        store.put(StoreCodec.heldKey(CH, 3L), new byte[] {0});

        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in"), store, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
    }

    /**
     * The audit's empirical probe, kept as a permanent pin: state written by a real engine,
     * restored with only its version entry (and everything before it) missing — as when the
     * changelog's oldest segment ages out under a mis-set cleanup.policy.
     */
    @Test
    void engineRefusesARealStoreStrippedOfItsVersionEntry() {
        MemoryOrderingStore original = new MemoryOrderingStore();
        ProcessEngine engine = new ProcessEngine("p", Map.of(CH, "in"), original, 64 * 1024);
        engine.onReceive(new ReceivedMessage(CH, 0L, 1L, new byte[] {'k'}, new byte[] {'v'}, List.of()));

        MemoryOrderingStore stripped = new MemoryOrderingStore();
        for (byte tag : StoreCodec.stateTags()) {
            original.scanPrefix(StoreCodec.tagPrefix(tag), stripped::put);
        }

        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in"), stripped, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
    }

    /**
     * Restore trusts the store's promised scan order — the deque head must be the minimum
     * held position — so a store breaking that contract is refused, not silently inverted.
     */
    @Test
    void engineRefusesHeldStateVisitedOutOfOrder() {
        ChannelId other = new ChannelId(new UUID(5, 5), 0);
        MemoryOrderingStore inner = new MemoryOrderingStore();
        ProcessEngine writer = new ProcessEngine("p", Map.of(CH, "in", other, "other"), inner, 64 * 1024);
        HeaderKV causes = new HeaderKV(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(Map.of(other, 5L))));
        writer.onReceive(new ReceivedMessage(CH, 0L, 1L, null, new byte[] {1}, List.of(causes)));
        writer.onReceive(new ReceivedMessage(CH, 1L, 2L, null, new byte[] {2}, List.of(causes)));
        writer.flushHolds();

        new ProcessEngine("p", Map.of(CH, "in", other, "other"), inner, 64 * 1024);

        OrderingStore reversed = new OrderingStore() {
            @Override
            public byte[] get(byte[] key) {
                return inner.get(key);
            }

            @Override
            public void put(byte[] key, byte[] value) {
                inner.put(key, value);
            }

            @Override
            public void delete(byte[] key) {
                inner.delete(key);
            }

            @Override
            public void scanPrefix(byte[] prefix, EntryConsumer consumer) {
                java.util.List<byte[][]> entries = new java.util.ArrayList<>();
                inner.scanPrefix(prefix, (key, value) -> entries.add(new byte[][] {key, value}));
                for (int i = entries.size() - 1; i >= 0; i--) {
                    consumer.accept(entries.get(i)[0], entries.get(i)[1]);
                }
            }
        };
        ParsleyFailClosedException thrown = assertThrows(ParsleyFailClosedException.class,
                () -> new ProcessEngine("p", Map.of(CH, "in", other, "other"), reversed, 64 * 1024));
        assertEquals(ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT, thrown.reason());
        assertTrue(thrown.getMessage().contains("order"),
                () -> "diagnosis should name the ordering breach: " + thrown.getMessage());
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
