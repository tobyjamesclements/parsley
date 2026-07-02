package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyEpoch}: the compacted-topic key/value codec and the {@link ParsleyEpoch.View}
 * lookup semantics (unbounded coordinates read as {@link Long#MAX_VALUE}).
 */
class ParsleyEpochTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * A {@code startsAt} value round-trips through {@link ParsleyEpoch#value}/{@link ParsleyEpoch#startsAt}.
     */
    @Test
    void startsAtValueRoundTrips() {
        assertEquals(1000L, ParsleyEpoch.startsAt(ParsleyEpoch.value(1000L)),
                "a startsAt value must round-trip byte-for-byte through the value codec");
    }

    /**
     * The same coordinate always encodes to the same key bytes, and distinct coordinates to distinct
     * keys — the property the global store relies on to look a bound up by coordinate.
     */
    @Test
    void keyEncodingIsStableAndCoordinateSpecific() {
        assertArrayEquals(ParsleyEpoch.key(T1_ID, 0), ParsleyEpoch.key(T1_ID, 0),
                "the same coordinate must encode to identical key bytes");
        assertEquals(false, java.util.Arrays.equals(ParsleyEpoch.key(T1_ID, 0), ParsleyEpoch.key(T1_ID, 1)),
                "different partitions must encode to different keys");
        assertEquals(false, java.util.Arrays.equals(ParsleyEpoch.key(T1_ID, 0), ParsleyEpoch.key(T2_ID, 0)),
                "different topics must encode to different keys");
    }

    /**
     * A malformed value (wrong length) is rejected rather than silently mis-decoded.
     */
    @Test
    void malformedValueLengthIsRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> ParsleyEpoch.startsAt(new byte[]{1, 2, 3}),
                "a value of the wrong length must be rejected");
    }

    /**
     * The {@link ParsleyEpoch.View#NONE} view treats every coordinate as unbounded
     * ({@link ParsleyEpoch#NO_BOUND}), so the strip step is a no-op when epoch bounding is disabled.
     */
    @Test
    void noneViewReportsEveryCoordinateUnbounded() {
        assertEquals(ParsleyEpoch.NO_BOUND, ParsleyEpoch.View.NONE.startsAt(T1_ID, 0),
                "the NONE view must report every coordinate as unbounded");
    }
}
