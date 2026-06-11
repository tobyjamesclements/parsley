package io.parsley.it;

import io.parsley.FenceToken;
import io.parsley.Partition;
import io.parsley.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FenceTokenRoundTripTest {

    private static final Partition P0 = new Partition("orders", 0);
    private static final Partition P1 = new Partition("payments", 1);

    @Test
    void encodeThenDecodeYieldsSameClock() {
        VectorClock clock = positions(Map.of(P0, 42L));
        FenceToken token = FenceToken.of(clock);

        FenceToken decoded = FenceToken.decode(token.encode());
        assertEquals(clock.positions(), decoded.vectorClock().positions());
    }

    @Test
    void multiPartitionClockRoundTrip() {
        VectorClock clock = positions(Map.of(P0, 10L, P1, 20L));
        FenceToken decoded = FenceToken.decode(FenceToken.of(clock).encode());
        assertEquals(clock.positions(), decoded.vectorClock().positions());
    }

    @Test
    void emptyClockRoundTrip() {
        VectorClock clock = positions(Map.of());
        FenceToken decoded = FenceToken.decode(FenceToken.of(clock).encode());
        assertTrue(decoded.vectorClock().positions().isEmpty());
    }

    @Test
    void differentClocksDecodeToDistinctPositions() {
        VectorClock c1 = positions(Map.of(P0, 1L));
        VectorClock c2 = positions(Map.of(P0, 2L));
        assertNotEquals(
                FenceToken.decode(FenceToken.of(c1).encode()).vectorClock().positions(),
                FenceToken.decode(FenceToken.of(c2).encode()).vectorClock().positions(),
                "Tokens with different clocks must decode to different positions");
    }

    @Test
    void tamperedTokenThrowsOnDecode() {
        String token = FenceToken.of(positions(Map.of(P0, 1L))).encode();
        String tampered = token.substring(0, token.length() - 4) + "XXXX";
        assertThrows(IllegalStateException.class, () -> FenceToken.decode(tampered));
    }

    @Test
    void encodeProducesNonBlankString() {
        assertFalse(FenceToken.of(positions(Map.of(P0, 1L))).encode().isBlank());
    }

    private static VectorClock positions(Map<Partition, Long> map) {
        return () -> map;
    }
}
