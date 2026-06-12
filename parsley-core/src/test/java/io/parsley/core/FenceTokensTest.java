package io.parsley.core;

import io.parsley.FenceToken;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FenceTokensTest {

    private static final TestClock CLOCK = TestClock.empty().advance("orders-0", 7L).advance("payments-1", 3L);

    @Test
    void explicitServicesRoundTrip() {
        TestEncryption encryption = new TestEncryption();
        TestClockSerialiser serialiser = new TestClockSerialiser();

        FenceToken<TestClock> token = FenceTokens.of(CLOCK, encryption, serialiser);
        FenceToken<TestClock> decoded = FenceTokens.decode(token.encode(), encryption, serialiser);

        assertEquals(CLOCK, decoded.vectorClock());
    }

    @Test
    void globalServicesRoundTrip() {
        FenceToken<TestClock> token = FenceTokens.of(CLOCK);
        FenceToken<TestClock> decoded = FenceTokens.decode(token.encode());

        assertEquals(CLOCK, decoded.vectorClock());
    }

    @Test
    void vectorClockReturnsWrappedClock() {
        assertEquals(CLOCK, FenceTokens.of(CLOCK).vectorClock());
    }

    @Test
    void encodeIsUrlSafe() {
        String encoded = FenceTokens.of(CLOCK).encode();
        assertFalse(encoded.isBlank());
        assertTrue(encoded.matches("[A-Za-z0-9_=-]+"));
    }
}
