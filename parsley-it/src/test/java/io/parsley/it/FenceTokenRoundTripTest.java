package io.parsley.it;

import io.parsley.FenceToken;
import io.parsley.kafka.KafkaVectorClock;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FenceTokenRoundTripTest {

    private static final TopicPartition TP0 = new TopicPartition("orders", 0);
    private static final TopicPartition TP1 = new TopicPartition("payments", 1);

    @Test
    void encodeThenDecodeYieldsSameClock() {
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(TP0, 42L));
        FenceToken token = FenceToken.of(clock);

        FenceToken decoded = FenceToken.decode(token.encode());
        assertEquals(clock, decoded.vectorClock());
    }

    @Test
    void multiPartitionClockRoundTrip() {
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(TP0, 10L, TP1, 20L));
        FenceToken decoded = FenceToken.decode(FenceToken.of(clock).encode());
        assertEquals(clock, decoded.vectorClock());
    }

    @Test
    void emptyClockRoundTrip() {
        KafkaVectorClock clock = KafkaVectorClock.empty();
        FenceToken decoded = FenceToken.decode(FenceToken.of(clock).encode());
        assertEquals(clock, decoded.vectorClock());
    }

    @Test
    void differentClocksDecodeToDistinctClocks() {
        KafkaVectorClock c1 = new KafkaVectorClock(Map.of(TP0, 1L));
        KafkaVectorClock c2 = new KafkaVectorClock(Map.of(TP0, 2L));
        assertNotEquals(
                FenceToken.decode(FenceToken.of(c1).encode()).vectorClock(),
                FenceToken.decode(FenceToken.of(c2).encode()).vectorClock(),
                "Tokens with different clocks must decode to different clocks");
    }

    @Test
    void tamperedTokenThrowsOnDecode() {
        String token = FenceToken.of(new KafkaVectorClock(Map.of(TP0, 1L))).encode();
        String tampered = token.substring(0, token.length() - 4) + "XXXX";
        assertThrows(IllegalStateException.class, () -> FenceToken.decode(tampered));
    }

    @Test
    void encodeProducesNonBlankString() {
        assertFalse(FenceToken.of(new KafkaVectorClock(Map.of(TP0, 1L))).encode().isBlank());
    }
}
