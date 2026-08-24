package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the value-object floor under every position the protocol handles: a negative or
 * null position is refused at construction with the one-taxonomy
 * {@code IllegalArgumentException} (D73), before any engine, store or codec logic can
 * consume it. Positions are log offsets; a negative one describes no record and would
 * poison every comparison the deliverability decision makes.
 *
 * <p>{@code Causes.of}'s check is also the backstop behind {@code CausesCodec}'s per-entry
 * negative-position refusal: a codec that lost its own guard still refuses through this one,
 * wrapped as "malformed causes header" — which is why
 * {@code CausesCodecTest#negativePositionIsDiagnosedPerEntryNamingItsChannel} pins the
 * codec's message and this class pins the backstop itself.
 */
class PositionRefusalTest {
    private static final ChannelId CH = new ChannelId(new UUID(1, 2), 0);

    /**
     * A negative feed position must be refused when the {@code ReceivedMessage} is built,
     * not carried into the engine's covered-position arithmetic. Regression caught: deleting
     * the constructor's position guard lets the record build and fails the
     * {@code assertThrows}.
     */
    @Test
    void negativeReceivedPositionIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new ReceivedMessage(CH, -1L, 0L, null, null, List.of()),
                "a negative feed position must be refused at construction");
        assertTrue(thrown.getMessage().contains("position must be non-negative"),
                () -> "the refusal must name the rule it enforces: " + thrown.getMessage());
    }

    /**
     * A negative cause position must be refused by {@code Causes.of}, naming the channel it
     * arrived on — this is the frontier's last line of defence, and the backstop behind the
     * codec's per-entry check (see the class Javadoc). Regression caught: deleting the
     * position guard in {@code Causes.of} lets the frontier build around the negative
     * position and fails the {@code assertThrows}.
     */
    @Test
    void negativeCausePositionIsRefusedNamingItsChannel() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> Causes.of(Map.of(CH, -1L)),
                "a negative cause position must be refused by the frontier builder");
        assertTrue(thrown.getMessage().contains("position must be non-negative on " + CH + ": -1"),
                () -> "the refusal must name the channel and the offending position: " + thrown.getMessage());
    }

    /**
     * A null cause position must be refused by the same guard with the same diagnosis — a
     * null in the map is the adjacent construction mistake, and letting it through would
     * plant an NPE in whatever compares the frontier later. Regression caught: deleting the
     * position guard in {@code Causes.of} lets the null sit in the frontier (TreeMap accepts
     * null values) and fails the {@code assertThrows}.
     */
    @Test
    void nullCausePositionIsRefusedNamingItsChannel() {
        Map<ChannelId, Long> withNull = new HashMap<>();
        withNull.put(CH, null);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> Causes.of(withNull),
                "a null cause position must be refused by the frontier builder");
        assertTrue(thrown.getMessage().contains("position must be non-negative on " + CH + ": null"),
                () -> "the refusal must name the channel and the null position: " + thrown.getMessage());
    }
}
