package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The decision unit, exercised in isolation with no host, no engine, no store: exactly the separately callable pure
 * function SPEC Structural 7 requires. Its inputs are complete — there is nothing else it could consult.
 */
class DeliverabilityTest {

    private static final ChannelId RECEIVED_A = new ChannelId(new UUID(0, 1), 0);
    private static final ChannelId RECEIVED_B = new ChannelId(new UUID(0, 2), 0);
    private static final ChannelId ELSEWHERE = new ChannelId(new UUID(0, 3), 0);
    private static final Set<ChannelId> RECEIVED = Set.of(RECEIVED_A, RECEIVED_B);

    private static Deliverability.SettledView settled(Map<ChannelId, Long> settled) {
        return channel -> settled.containsKey(channel)
                ? OptionalLong.of(settled.get(channel))
                : OptionalLong.empty();
    }

    @Test
    void noCausesIsDeliverable() {
        assertTrue(Deliverability.decide(Causes.none(), RECEIVED, settled(Map.of())).isDeliverable());
    }

    @Test
    void causeOnUnreceivedChannelIsVacuouslySatisfied() {
        Causes causes = Causes.of(Map.of(ELSEWHERE, 100L));
        assertTrue(Deliverability.decide(causes, RECEIVED, settled(Map.of())).isDeliverable());
    }

    @Test
    void causeAtOrBelowSettledIsSatisfied() {
        Causes causes = Causes.of(Map.of(RECEIVED_A, 5L));
        assertTrue(Deliverability.decide(causes, RECEIVED, settled(Map.of(RECEIVED_A, 5L))).isDeliverable());
        assertTrue(Deliverability.decide(causes, RECEIVED, settled(Map.of(RECEIVED_A, 9L))).isDeliverable());
    }

    @Test
    void causeAboveSettledHolds() {
        Causes causes = Causes.of(Map.of(RECEIVED_A, 5L));
        Deliverability.Verdict verdict = Deliverability.decide(causes, RECEIVED, settled(Map.of(RECEIVED_A, 4L)));
        assertFalse(verdict.isDeliverable());
        Deliverability.Held held = (Deliverability.Held) verdict;
        assertEquals(1, held.blockers().size());
        assertEquals(RECEIVED_A, held.blockers().get(0).channel());
        assertEquals(5L, held.blockers().get(0).requiredPosition());
        assertEquals(OptionalLong.of(4L), held.blockers().get(0).settledPosition());
    }

    @Test
    void unknownChannelStateHolds() {
        Causes causes = Causes.of(Map.of(RECEIVED_A, 0L));
        Deliverability.Verdict verdict = Deliverability.decide(causes, RECEIVED, settled(Map.of()));
        assertFalse(verdict.isDeliverable(), "with nothing known of a received channel, any cause on it blocks");
        assertEquals(OptionalLong.empty(),
                ((Deliverability.Held) verdict).blockers().get(0).settledPosition());
    }

    @Test
    void mixedCausesReportEveryBlocker() {
        Causes causes = Causes.of(Map.of(RECEIVED_A, 5L, RECEIVED_B, 2L, ELSEWHERE, 50L));
        Deliverability.Verdict verdict = Deliverability.decide(
                causes, RECEIVED, settled(Map.of(RECEIVED_A, 10L, RECEIVED_B, 1L)));
        assertFalse(verdict.isDeliverable());
        Deliverability.Held held = (Deliverability.Held) verdict;
        assertEquals(1, held.blockers().size(), "only the unsatisfied received-channel cause blocks");
        assertEquals(RECEIVED_B, held.blockers().get(0).channel());
    }
}
