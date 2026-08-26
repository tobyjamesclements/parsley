package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.Deliverability;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the companion past's contract: merges are pointwise maxima over immutable
 * values, the wire form is the frozen causes grammar byte for byte, and coverage fails
 * closed over channels the past cannot verify — the deliberate inversion of the delivery
 * gate's skip (issue #96, Boundary 2).
 */
class CausalPastTest {
    private static final ChannelId ORDERS = new ChannelId(new UUID(1, 1), 3);
    private static final ChannelId EVENTS = new ChannelId(new UUID(1, 2), 0);

    private static CausalPast past(Map<ChannelId, Long> byChannel) {
        return CausalPast.of(Causes.of(byChannel));
    }

    /** The empty past names nothing, and building over an empty frontier yields it. */
    @Test
    void emptyPastNamesNothing() {
        assertTrue(CausalPast.none().isEmpty());
        assertEquals(0, CausalPast.none().size());
        assertEquals(Causes.none(), CausalPast.none().causes());
        assertEquals(CausalPast.none(), CausalPast.of(Causes.none()));
    }

    /** Refuses a null frontier rather than carrying one. */
    @Test
    void refusesNullCauses() {
        assertThrows(IllegalArgumentException.class, () -> CausalPast.of(null));
    }

    /**
     * The wire form is the frozen causes grammar itself, byte for byte, so an encoded past
     * is a valid {@code parsley.causes} header value and round trips canonically. This pin
     * is what keeps the token format from drifting into a private dialect of the grammar.
     */
    @Test
    void encodesInTheFrozenGrammarAndRoundTrips() throws Exception {
        CausalPast past = past(Map.of(ORDERS, 42L, EVENTS, 7L));
        byte[] encoded = past.encode();
        assertArrayEquals(CausesCodec.encode(past.causes()), encoded,
                "the token wire form must be the codec's, not a dialect");
        assertEquals(past, CausalPast.decode(encoded));
        assertArrayEquals(new byte[] {CausesCodec.FORMAT_VERSION, 0}, CausalPast.none().encode(),
                "the empty past must encode as the codec's empty frontier");
        assertEquals(CausalPast.none(), CausalPast.decode(CausalPast.none().encode()));
    }

    /** Merging a coordinate adds an unnamed channel and advances a named one. */
    @Test
    void mergeCoordinateAddsAndAdvances() {
        CausalPast past = CausalPast.none().merge(ORDERS, 5);
        assertEquals(past(Map.of(ORDERS, 5L)), past);
        assertEquals(1, past.size());
        assertFalse(past.isEmpty());
        assertEquals(past(Map.of(ORDERS, 9L)), past.merge(ORDERS, 9));
        assertEquals(past(Map.of(ORDERS, 5L, EVENTS, 2L)), past.merge(EVENTS, 2));
        assertEquals(2, past.merge(EVENTS, 2).size());
    }

    /**
     * A merge never regresses: a coordinate at or below the named position changes
     * nothing, so replaying an already-covered observation is harmless.
     */
    @Test
    void mergeCoordinateNeverRegresses() {
        CausalPast past = past(Map.of(ORDERS, 5L));
        assertEquals(past, past.merge(ORDERS, 3));
        assertEquals(past, past.merge(ORDERS, 5));
    }

    /** Merging pasts takes the greater position per channel, from either side. */
    @Test
    void mergePastIsThePointwiseMaximum() {
        CausalPast left = past(Map.of(ORDERS, 5L, EVENTS, 9L));
        CausalPast right = past(Map.of(ORDERS, 7L, EVENTS, 2L));
        CausalPast both = past(Map.of(ORDERS, 7L, EVENTS, 9L));
        assertEquals(both, left.merge(right));
        assertEquals(both, right.merge(left));
        assertEquals(left, left.merge(CausalPast.none()));
        assertEquals(left, CausalPast.none().merge(left));
    }

    /**
     * A merge whose result would equal one side returns that side itself — the steady
     * state of a session re-reading settled data — so callers can skip re-encoding an
     * unchanged token. Identity is the pin: an implementation that rebuilt an equal past
     * would pass equality but fail these.
     */
    @Test
    void mergePastReturnsTheCoveringSideItself() {
        CausalPast covering = past(Map.of(ORDERS, 5L, EVENTS, 9L));
        CausalPast covered = past(Map.of(ORDERS, 3L));
        assertSame(covering, covering.merge(covered));
        assertSame(covering, covered.merge(covering));
        assertSame(covering, covering.merge(covering));
        assertSame(covering, covering.merge(CausalPast.none()));
        assertSame(covering, CausalPast.none().merge(covering));
    }

    /** Instances are values: a merge returns a new past and the original is unchanged. */
    @Test
    void mergeLeavesTheOriginalUnchanged() {
        CausalPast original = past(Map.of(ORDERS, 5L));
        original.merge(EVENTS, 2);
        original.merge(past(Map.of(ORDERS, 9L)));
        assertEquals(past(Map.of(ORDERS, 5L)), original);
    }

    /**
     * Merge arguments are validated up front. The negative-position refusal must not
     * depend on what the past already names: a negative position against an
     * already-covered channel would otherwise vanish into the maximum.
     */
    @Test
    void mergeRefusesNullAndNegativeArguments() {
        CausalPast past = past(Map.of(ORDERS, 5L));
        assertThrows(IllegalArgumentException.class, () -> past.merge(null, 1));
        assertThrows(IllegalArgumentException.class, () -> past.merge((CausalPast) null));
        assertThrows(IllegalArgumentException.class, () -> CausalPast.none().merge(ORDERS, -1));
        assertThrows(IllegalArgumentException.class, () -> past.merge(ORDERS, -1),
                "a negative position must be refused even when the maximum would discard it");
    }

    /** An empty token is covered by any past: a session that has observed nothing needs nothing. */
    @Test
    void emptyTokenIsCoveredByAnyPast() {
        assertTrue(CausalPast.none().coverageOf(CausalPast.none()).covers());
        assertTrue(past(Map.of(ORDERS, 5L)).coverageOf(CausalPast.none()).covers());
    }

    /** A past at or above every named position covers the token. */
    @Test
    void coversAtOrAboveEveryNamedPosition() {
        CausalPast token = past(Map.of(ORDERS, 5L, EVENTS, 2L));
        assertTrue(past(Map.of(ORDERS, 5L, EVENTS, 2L)).coverageOf(token).covers());
        assertTrue(past(Map.of(ORDERS, 9L, EVENTS, 2L, new ChannelId(new UUID(9, 9), 1), 0L))
                .coverageOf(token).covers());
    }

    /** A past behind on one channel reports that gap exactly: channel, required, recorded. */
    @Test
    void reportsAGapWhenBehind() {
        CausalPast.Coverage coverage = past(Map.of(ORDERS, 3L)).coverageOf(past(Map.of(ORDERS, 5L)));
        assertFalse(coverage.covers());
        assertEquals(1, coverage.gaps().size());
        Deliverability.Blocker gap = coverage.gaps().get(0);
        assertEquals(ORDERS, gap.channel());
        assertEquals(5L, gap.requiredPosition());
        assertEquals(OptionalLong.of(3L), gap.settledPosition());
    }

    /**
     * The load-bearing inversion (issue #96, Boundary 2): a channel the past has never
     * recorded fails the check, where the delivery gate skips it. The gate's skip is
     * mandatory for liveness — a process cannot wait for a channel it will never see
     * (Liveness 4) — but a read tier serving on a channel it cannot verify is a silent
     * read-your-writes violation, so the same inputs must answer the two questions
     * oppositely. The contrast is asserted, not narrated: reusing the gate's disposition
     * here turns this red.
     */
    @Test
    void failsClosedOverAChannelThePastCannotVerify() {
        CausalPast recorded = past(Map.of(EVENTS, 9L));
        CausalPast token = past(Map.of(ORDERS, 5L));

        assertTrue(Deliverability.decide(token.causes(), recorded.causes().byChannel().keySet(),
                        channel -> {
                            Long recordedPosition = recorded.causes().byChannel().get(channel);
                            return recordedPosition == null ? OptionalLong.empty() : OptionalLong.of(recordedPosition);
                        })
                .isDeliverable(),
                "the delivery gate skips the unreceived channel and would deliver");
        CausalPast.Coverage coverage = recorded.coverageOf(token);
        assertFalse(coverage.covers(), "the read tier must refuse what it cannot verify");
        assertEquals(1, coverage.gaps().size());
        Deliverability.Blocker gap = coverage.gaps().get(0);
        assertEquals(ORDERS, gap.channel());
        assertEquals(5L, gap.requiredPosition());
        assertEquals(OptionalLong.empty(), gap.settledPosition(),
                "an unverifiable channel must report no recorded position, not zero");
    }

    /** Every shortfall is reported, in channel order, so a retry policy sees the whole gap. */
    @Test
    void reportsEveryGapInChannelOrder() {
        CausalPast token = past(Map.of(ORDERS, 5L, EVENTS, 2L));
        CausalPast.Coverage coverage = past(Map.of(EVENTS, 1L)).coverageOf(token);
        assertFalse(coverage.covers());
        assertEquals(2, coverage.gaps().size());
        assertEquals(ORDERS, coverage.gaps().get(0).channel(), "gaps must arrive in ChannelId order");
        assertEquals(EVENTS, coverage.gaps().get(1).channel());
    }

    /** Coverage is monotone under merge: recording the missing coordinate closes the gap. */
    @Test
    void mergingTheMissingCoordinateClosesTheGap() {
        CausalPast token = past(Map.of(ORDERS, 5L));
        CausalPast recorded = past(Map.of(EVENTS, 9L));
        assertFalse(recorded.coverageOf(token).covers());
        assertTrue(recorded.merge(ORDERS, 5).coverageOf(token).covers());
        assertTrue(recorded.merge(token).coverageOf(token).covers(),
                "a past merged with the token itself must cover it");
    }

    /**
     * Coverage refuses a null token, and copies its gaps. The copy is pinned through a
     * directly-constructed mutable list, because the gaps handed over by the decision are
     * already unmodifiable and would keep an aliasing constructor green.
     */
    @Test
    void coverageValidatesItsInputsAndProtectsItsGaps() {
        CausalPast past = past(Map.of(ORDERS, 5L));
        assertThrows(IllegalArgumentException.class, () -> past.coverageOf(null));
        assertThrows(IllegalArgumentException.class, () -> new CausalPast.Coverage(null));
        java.util.List<Deliverability.Blocker> mutable = new java.util.ArrayList<>();
        mutable.add(new Deliverability.Blocker(ORDERS, 1, OptionalLong.empty()));
        CausalPast.Coverage coverage = new CausalPast.Coverage(mutable);
        mutable.clear();
        assertEquals(1, coverage.gaps().size(), "the gaps must be copied, not aliased");
        assertThrows(UnsupportedOperationException.class,
                () -> coverage.gaps().add(new Deliverability.Blocker(ORDERS, 1, OptionalLong.empty())));
    }

    /** Equality follows the carried frontier, and the rendering names the type. */
    @Test
    void equalityFollowsTheCarriedFrontier() {
        assertEquals(past(Map.of(ORDERS, 5L)), past(Map.of(ORDERS, 5L)));
        assertEquals(past(Map.of(ORDERS, 5L)).hashCode(), past(Map.of(ORDERS, 5L)).hashCode());
        assertNotEquals(past(Map.of(ORDERS, 5L)), past(Map.of(ORDERS, 6L)));
        assertNotEquals(past(Map.of(ORDERS, 5L)), CausalPast.none());
        assertTrue(past(Map.of(ORDERS, 5L)).toString().startsWith("CausalPast"));
    }
}
