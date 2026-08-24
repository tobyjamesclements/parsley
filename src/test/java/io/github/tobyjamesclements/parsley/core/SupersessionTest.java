package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the mid-run supersession refusal against a real successor's committed progress.
 *
 * <p>D77's most reachable case for {@code COVERED_POSITION_FED} is routine supersession: a
 * session-timed-out execution's background facts round reads the group's committed offsets,
 * sees its successor's progress, and advances coverage past records still buffered in its own
 * consumer. {@code ProcessEngineTest#feedAtAReportCoveredPositionFailsClosedAsCoveredPositionFed}
 * pins the reason with a hand-written report; this class stages the condition structurally —
 * two lifetimes of one logical process over one committed ordering image, the report built
 * from the successor engine's actual state, the way {@code ParsleyProcessor#probeHints}
 * derives read positions from {@code ProcessEngine#fedUpTo} — and then pins the recovery
 * promise the refusal's message makes: a restart adopting the group's real progress delivers
 * normally, and the refusal does not recur (D77, D10).
 */
class SupersessionTest {
    private static final ChannelId C1 = new ChannelId(new UUID(11, 1), 0);
    private static final ChannelId C2 = new ChannelId(new UUID(11, 2), 0);
    private static final Map<ChannelId, String> BOTH = Map.of(C1, "c1", C2, "c2");

    private static ReceivedMessage plain(ChannelId channel, long position, String uid) {
        return new ReceivedMessage(channel, position, position, uid.getBytes(), uid.getBytes(), List.of());
    }

    /**
     * Copies a just-committed store's image into an independent store, purely through the
     * {@link OrderingStore} surface (an empty prefix visits every entry), mirroring a second
     * instance of the same logical process restoring from the shared changelog. Callers
     * invoke this immediately after {@link MemoryOrderingStore#commit()}, when the working
     * image is exactly the committed image.
     */
    private static MemoryOrderingStore forkCommittedImage(OrderingStore justCommitted) {
        MemoryOrderingStore fork = new MemoryOrderingStore();
        justCommitted.scanPrefix(new byte[0], fork::put);
        fork.commit();
        return fork;
    }

    /**
     * The read-position report a facts round gathers once the group's committed offsets are
     * the successor's: per channel, the successor's next unread position. Built by querying
     * the successor engine, not from invented numbers, the way the Kafka host's probe hints
     * are derived from {@code engine.fedUpTo}.
     */
    private static PositionFacts factsFromCommittedProgressOf(ProcessEngine successor) {
        Map<ChannelId, Long> committedNextRead = new HashMap<>();
        for (ChannelId channel : successor.receivedChannelSet()) {
            successor.fedUpTo(channel).ifPresent(fed -> committedNextRead.put(channel, fed + 1));
        }
        return new PositionFacts(committedNextRead, Map.of(), Set.of());
    }

    private record TwoLifetimes(ProcessEngine superseded, ProcessEngine successor,
                                MemoryOrderingStore successorStore) {
    }

    /**
     * Lifetime one feeds, delivers and commits positions 0..2 of C1, then keeps running.
     * The successor restores from that committed image over an independent store, feeds,
     * delivers and commits positions 3..5 — the group's real committed progress is now the
     * successor's, while the superseded engine still believes coverage ends at 2.
     */
    private static TwoLifetimes stageSupersession() {
        MemoryOrderingStore firstStore = new MemoryOrderingStore();
        ProcessEngine superseded = new ProcessEngine("p", BOTH, firstStore);
        for (long position = 0; position <= 2; position++) {
            superseded.onReceive(plain(C1, position, "a" + position));
            superseded.markDelivered(C1, position);
        }
        superseded.flushHolds();
        firstStore.commit();

        MemoryOrderingStore successorStore = forkCommittedImage(firstStore);
        ProcessEngine successor = new ProcessEngine("p", BOTH, successorStore);
        for (long position = 3; position <= 5; position++) {
            successor.onReceive(plain(C1, position, "b" + position));
            successor.markDelivered(C1, position);
        }
        successor.flushHolds();
        successorStore.commit();
        return new TwoLifetimes(superseded, successor, successorStore);
    }

    /**
     * Catches the engine silently dropping — or worse, delivering — a feed at a position its
     * successor's real committed progress covers. The report is constructed from the
     * successor engine's actual state, so a regression anywhere along the chain (the facts
     * round not adopting committed coverage, or the covered-feed branch degrading to a drop
     * or an acceptance) turns this red; the fabricated-report pin in {@code ProcessEngineTest}
     * cannot see the first of those. The refusal must carry {@code COVERED_POSITION_FED} and
     * name supersession, D77's honest diagnosis in place of a host feed-order accusation.
     */
    @Test
    void factsBuiltFromARealSuccessorsCommittedProgressMakeACoveredFeedRefuseAsCoveredPositionFed() {
        TwoLifetimes lifetimes = stageSupersession();
        ProcessEngine superseded = lifetimes.superseded();

        superseded.onFacts(factsFromCommittedProgressOf(lifetimes.successor()));
        assertEquals(OptionalLong.of(5), superseded.fedUpTo(C1),
                "the facts round must adopt the group's committed coverage, which is the successor's progress");

        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class,
                () -> superseded.onReceive(plain(C1, 3, "z3")),
                "a superseded execution fed a position its successor's committed progress covers must refuse"
                        + " loudly, not drop the record silently or deliver it");
        assertEquals(ParsleyFailClosedException.Reason.COVERED_POSITION_FED, e.reason(),
                "the refusal is the report/feed contradiction, not a host feed-order breach (D77)");
        assertTrue(e.getMessage().contains("superseded"),
                "the diagnosis must name supersession as the recoverable cause (D77), got: " + e.getMessage());
        assertEquals(0, superseded.heldCountTotal(),
                "a refused feed must leave nothing in the hold-back buffer");
        assertTrue(superseded.nextDeliverable().isEmpty(),
                "a refused feed must never surface as a deliverable");
    }

    /**
     * Catches the refusal's own recovery promise breaking: D77's message states that a
     * superseded execution's step cannot commit, "a restart recovers, and this refusal then
     * does not recur". A clean restart restores from the group's real committed state — the
     * successor's image — so its session floor covers the successor's progress: a replayed
     * covered position is a committed duplicate dropped silently (D10), and the next unread
     * position delivers normally. Red if the restart refuses again, or if the covered replay
     * is refused instead of dropped.
     */
    @Test
    void aRestartAdoptingTheSuccessorsCommittedProgressRecoversAndTheRefusalDoesNotRecur() {
        TwoLifetimes lifetimes = stageSupersession();
        ProcessEngine superseded = lifetimes.superseded();
        superseded.onFacts(factsFromCommittedProgressOf(lifetimes.successor()));
        assertThrows(ParsleyFailClosedException.class, () -> superseded.onReceive(plain(C1, 3, "z3")),
                "staging: the superseded execution must first hit the covered-position refusal");

        ProcessEngine restarted = new ProcessEngine("p", BOTH, forkCommittedImage(lifetimes.successorStore()));
        restarted.onFacts(factsFromCommittedProgressOf(lifetimes.successor()));

        assertEquals(ProcessEngine.ReceiveOutcome.DUPLICATE_DROPPED, restarted.onReceive(plain(C1, 4, "z4")),
                "after the restart a replayed covered position is a committed duplicate below the session"
                        + " floor, dropped silently (D10) — the refusal must not recur (D77)");
        assertEquals(ProcessEngine.ReceiveOutcome.ACCEPTED, restarted.onReceive(plain(C1, 6, "c6")),
                "the restart must accept the successor's next unread position normally");
        DeliverableMessage next = restarted.nextDeliverable().orElseThrow(
                () -> new AssertionError("the accepted post-restart feed must become deliverable"));
        assertEquals(6, next.position(), "the restart delivers exactly the next unread position");
        restarted.markDelivered(C1, 6);
        assertEquals(0, restarted.heldCountTotal(),
                "the restarted engine runs on with nothing stuck in the hold-back buffer");
    }
}
