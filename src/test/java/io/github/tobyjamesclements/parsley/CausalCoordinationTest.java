package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CausalCoordination}, the public handle that turns on topology-epoch coordination.
 * Exercised over the in-memory transport double via the package-private {@link CausalCoordination#forRuntime}
 * seam, with the runtime driven synchronously — no broker.
 */
class CausalCoordinationTest {

    /**
     * {@link CausalCoordination#requestEpochTransition()} attributes the snapshot to a local member and
     * drives the round to a commit — the operator surface for evolving already-running nodes.
     */
    @Test
    void requestEpochTransitionOpensAndCommitsARoundOwnedByALocalMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        runtime.join("A");   // a local member, as a participating task's init() would

        coordination.requestEpochTransition();
        settle(log, runtime);

        assertEquals(1L, runtime.committedEpochId(),
                "requesting a transition opens a round owned by the local member and commits it");
    }

    /** Requesting a transition before any task has initialised coordination fails with a clear message. */
    @Test
    void requestEpochTransitionBeforeAnyRuntimeFails() {
        CausalCoordination coordination = CausalCoordination.create("epoch-events", Set.of());
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "with no initialised runtime there is nothing to request a transition on");
    }

    /** Requesting a transition with a runtime but no joined member fails: there is no local owner to attribute it to. */
    @Test
    void requestEpochTransitionWithNoLocalMemberFails() {
        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "no local member has joined, so the request cannot be attributed to a local owner");
    }

    /** {@link CausalCoordination#close()} is idempotent and safe to call before any task initialised the runtime. */
    @Test
    void closeIsIdempotentAndSafeBeforeInit() {
        CausalCoordination uninitialised = CausalCoordination.create("epoch-events", Set.of());
        uninitialised.close();   // no runtime built yet — a no-op

        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        coordination.close();
        coordination.close();   // idempotent
    }

    /**
     * A cold start (epoch 0) does not block: with no established epoch there is no history to strip, so
     * a fresh task's {@code awaitJoinCommit} returns without opening a round or waiting.
     */
    @Test
    void awaitJoinCommitDoesNotBlockAtEpochZero() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        runtime.start();
        try {
            coordination.awaitJoinCommit(runtime, "J");   // returns once bootstrapped; no round, no block
            assertEquals(0L, runtime.committedEpochId(), "a cold start stays at epoch 0 and does not block");
            assertEquals(0L, log.commitCount(), "no epoch was committed by the join at epoch 0");
        } finally {
            runtime.close();
        }
    }

    /**
     * A task joining an <em>established</em> epoch whose boundary never commits (its round can never
     * complete — a running member never publishes) fails after the join timeout, so Kafka Streams
     * restarts and retries the join rather than proceeding on an unknown floor.
     */
    @Test
    void awaitJoinCommitTimesOutWhenTheEpochNeverCommits() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        // Seed an established epoch 1 with a running member R, so the joiner's round cannot vacuously
        // complete and R (not live here) never publishes for it.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new EpochEvent.JoinRequested("R"));
        seeder.append(new EpochEvent.SnapshotRequested("R"));
        seeder.append(new EpochEvent.EpochCommitted(1, ParsleyClock.empty()));

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of(), Duration.ofMillis(300));
        runtime.start();
        try {
            runtime.join("J");   // a local member to attribute (own) the joiner's round to
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> coordination.awaitJoinCommit(runtime, "J"),
                    "a joiner whose epoch never commits must fail after the timeout");
            assertTrue(failure.getMessage().contains("join did not commit"),
                    "the failure names the join timeout so the cause is clear");
            assertEquals(1L, runtime.committedEpochId(), "the settled epoch never advanced past the established one");
        } finally {
            runtime.close();
        }
    }

    /** Folds every runtime until the shared log stops growing (three quiet passes). */
    private static void settle(InMemoryEpochTransport.SharedLog log, ParsleyEpochRuntime... runtimes) {
        int quietPasses = 0;
        while (quietPasses < 3) {
            long before = log.events().size();
            for (ParsleyEpochRuntime runtime : runtimes) {
                runtime.runOnce();
            }
            quietPasses = (log.events().size() == before) ? quietPasses + 1 : 0;
        }
    }
}
