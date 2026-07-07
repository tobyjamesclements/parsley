package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyCoordination}, the public handle that turns on topology-epoch coordination.
 * Exercised over the in-memory transport double via the package-private {@link ParsleyCoordination#forRuntime}
 * seam, with the runtime driven synchronously — no broker.
 */
class ParsleyCoordinationTest {

    private static final Uuid T1 = Uuid.randomUuid();

    /**
     * {@link ParsleyCoordination#requestEpochTransition()} attributes the snapshot to a local member and
     * drives the round to a commit — the operator surface for evolving already-running nodes.
     */
    @Test
    void requestEpochTransitionOpensAndCommitsARoundOwnedByALocalMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.join("A", Set.of(), Set.of());   // a local member, as a participating task's init() would

        coordination.requestEpochTransition();
        settle(log, runtime);

        assertEquals(1L, runtime.committedEpochId(),
                "requesting a transition opens a round owned by the local member and commits it");
    }

    /** Requesting a transition before any task has initialised coordination fails with a clear message. */
    @Test
    void requestEpochTransitionBeforeAnyRuntimeFails() {
        ParsleyCoordination coordination = ParsleyCoordination.create("epoch-events");
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "with no initialised runtime there is nothing to request a transition on");
    }

    /** Requesting a transition with a runtime but no joined member fails: there is no local owner to attribute it to. */
    @Test
    void requestEpochTransitionWithNoLocalMemberFails() {
        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "no local member has joined, so the request cannot be attributed to a local owner");
    }

    /** {@link ParsleyCoordination#close()} is idempotent and safe to call before any task initialised the runtime. */
    @Test
    void closeIsIdempotentAndSafeBeforeInit() {
        ParsleyCoordination uninitialised = ParsleyCoordination.create("epoch-events");
        uninitialised.close();   // no runtime built yet — a no-op

        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
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
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
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
     * {@link ParsleyCoordination#leave()} drains before removing: it must not remove a local member while its
     * buffer is not drained; once every member reports drained it appends the {@code Leave}, waits until the
     * member has left the running set, and requests a new epoch over the remaining members (this node
     * excluded). Driven with a live runtime thread, since leave() blocks across its phases; assertions read
     * only thread-safe runtime state (the shared log is not safe to read concurrently).
     */
    @Test
    void leaveDrainsBeforeRemovingThenRequestsANewEpoch() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            runtime.join("A", Set.of(), Set.of());
            runtime.requestSnapshot("A");                 // cold start: commits epoch 1, promoting A to running
            waitUntil(() -> runtime.isRunningMember("A"));

            Thread leaver = new Thread(coordination::leave, "leave-test");
            leaver.start();

            // Phase 1 blocks while A's buffer is not reported drained: A stays a running member.
            Thread.sleep(200);
            assertTrue(leaver.isAlive(), "leave() blocks while A's buffer is not drained");
            assertTrue(runtime.isRunningMember("A"), "leave() must not remove A before it is drained");

            runtime.reportDrained("A", true);             // A drains -> leave() proceeds through its phases
            leaver.join(5000);
            assertFalse(leaver.isAlive(), "leave() returns once A is drained and removed");

            assertFalse(runtime.isRunningMember("A"), "leave() removed A from the running set");
            waitUntil(() -> runtime.committedEpochId() >= 2L);   // phase 3 committed a new epoch without A
            assertEquals(2L, runtime.committedEpochId(), "leave() requested a new epoch (epoch 2) excluding A");
        } finally {
            runtime.close();
        }
    }

    /**
     * A normal restart of an already-running member does not block: {@code awaitJoinCommit} sees it is
     * still a running member on the log and returns at once, without opening a round or bumping the epoch.
     */
    @Test
    void awaitJoinCommitDoesNotBlockForAnAlreadyRunningMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested("M", Set.of(), Set.of()));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("M"));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            coordination.awaitJoinCommit(runtime, "M");   // M is already running -> returns without blocking
            assertEquals(1L, runtime.committedEpochId(),
                    "a normal restart of a running member neither blocks nor bumps the epoch");
        } finally {
            runtime.close();
        }
    }

    /**
     * Regression test for the BACKLOG.md deadlock: a joiner blocked in {@code awaitJoinCommit} on its own
     * thread (standing in for a task thread stuck inside {@code init()}) must not wait forever on a running
     * member that shares its {@code StreamThread} and so can never run {@code pollEpochCoordination()}. R
     * never calls {@code publishFrontier} directly — its only publication channel is the completeness
     * snapshot registered via {@link ParsleyEpochRuntime#registerLocalCompleteness}, exactly modelling a
     * task thread that never gets to run. Without the runtime auto-publishing on R's behalf, the joiner
     * thread below hangs forever; with it, the round completes and the joiner unblocks.
     */
    @Test
    void awaitJoinCommitUnblocksWhenARunningMembersOnlyPublicationIsItsRegisteredCompletenessSnapshot()
            throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested("R", Set.of(), Set.of()));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("R"));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));   // R is running at epoch 1

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.join("R", Set.of(), Set.of());   // R's task lives on this instance, as its init() declared
        ParsleyClock rCompleteness = ParsleyClock.empty().observe(T1, 0, 9);
        runtime.registerLocalCompleteness("R", () -> rCompleteness);   // R's only publish channel
        runtime.start();
        try {
            // Mirrors ParsleyProcessor#init's exact call order on the task thread: join() (declares the
            // member, so the eventual commit can promote it) then the blocking awaitJoinCommit.
            Thread joiner = new Thread(() -> {
                runtime.join("J", Set.of(), Set.of());
                coordination.awaitJoinCommit(runtime, "J");
            }, "joiner-test");
            joiner.start();
            joiner.join(5000);

            assertFalse(joiner.isAlive(),
                    "the joiner must unblock once R's registered completeness is auto-published and round 2 commits");
            assertTrue(runtime.isRunningMember("J"), "the commit promoted the joiner to running");
            assertEquals(2L, runtime.committedEpochId(), "round 2 committed using R's auto-published completeness");
            assertEquals(rCompleteness, runtime.committedLowerBounds(),
                    "the floor is R's registered completeness — its only publication came via auto-publish");
        } finally {
            runtime.close();
        }
    }

    /** Polls {@code condition} (10ms) until true or 5s elapses; throws {@link AssertionError} on timeout. */
    private static void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 5s");
            }
            Thread.sleep(10);
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
