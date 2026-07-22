package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyTopicIdentityWatch} — the mid-run E1 enforcement (T3.0 A13): tasks
 * register the topic name → UUID bindings they resolved at init, a poll compares them against the
 * broker's current view, and once a recreation or deletion is detected every
 * {@link ParsleyTopicIdentityWatch#ensureIntact} call fails the caller fast.
 */
class ParsleyTopicIdentityWatchTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** Hammer schedule seed; override with {@code -Dparsley.hammer.seed} to replay a failure. */
    private static final long HAMMER_SEED = Long.getLong("parsley.hammer.seed", 71_000L);
    /** Hammer iterations per run; override with {@code -Dparsley.hammer.iterations} to soak longer. */
    private static final int HAMMER_ITERATIONS = Integer.getInteger("parsley.hammer.iterations", 200);
    /** Concurrent {@code ensureIntact} readers per hammer iteration — the per-record task checks. */
    private static final int HAMMER_READERS = 3;

    /** A {@link ParsleyTopicAdmin} double resolving per-topic UUIDs or throwing a configured failure. */
    private static final class FixedIdsAdmin implements ParsleyTopicAdmin {
        private final Map<String, Uuid> ids = new HashMap<>();
        private final Map<String, Exception> failures = new HashMap<>();

        FixedIdsAdmin with(String topic, Uuid id) {
            ids.put(topic, id);
            return this;
        }

        FixedIdsAdmin failing(String topic, Exception failure) {
            failures.put(topic, failure);
            return this;
        }

        @Override public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
            Map<String, Uuid> resolved = new HashMap<>();
            for (String topic : topics) {
                Exception failure = failures.get(topic);
                if (failure != null) {
                    throw failure;
                }
                resolved.put(topic, ids.get(topic));
            }
            return resolved;
        }

        @Override public Map<String, Integer> partitionCounts(List<String> topics) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public Map<String, String> cleanupPolicies(List<String> topics) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public Map<Integer, Long> endOffsets(String topic) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public void close() {}
    }

    /**
     * The intact case: every expected topic still resolves to its registered UUID, so the poll
     * finds nothing and {@code ensureIntact} stays a no-op.
     */
    @Test
    void unchangedTopicIdsStayIntactAcrossPolls() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.expect("t2", T2_ID);

        watch.poll(new FixedIdsAdmin().with("t1", T1_ID).with("t2", T2_ID));

        assertDoesNotThrow(watch::ensureIntact,
                "unchanged topic UUIDs must keep the watch intact — the check must stay a no-op");
    }

    /**
     * The A13 core: a topic whose broker-current UUID differs from the init-time resolution was
     * deleted and recreated mid-run; the poll marks the watch broken and every subsequent
     * {@code ensureIntact} throws, naming the topic and both UUIDs.
     */
    @Test
    void aRecreatedTopicBreaksTheWatchAndEnsureIntactThrows() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        Uuid recreated = Uuid.randomUuid();

        watch.poll(new FixedIdsAdmin().with("t1", recreated));

        CausalTopicRecreatedException failure =
                assertThrows(CausalTopicRecreatedException.class, watch::ensureIntact,
                        "a changed topic UUID must fail every subsequent identity check");
        assertTrue(failure.getMessage().contains("t1"),
                "the failure must name the recreated topic: " + failure.getMessage());
        assertTrue(failure.getMessage().contains(T1_ID.toString())
                        && failure.getMessage().contains(recreated.toString()),
                "the failure must carry both the init-time and the current UUID: " + failure.getMessage());
    }

    /**
     * A topic that no longer exists at all — {@link UnknownTopicOrPartitionException} in the
     * resolution failure chain — is positive evidence of deletion: its records can never arrive
     * again and a recreation would rebind coordinates, so the watch breaks rather than retrying
     * silently forever.
     */
    @Test
    void aDeletedTopicBreaksTheWatch() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);

        watch.poll(new FixedIdsAdmin().failing("t1",
                new ExecutionException(new UnknownTopicOrPartitionException("gone"))));

        assertThrows(CausalTopicRecreatedException.class, watch::ensureIntact,
                "a provably deleted causal topic must fail the identity check");
    }

    /**
     * A transient resolution failure (an unreachable broker, a timeout) is never a verdict: the
     * poll logs and retries next tick, and the watch stays intact — only positive evidence of a
     * changed or missing topic may break identity.
     */
    @Test
    void aTransientResolutionFailureNeverBreaksTheWatch() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);

        watch.poll(new FixedIdsAdmin().failing("t1",
                new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("broker away"))));

        assertDoesNotThrow(watch::ensureIntact,
                "a transient admin failure must be retried, never treated as a recreation");
    }

    /**
     * Two tasks of one instance registering different UUIDs for the same topic name is itself
     * proof the topic was recreated between their inits (a recreation racing startup): the watch
     * breaks immediately, without waiting for a poll.
     */
    @Test
    void conflictingInitTimeResolutionsBreakTheWatchImmediately() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.expect("t1", Uuid.randomUuid());

        assertThrows(CausalTopicRecreatedException.class, watch::ensureIntact,
                "conflicting init-time resolutions of one topic name must break the watch without a poll");
    }

    /**
     * A poll that finds one broken topic must still leave the first detected detail in place — the
     * failure message reports the first violation, and later polls cannot un-break the watch even
     * if the broker view flaps back.
     */
    @Test
    void aBrokenWatchStaysBrokenAcrossLaterCleanPolls() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.poll(new FixedIdsAdmin().with("t1", Uuid.randomUuid()));

        watch.poll(new FixedIdsAdmin().with("t1", T1_ID));

        assertThrows(CausalTopicRecreatedException.class, watch::ensureIntact,
                "identity, once broken, must stay broken for the process lifetime — a flapping broker "
                        + "view must not silently resume a member that already mislabelled");
    }

    /**
     * Seeded hammer for the watch's cross-thread contract, in the thread shape production runs it:
     * one poll thread (the {@code parsley-topic-identity-watch} daemon), several reader threads
     * calling {@code ensureIntact} per record on a directly held reference (as
     * {@code ParsleyProcessor} does after init), a concurrent init-time {@code expect} racing the
     * poll's iteration over the expectation map, and the close path's {@code unregister} racing a
     * still-scheduled poll. Each iteration asserts that the breaking poll's verdict surfaces to
     * every reader, that a surfaced failure is monotonic (later checks keep throwing), that every
     * observed failure message is complete — never a torn read of the detail — and that no thread
     * observes any other exception, close-versus-poll included. Replay a failure with
     * {@code -Dparsley.hammer.seed}; every assertion message carries the seed.
     */
    @Test
    void hammerPollAgainstReadersExpectAndCloseKeepsTheIdentityContract() throws Exception {
        Random random = new Random(HAMMER_SEED);
        for (int iteration = 0; iteration < HAMMER_ITERATIONS; iteration++) {
            runHammerIteration(random, iteration);
        }
    }

    /** One hammer round: fresh watch, racing threads, join, verdict. See the hammer test's Javadoc. */
    private static void runHammerIteration(Random random, int iteration) throws InterruptedException {
        String context = "seed=" + HAMMER_SEED + " iteration=" + iteration;
        String watchId = "hammer-watch-" + iteration;
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        ParsleyTopicIdentityWatch.register(watchId, watch);
        try {
            watch.expect("t1", T1_ID);
            Uuid recreated = seededUuidOtherThan(T1_ID, random);
            FixedIdsAdmin intactView = new FixedIdsAdmin().with("t1", T1_ID).with("t2", T2_ID);
            FixedIdsAdmin recreatedView = new FixedIdsAdmin().with("t1", recreated).with("t2", T2_ID);
            int intactPollsFirst = random.nextInt(4);

            ConcurrentLinkedQueue<Throwable> threadFailures = new ConcurrentLinkedQueue<>();
            CountDownLatch startGate = new CountDownLatch(1);
            AtomicBoolean stopPolling = new AtomicBoolean();

            Thread poller = hammerThread("hammer-poller", startGate, threadFailures, () -> {
                for (int i = 0; i < intactPollsFirst; i++) {
                    watch.poll(intactView);
                }
                // Poll until told to stop — deliberately across the closer's unregister, the
                // close-versus-poll race a still-scheduled poll runs in production.
                while (!stopPolling.get()) {
                    watch.poll(recreatedView);
                }
            });
            List<Thread> hammer = new ArrayList<>();
            for (int r = 0; r < HAMMER_READERS; r++) {
                hammer.add(hammerThread("hammer-reader-" + r, startGate, threadFailures,
                        () -> readUntilBrokenAssertingCoherence(watch, recreated, context)));
            }
            hammer.add(hammerThread("hammer-expecter", startGate, threadFailures, () -> {
                for (int i = 0; i < 50; i++) {
                    watch.expect("t2", T2_ID);
                }
            }));
            hammer.add(hammerThread("hammer-closer", startGate, threadFailures, () -> {
                ParsleyTopicIdentityWatch.unregister(watchId);
                assertNull(ParsleyTopicIdentityWatch.lookup(watchId),
                        "an unregistered watch id must stop resolving (" + context + ")");
            }));

            startGate.countDown();
            for (Thread thread : hammer) {
                joinOrFail(thread, context);
            }
            stopPolling.set(true);
            joinOrFail(poller, context);

            assertEquals(List.of(), List.copyOf(threadFailures),
                    "no hammer thread may observe an unexpected failure (" + context + ")");
            assertThrows(CausalTopicRecreatedException.class, watch::ensureIntact,
                    "after the hammer settles, the detected recreation must still fail every check ("
                            + context + ")");
        } finally {
            ParsleyTopicIdentityWatch.unregister(watchId);
        }
    }

    /** A started thread that waits on {@code startGate}, runs {@code body}, and queues any throwable. */
    private static Thread hammerThread(String name, CountDownLatch startGate,
                                       ConcurrentLinkedQueue<Throwable> threadFailures, Runnable body) {
        Thread thread = new Thread(() -> {
            try {
                startGate.await();
                body.run();
            } catch (Throwable t) {
                threadFailures.add(t);
            }
        }, name);
        thread.start();
        return thread;
    }

    /** Joins {@code thread} with a generous bound; a thread still alive after it is a hang, not slowness. */
    private static void joinOrFail(Thread thread, String context) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(thread.isAlive(),
                "hammer thread '" + thread.getName() + "' failed to finish (" + context + ")");
    }

    /**
     * The reader body: spins {@code ensureIntact} until the breaking poll's verdict surfaces, then
     * asserts the surfaced failure is complete and that identity stays broken for later checks —
     * the per-record call pattern of a task that keeps processing after a recreation was detected.
     */
    private static void readUntilBrokenAssertingCoherence(ParsleyTopicIdentityWatch watch,
                                                          Uuid recreated, String context) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        CausalTopicRecreatedException surfaced = null;
        while (surfaced == null) {
            assertTrue(System.nanoTime() < deadlineNanos,
                    "the breaking poll's verdict never surfaced to this reader (" + context + ")");
            try {
                watch.ensureIntact();
            } catch (CausalTopicRecreatedException e) {
                surfaced = e;
            }
        }
        assertMessageComplete(surfaced, recreated, context);
        for (int i = 0; i < 100; i++) {
            CausalTopicRecreatedException again = assertThrows(CausalTopicRecreatedException.class,
                    watch::ensureIntact,
                    "identity, once surfaced as broken, must stay broken for every later check ("
                            + context + ")");
            assertMessageComplete(again, recreated, context);
        }
    }

    /** Asserts {@code failure} carries the full recreation detail — a torn read would truncate it. */
    private static void assertMessageComplete(CausalTopicRecreatedException failure, Uuid recreated,
                                              String context) {
        String message = failure.getMessage();
        assertTrue(message.contains("t1") && message.contains(T1_ID.toString())
                        && message.contains(recreated.toString()),
                "the surfaced failure must carry the complete detail — topic name and both UUIDs ("
                        + context + "): " + message);
        assertTrue(message.contains("Do not delete and recreate causal topics"),
                "the surfaced failure must end with the operator guidance, not a torn detail ("
                        + context + "): " + message);
    }

    /** A seeded UUID distinct from {@code other} — the recreated topic's new broker identity. */
    private static Uuid seededUuidOtherThan(Uuid other, Random random) {
        Uuid id;
        do {
            id = new Uuid(random.nextLong(), random.nextLong());
        } while (id.equals(other));
        return id;
    }
}
